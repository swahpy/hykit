package cmap

import (
	"hash/maphash"
	"sync"
)

// Map is what every impl in this package satisfies. All methods are safe
// to call concurrently. Individual calls are atomic; combining Load with
// a follow-up Store is not race-free — other goroutines can slip in
// between. Use LoadOrStore, LoadAndDelete, or Compute when you need
// "check then act" atomicity.
type Map[K comparable, V any] interface {
	// Load returns the stored value for k. ok is true if the key was there;
	// on a miss, v is the zero value of V.
	Load(k K) (v V, ok bool)

	// Store sets k to v. Overwrites any existing value.
	Store(k K, v V)

	// Delete removes k. No-op (not a panic) if k isn't there.
	Delete(k K)

	// LoadOrStore returns the existing value for the key if present; otherwise
	// stores and returns v. loaded is true iff the key was already present.
	// The check-and-store is atomic — competing callers will see exactly one
	// Store happen.
	LoadOrStore(k K, v V) (actual V, loaded bool)

	// LoadAndDelete atomically loads the value for the key and deletes it.
	// loaded is true iff the key was present before the call. On a miss,
	// value is the zero value of V.
	LoadAndDelete(k K) (value V, loaded bool)

	// Compute atomically transforms the value for k. fn is called under the
	// map's lock with the current value and whether the key existed; the value
	// fn returns replaces (or creates) the entry. If the key was absent when
	// Compute was called, exists is false and oldValue is the zero value of V.
	// The returned newValue is the value now stored; existed reports whether
	// the key was present before the call.
	Compute(k K, fn func(oldValue V, exists bool) V) (newValue V, existed bool)
}

// Compile-time proof each impl still satisfies Map[K, V].
var (
	_ Map[string, string] = (*MutexMap[string, string])(nil)
	_ Map[string, string] = (*RWMutexMap[string, string])(nil)
	_ Map[string, string] = (*SyncMap[string, string])(nil)
	_ Map[string, string] = (*ShardedMap[string, string])(nil)
)

// --- MutexMap ---

// MutexMap is a simple map protected by a mutex.
type MutexMap[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]V
}

// NewMutexMap returns a MutexMap preallocated for size entries.
func NewMutexMap[K comparable, V any](size int) *MutexMap[K, V] {
	return &MutexMap[K, V]{m: make(map[K]V, size)}
}

// Load returns the value for k and whether it was present.
func (m *MutexMap[K, V]) Load(k K) (V, bool) {
	m.mu.Lock()
	v, ok := m.m[k]
	m.mu.Unlock()
	return v, ok
}

// Store sets k to v.
func (m *MutexMap[K, V]) Store(k K, v V) {
	m.mu.Lock()
	m.m[k] = v
	m.mu.Unlock()
}

// Delete removes k from the map.
func (m *MutexMap[K, V]) Delete(k K) {
	m.mu.Lock()
	delete(m.m, k)
	m.mu.Unlock()
}

func (m *MutexMap[K, V]) LoadOrStore(k K, v V) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.m[k]; ok {
		return existing, true
	}
	m.m[k] = v
	return v, false
}

func (m *MutexMap[K, V]) LoadAndDelete(k K) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.m[k]
	if ok {
		delete(m.m, k)
	}
	return v, ok
}

func (m *MutexMap[K, V]) Compute(k K, fn func(V, bool) V) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, existed := m.m[k]
	newV := fn(old, existed)
	m.m[k] = newV
	return newV, existed
}

// --- RWMutexMap ---

// RWMutexMap is a simple map protected by a read-write mutex.
type RWMutexMap[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

// NewRWMutexMap returns an RWMutexMap preallocated for size entries.
func NewRWMutexMap[K comparable, V any](size int) *RWMutexMap[K, V] {
	return &RWMutexMap[K, V]{m: make(map[K]V, size)}
}

// Load returns the value for k and whether it was present.
func (m *RWMutexMap[K, V]) Load(k K) (V, bool) {
	m.mu.RLock()
	v, ok := m.m[k]
	m.mu.RUnlock()
	return v, ok
}

// Store sets k to v.
func (m *RWMutexMap[K, V]) Store(k K, v V) {
	m.mu.Lock()
	m.m[k] = v
	m.mu.Unlock()
}

// Delete removes k from the map.
func (m *RWMutexMap[K, V]) Delete(k K) {
	m.mu.Lock()
	delete(m.m, k)
	m.mu.Unlock()
}

func (m *RWMutexMap[K, V]) LoadOrStore(k K, v V) (V, bool) {
	// Hit-fast path: read lock is enough when the key already exists.
	m.mu.RLock()
	if existing, ok := m.m[k]; ok {
		m.mu.RUnlock()
		return existing, true
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check after upgrading — someone else may have inserted while we were
	// waiting for the write lock.
	if existing, ok := m.m[k]; ok {
		return existing, true
	}
	m.m[k] = v
	return v, false
}

func (m *RWMutexMap[K, V]) LoadAndDelete(k K) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.m[k]
	if ok {
		delete(m.m, k)
	}
	return v, ok
}

func (m *RWMutexMap[K, V]) Compute(k K, fn func(V, bool) V) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, existed := m.m[k]
	newV := fn(old, existed)
	m.m[k] = newV
	return newV, existed
}

// --- sync.Map ---

// SyncMap is a generic wrapper around sync.Map.
type SyncMap[K comparable, V any] struct {
	m sync.Map
}

// NewSyncMap returns a SyncMap. The size argument is ignored — sync.Map takes
// no capacity hint — and is kept for API symmetry with the other constructors.
func NewSyncMap[K comparable, V any](_ int) *SyncMap[K, V] { return &SyncMap[K, V]{} }

// Load returns the value for k and whether it was present.
func (m *SyncMap[K, V]) Load(k K) (V, bool) {
	var zero V
	v, ok := m.m.Load(k)
	if !ok {
		return zero, false
	}
	return v.(V), true
}

// Store sets k to v.
func (m *SyncMap[K, V]) Store(k K, v V) { m.m.Store(k, v) }

// Delete removes k from the map.
func (m *SyncMap[K, V]) Delete(k K) { m.m.Delete(k) }

func (m *SyncMap[K, V]) LoadOrStore(k K, v V) (V, bool) {
	actual, loaded := m.m.LoadOrStore(k, v)
	return actual.(V), loaded
}

func (m *SyncMap[K, V]) LoadAndDelete(k K) (V, bool) {
	v, loaded := m.m.LoadAndDelete(k)
	if !loaded {
		var zero V
		return zero, false
	}
	return v.(V), true
}

// Compute — see Map.Compute. NOTE: on SyncMap this uses sync.Map's
// CompareAndSwap under the hood, which requires V to be a runtime-comparable
// type. Storing non-comparable V (slices, maps, functions) will panic when
// Compute is called. If your V is not comparable, use ShardedMap or
// MutexMap instead. fn may run more than once under contention; keep it
// side-effect-free.
func (m *SyncMap[K, V]) Compute(k K, fn func(V, bool) V) (V, bool) {
	for {
		raw, existed := m.m.Load(k)
		var old V
		if existed {
			old = raw.(V)
		}
		newV := fn(old, existed)

		if existed {
			// Swap only if the value is still what we read.
			if m.m.CompareAndSwap(k, raw, newV) {
				return newV, true
			}
		} else {
			// Insert only if nobody stored anything in the meantime.
			if _, loaded := m.m.LoadOrStore(k, newV); !loaded {
				return newV, false
			}
		}
		// Contended path: someone changed the entry, retry.
	}
}

// --- Sharded Map ---

const (
	numShards = 256           // power of two → mask instead of modulo
	shardMask = numShards - 1 // [0 - shardMask] shards to hold the map entries
)

// shard is a single shard of the sharded map, containing a mutex and a map.
type shard[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]V
	_  [48]byte // pad shard to a full 64B cache line (8 + 8 + 48 = 64)
}

// ShardedMap is a sharded map that partitions the key space across N shards, each with its own mutex.
type ShardedMap[K comparable, V any] struct {
	shards [numShards]shard[K, V]
	seed   maphash.Seed
}

// NewShardedMap returns a ShardedMap. size is a total-capacity hint spread evenly across shards.
func NewShardedMap[K comparable, V any](size int) *ShardedMap[K, V] {
	s := &ShardedMap[K, V]{seed: maphash.MakeSeed()}
	per := size/numShards + 1
	for i := range s.shards {
		s.shards[i].m = make(map[K]V, per)
	}
	return s
}

// at returns the shard for a given key. It uses maphash to hash
// the key and then masks it to find the appropriate shard index.
func (s *ShardedMap[K, V]) at(k K) *shard[K, V] {
	return &s.shards[maphash.Comparable(s.seed, k)&shardMask]
}

// Load retrieves the value for a key from the sharded map.
// It locks the shard's mutex for reading, retrieves the value,
// and then unlocks the mutex.
func (s *ShardedMap[K, V]) Load(k K) (V, bool) {
	sh := s.at(k)
	sh.mu.Lock()
	v, ok := sh.m[k]
	sh.mu.Unlock()
	return v, ok
}

// Store sets the value for a key in the sharded map.
func (s *ShardedMap[K, V]) Store(k K, v V) {
	sh := s.at(k)
	sh.mu.Lock()
	sh.m[k] = v
	sh.mu.Unlock()
}

// Delete removes the key from the sharded map.
func (s *ShardedMap[K, V]) Delete(k K) {
	sh := s.at(k)
	sh.mu.Lock()
	delete(sh.m, k)
	sh.mu.Unlock()
}

func (s *ShardedMap[K, V]) LoadOrStore(k K, v V) (V, bool) {
	sh := s.at(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if existing, ok := sh.m[k]; ok {
		return existing, true
	}
	sh.m[k] = v
	return v, false
}

func (s *ShardedMap[K, V]) LoadAndDelete(k K) (V, bool) {
	sh := s.at(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	v, ok := sh.m[k]
	if ok {
		delete(sh.m, k)
	}
	return v, ok
}

func (s *ShardedMap[K, V]) Compute(k K, fn func(V, bool) V) (V, bool) {
	sh := s.at(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	old, existed := sh.m[k]
	newV := fn(old, existed)
	sh.m[k] = newV
	return newV, existed
}

// Len returns the total number of entries in the sharded map.
// It is O(numShards) and NOT a consistent snapshot — entries
// can be added/removed in other shards while we're counting.
// Fine for metrics, wrong for anything that needs a true point-in-time count.
func (s *ShardedMap[K, V]) Len() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		n += len(sh.m)
		sh.mu.Unlock()
	}
	return n
}

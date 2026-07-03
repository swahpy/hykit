package cmap

import (
	"hash/maphash"
	"sync"
)

// Map is the common interface satisfied by every implementation in this package.
type Map[K comparable, V any] interface {
	Load(k K) (V, bool)
	Store(k K, v V)
	Delete(k K)
}

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

// --- sync.Map ---

// SyncMap is a generic wrapper around sync.Map.
type SyncMap[K comparable, V any] struct {
	m sync.Map
}

// NewSyncMap returns a SyncMap. The size argument is ignored — sync.Map takes no capacity hint — and is kept for API symmetry with the other constructors.
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
	p := s.at(k)
	p.mu.Lock()
	v, ok := p.m[k]
	p.mu.Unlock()
	return v, ok
}

// Store sets the value for a key in the sharded map.
func (s *ShardedMap[K, V]) Store(k K, v V) {
	p := s.at(k)
	p.mu.Lock()
	p.m[k] = v
	p.mu.Unlock()
}

// Delete removes the key from the sharded map.
func (s *ShardedMap[K, V]) Delete(k K) {
	p := s.at(k)
	p.mu.Lock()
	delete(p.m, k)
	p.mu.Unlock()
}

// Len returns the total number of entries in the sharded map.
// It is O(numShards) and NOT a consistent snapshot — entries
// can be added/removed in other shards while we're counting.
// Fine for metrics, wrong for anything that needs a true point-in-time count.
func (s *ShardedMap[K, V]) Len() int {
	n := 0
	for i := range s.shards {
		p := &s.shards[i]
		p.mu.Lock()
		n += len(p.m)
		p.mu.Unlock()
	}
	return n
}

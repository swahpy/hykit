package cmap

import (
	"hash/maphash"
	"sync"
)

// Map is the common interface satisfied by every map implementation in this package.
type Map interface {
	Load(k string) (string, bool)
	Store(k, v string)
}

// --- MutexMap ---

// MutexMap is a simple map protected by a mutex.
type MutexMap struct {
	mu sync.Mutex
	m  map[string]string
}

// NewMutexMap returns a MutexMap preallocated for size entries.
func NewMutexMap(size int) *MutexMap {
	return &MutexMap{m: make(map[string]string, size)}
}

// Load returns the value for k and whether it was present.
func (m *MutexMap) Load(k string) (string, bool) {
	m.mu.Lock()
	v, ok := m.m[k]
	m.mu.Unlock()
	return v, ok
}

// Store sets k to v.
func (m *MutexMap) Store(k, v string) {
	m.mu.Lock()
	m.m[k] = v
	m.mu.Unlock()
}

// --- RWMutexMap ---

// RWMutexMap is a simple map protected by a read-write mutex.
type RWMutexMap struct {
	mu sync.RWMutex
	m  map[string]string
}

// NewRWMutexMap returns an RWMutexMap preallocated for size entries.
func NewRWMutexMap(size int) *RWMutexMap {
	return &RWMutexMap{m: make(map[string]string, size)}
}

// Load returns the value for k and whether it was present.
func (m *RWMutexMap) Load(k string) (string, bool) {
	m.mu.RLock()
	v, ok := m.m[k]
	m.mu.RUnlock()
	return v, ok
}

// Store sets k to v.
func (m *RWMutexMap) Store(k, v string) {
	m.mu.Lock()
	m.m[k] = v
	m.mu.Unlock()
}

// --- sync.Map ---

// SyncMap is a string-typed wrapper around sync.Map.
type SyncMap struct {
	m sync.Map
}

// NewSyncMap returns a SyncMap. The size argument is ignored — sync.Map takes no capacity hint — and is kept for API symmetry with the other constructors.
func NewSyncMap(_ int) *SyncMap { return &SyncMap{} }

// Load returns the value for k and whether it was present.
func (m *SyncMap) Load(k string) (string, bool) {
	v, ok := m.m.Load(k)
	if !ok {
		return "", false
	}
	return v.(string), true
}

// Store sets k to v.
func (m *SyncMap) Store(k, v string) {
	m.m.Store(k, v)
}

// --- Sharded Map ---

const (
	numShards = 256           // power of two → mask instead of modulo
	shardMask = numShards - 1 // [0 - shardMask] shards to hold the map entries
)

// shard is a single shard of the sharded map, containing a mutex and a map.
type shard struct {
	mu sync.Mutex
	m  map[string]string
	_  [48]byte // pad shard to a full 64B cache line (8 + 8 + 48 = 64)
}

// ShardedMap is a sharded map that partitions the key space across N shards, each with its own mutex.
type ShardedMap struct {
	shards [numShards]shard
	seed   maphash.Seed
}

// NewShardedMap returns a ShardedMap. size is a total-capacity hint spread evenly across shards.
func NewShardedMap(size int) *ShardedMap {
	s := &ShardedMap{seed: maphash.MakeSeed()}
	per := size/numShards + 1
	for i := range s.shards {
		s.shards[i].m = make(map[string]string, per)
	}
	return s
}

// at returns the shard for a given key. It uses maphash to hash
// the key and then masks it to find the appropriate shard index.
func (s *ShardedMap) at(key string) *shard {
	return &s.shards[maphash.String(s.seed, key)&shardMask]
}

// Load retrieves the value for a key from the sharded map.
// It locks the shard's mutex for reading, retrieves the value,
// and then unlocks the mutex.
func (s *ShardedMap) Load(k string) (string, bool) {
	p := s.at(k)
	p.mu.Lock()
	v, ok := p.m[k]
	p.mu.Unlock()
	return v, ok
}

// Store sets the value for a key in the sharded map.
func (s *ShardedMap) Store(k, v string) {
	p := s.at(k)
	p.mu.Lock()
	p.m[k] = v
	p.mu.Unlock()
}

// Delete removes the key from the sharded map.
func (s *ShardedMap) Delete(k string) {
	p := s.at(k)
	p.mu.Lock()
	delete(p.m, k)
	p.mu.Unlock()
}

// Len returns the total number of entries in the sharded map.
// It is O(numShards) and NOT a consistent snapshot — entries
// can be added/removed in other shards while we're counting.
// Fine for metrics, wrong for anything that needs a true point-in-time count.
func (s *ShardedMap) Len() int {
	n := 0
	for i := range s.shards {
		p := &s.shards[i]
		p.mu.Lock()
		n += len(p.m)
		p.mu.Unlock()
	}
	return n
}

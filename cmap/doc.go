// Package cmap provides four concurrent map implementations that share a
// common generic Map interface:
//
//	type Map[K comparable, V any] interface {
//	    Load(k K) (V, bool)
//	    Store(k K, v V)
//	    Delete(k K)
//	    LoadOrStore(k K, v V) (actual V, loaded bool)
//	    LoadAndDelete(k K) (value V, loaded bool)
//	    Compute(k K, fn func(oldValue V, exists bool) V) (newValue V, existed bool)
//	}
//
// So callers can pick the one that matches their workload and swap without
// touching call sites. Every implementation supports Delete.
//
// Semantics of the read-modify-write methods:
//
//   - LoadOrStore does NOT overwrite: on a hit, the stored value is unchanged
//     and the caller's v is discarded.
//   - LoadAndDelete atomically returns the value and removes the key. On miss,
//     returns the zero value of V and loaded=false.
//   - Compute always writes: returning the zero value from fn stores the zero
//     value; there is no "leave the entry alone" or "delete" signal from fn.
//     If you need conditional delete, do it explicitly with Delete. On
//     SyncMap the implementation is lock-free (CAS retry loop), so fn may
//     run more than once under contention — keep fn side-effect-free.
//
// SyncMap.Compute has an extra caveat: it relies on sync.Map.CompareAndSwap
// and therefore requires V to be a runtime-comparable type. If your V is a
// slice, map, or function type, use ShardedMap or MutexMap instead.
//
// Len is only on ShardedMap: sync.Map has no O(1) size, and MutexMap /
// RWMutexMap are single-lock so adding Len there would be trivial but out
// of scope for this interface (which focuses on per-key operations).
//
// The implementations are:
//
//   - MutexMap:   a plain map guarded by a sync.Mutex. Simple; scales
//     poorly under contention.
//   - RWMutexMap: same, but with sync.RWMutex. Better for read-heavy
//     workloads, worse when writers show up often.
//   - SyncMap:    a thin wrapper over sync.Map. Optimized for read-mostly
//     or disjoint-key workloads; allocates on Store.
//   - ShardedMap: partitions the key space across 256 shards, each with
//     its own mutex. Best throughput under high concurrency;
//     zero allocations on the hot path.
//
// When in doubt, start with ShardedMap — it dominates the other three on
// every read/write mix in the accompanying benchmarks. See bench_test.go
// for the harness and Mops/s numbers per implementation.
package cmap

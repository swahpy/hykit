// Package cmap provides four concurrent map implementations that share a
// common generic Map interface:
//
//	type Map[K comparable, V any] interface {
//	    Load(k K) (V, bool)
//	    Store(k K, v V)
//	    Delete(k K)
//	}
//
// So callers can pick the one that matches their workload and swap without
// touching call sites. Every implementation supports Delete.
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

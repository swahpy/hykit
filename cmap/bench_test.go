package cmap

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"
)

// Benchmark 4 implementations (mutex, rwmutex, syncmap, sharded) under 4
// read/write mixes (r100, r90, r50, r10) at the current -cpu setting.
//
// Run with:
//
//	go test -bench . -benchmem -run=^$ -cpu=1,2,4,8 -benchtime=2s
//
// Reports Mops/s as a custom metric — higher is better.

const numKeys = 1_000_000

type impl struct {
	name string
	make func(size int) Map[string, string]
}

var impls = []impl{
	{"mutex", func(n int) Map[string, string] { return NewMutexMap[string, string](n) }},
	{"rwmutex", func(n int) Map[string, string] { return NewRWMutexMap[string, string](n) }},
	{"syncmap", func(n int) Map[string, string] { return NewSyncMap[string, string](n) }},
	{"sharded", func(n int) Map[string, string] { return NewShardedMap[string, string](n) }},
}

type mix struct {
	name    string
	readPct int // 0..100
}

var mixes = []mix{
	{"r100", 100},
	{"r90", 90},
	{"r50", 50},
	{"r10", 10},
}

// keys is pre-generated so string allocation & conversion aren't part of the
// hot path — we want to measure the map, not strconv.
var keys = func() []string {
	ks := make([]string, numKeys)
	for i := range ks {
		ks[i] = strconv.Itoa(i)
	}
	return ks
}()

// prewarm loads every key so Load hits are realistic and shards are already
// sized.
func prewarm(m Map[string, string]) {
	for _, k := range keys {
		m.Store(k, k)
	}
}

func BenchmarkMaps(b *testing.B) {
	for _, im := range impls {
		for _, mx := range mixes {
			name := fmt.Sprintf("%s/%s", im.name, mx.name)
			b.Run(name, func(b *testing.B) {
				m := im.make(numKeys)
				prewarm(m)

				b.ResetTimer()
				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					// Per-goroutine PRNG — avoids contention on a shared source,
					// which would otherwise show up as a false bottleneck for the
					// faster implementations.
					r := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
					threshold := uint32(mx.readPct) * (^uint32(0) / 100)
					for pb.Next() {
						k := keys[r.Uint64N(numKeys)]
						if r.Uint32() < threshold {
							_, _ = m.Load(k)
						} else {
							m.Store(k, k)
						}
					}
				})

				nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
				b.ReportMetric(1000.0/nsPerOp, "Mops/s")
			})
		}
	}
}

func BenchmarkLoadOrStore(b *testing.B) {
	for _, im := range impls {
		b.Run(im.name, func(b *testing.B) {
			m := im.make(numKeys)
			prewarm(m)

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
				for pb.Next() {
					k := keys[r.Uint64N(numKeys)]
					_, _ = m.LoadOrStore(k, k)
				}
			})

			nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
			b.ReportMetric(1000.0/nsPerOp, "Mops/s")
		})
	}
}

// BenchmarkLoadOrStoreMiss — every op is a miss (fresh key beyond the
// pre-warmed keyspace), forcing the write-lock / upgrade path on impls that
// have one. Complements BenchmarkLoadOrStore's 100%-hit workload.
func BenchmarkLoadOrStoreMiss(b *testing.B) {
	for _, im := range impls {
		b.Run(im.name, func(b *testing.B) {
			m := im.make(numKeys)
			prewarm(m)

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
				for pb.Next() {
					// Keys guaranteed outside the pre-warmed range, so each
					// LoadOrStore is a miss for the map (though later ops may
					// hit if the same tag is drawn twice).
					tag := numKeys + r.Uint64N(numKeys)
					k := strconv.FormatUint(tag, 10)
					_, _ = m.LoadOrStore(k, k)
				}
			})

			nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
			b.ReportMetric(1000.0/nsPerOp, "Mops/s")
		})
	}
}

func BenchmarkCompute(b *testing.B) {
	for _, im := range impls {
		b.Run(im.name, func(b *testing.B) {
			m := im.make(numKeys)
			prewarm(m)

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
				for pb.Next() {
					k := keys[r.Uint64N(numKeys)]
					m.Compute(k, func(old string, _ bool) string {
						return old // no-op update; measures overhead of Compute itself
					})
				}
			})

			nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
			b.ReportMetric(1000.0/nsPerOp, "Mops/s")
		})
	}
}

// BenchmarkComputeHotKey — all goroutines contend on ONE key with a fn that
// always produces a new value. On SyncMap this forces CAS retries, exposing
// the lock-free retry cost that BenchmarkCompute (uniform keys, no-op fn)
// deliberately hides.
func BenchmarkComputeHotKey(b *testing.B) {
	for _, im := range impls {
		b.Run(im.name, func(b *testing.B) {
			m := im.make(1)
			m.Store("hot", "")

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
				for pb.Next() {
					// Return a value derived from a random tag so CAS never
					// trivially swaps "same to same".
					tag := r.Uint64()
					m.Compute("hot", func(old string, _ bool) string {
						return strconv.FormatUint(tag, 10)
					})
				}
			})

			nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
			b.ReportMetric(1000.0/nsPerOp, "Mops/s")
		})
	}
}

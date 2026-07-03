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
				b.ReportMetric(0, "ns/op") // suppress noise; keep b.N & Mops/s
			})
		}
	}
}

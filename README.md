# hykit

A personal Go toolbox. General-purpose utilities the author reaches for repeatedly, packaged as small, well-tested, standalone packages.

- **Zero third-party dependencies** — standard library only.
- **Go 1.26+**.
- **`v0.x`** — API may change until `v1.0.0`.
- MIT licensed.

## Install

```bash
go get github.com/swahpy/hykit@latest
```

## Components

### `cmap` — Concurrent map implementations

> **v0.2.0:** the interface became generic (`Map[K comparable, V any]`) and every implementation supports `Delete`.
>
> **v0.3.0:** added `LoadOrStore`, `LoadAndDelete`, and `Compute` to the `Map` interface. Users of the four concrete types are unaffected; anyone who wrote their own `Map` implementation must add the three new methods.

Four `Map` implementations behind a shared interface, benchmarked head-to-head:

- `MutexMap` — plain `sync.Mutex`; simplest, scales poorly.
- `RWMutexMap` — `sync.RWMutex`; better for read-heavy work.
- `SyncMap` — thin wrapper over `sync.Map`; good for read-mostly / disjoint keys.
- `ShardedMap` — 256 shards, each with its own mutex; **fastest** across every read/write mix.

```go
import "github.com/swahpy/hykit/cmap"

m := cmap.NewShardedMap[string, string](1_000_000)
m.Store("hello", "world")
v, ok := m.Load("hello") // "world", true

// Atomic "if absent, store" — perfect for one-time init.
actual, loaded := m.LoadOrStore("hello", "again")
// actual == "world", loaded == true (existing value kept)

// Atomically take a value out.
v, ok = m.LoadAndDelete("hello") // "world", true

// Read-modify-write in one atomic step. Perfect for counters or list append.
counters := cmap.NewShardedMap[string, int](0)
counters.Compute("clicks", func(old int, _ bool) int {
    return old + 1
})
```

Run benchmarks:

```bash
go test -bench . -benchmem -run=^$ -cpu=1,2,4,8 -benchtime=2s ./cmap/
```

## License

MIT — see [LICENSE](./LICENSE).

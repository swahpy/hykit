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

> **v0.2.0 breaking change:** the interface is now generic (`Map[K comparable, V any]`) and every implementation supports `Delete`. Users of v0.1.0 must add explicit type parameters to constructor calls.

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
m.Delete("hello")
```

Run benchmarks:

```bash
go test -bench . -benchmem -run=^$ -cpu=1,2,4,8 -benchtime=2s ./cmap/
```

## Roadmap

| Month | Planned                                                               |
| ----- | --------------------------------------------------------------------- |
| 1     | `cmap` (done), scaffold, CI, `v0.1.0`                                 |
| 2     | `slice` (generic helpers) + `retry` (backoff)                         |
| 3     | `pool` (generic object pool)                                          |
| 4     | `set` + `stringx`                                                     |
| 5     | `timex` + `errx`                                                      |
| 6     | Retrospective — keep expanding vs. pivot to a themed library          |

The roadmap is a target, not a contract. Real usage drives priority.

## License

MIT — see [LICENSE](./LICENSE).

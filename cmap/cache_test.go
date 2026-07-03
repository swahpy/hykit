package cmap

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
)

// factories drives all shared behavioral tests. Each implementation satisfies
// the Map interface, so one table covers them all.
var factories = []struct {
	name string
	make func(size int) Map[string, string]
}{
	{"MutexMap", func(n int) Map[string, string] { return NewMutexMap[string, string](n) }},
	{"RWMutexMap", func(n int) Map[string, string] { return NewRWMutexMap[string, string](n) }},
	{"SyncMap", func(n int) Map[string, string] { return NewSyncMap[string, string](n) }},
	{"ShardedMap", func(n int) Map[string, string] { return NewShardedMap[string, string](n) }},
}

// TestLoadMissing — an empty map returns (zero, false) for any key.
func TestLoadMissing(t *testing.T) {
	for _, f := range factories {
		t.Run(f.name, func(t *testing.T) {
			m := f.make(4)
			v, ok := m.Load("nope")
			if ok || v != "" {
				t.Fatalf("empty Load: got (%q,%v), want (\"\",false)", v, ok)
			}
		})
	}
}

// TestStoreLoad — round-trip a handful of keys.
func TestStoreLoad(t *testing.T) {
	for _, f := range factories {
		t.Run(f.name, func(t *testing.T) {
			m := f.make(8)
			for i := 0; i < 32; i++ {
				k := strconv.Itoa(i)
				m.Store(k, "v"+k)
			}
			for i := 0; i < 32; i++ {
				k := strconv.Itoa(i)
				v, ok := m.Load(k)
				if !ok || v != "v"+k {
					t.Fatalf("Load(%q): got (%q,%v), want (\"v%s\",true)", k, v, ok, k)
				}
			}
		})
	}
}

// TestOverwrite — Store on an existing key replaces the value.
func TestOverwrite(t *testing.T) {
	for _, f := range factories {
		t.Run(f.name, func(t *testing.T) {
			m := f.make(1)
			m.Store("k", "old")
			m.Store("k", "new")
			v, ok := m.Load("k")
			if !ok || v != "new" {
				t.Fatalf("overwrite: got (%q,%v), want (\"new\",true)", v, ok)
			}
		})
	}
}

// TestEmptyKey — the empty string is a valid key; don't special-case it.
func TestEmptyKey(t *testing.T) {
	for _, f := range factories {
		t.Run(f.name, func(t *testing.T) {
			m := f.make(1)
			m.Store("", "empty")
			v, ok := m.Load("")
			if !ok || v != "empty" {
				t.Fatalf("empty key: got (%q,%v), want (\"empty\",true)", v, ok)
			}
		})
	}
}

// TestConcurrentStoreLoad — hammer each impl with N goroutines each writing
// its own key range, then verify every write is readable. -race catches any
// data races.
func TestConcurrentStoreLoad(t *testing.T) {
	const goroutines = 32
	const perG = 500

	for _, f := range factories {
		t.Run(f.name, func(t *testing.T) {
			m := f.make(goroutines * perG)
			var wg sync.WaitGroup
			wg.Add(goroutines)
			for g := 0; g < goroutines; g++ {
				g := g
				go func() {
					defer wg.Done()
					for i := 0; i < perG; i++ {
						k := fmt.Sprintf("g%d-k%d", g, i)
						m.Store(k, k)
					}
				}()
			}
			wg.Wait()

			for g := 0; g < goroutines; g++ {
				for i := 0; i < perG; i++ {
					k := fmt.Sprintf("g%d-k%d", g, i)
					v, ok := m.Load(k)
					if !ok || v != k {
						t.Fatalf("Load(%q): got (%q,%v), want (%q,true)", k, v, ok, k)
					}
				}
			}
		})
	}
}

// TestConcurrentReadersWriter — many readers on stable keys while a writer
// churns disjoint keys. Should be race-free and readers should always see
// their known values.
func TestConcurrentReadersWriter(t *testing.T) {
	for _, f := range factories {
		t.Run(f.name, func(t *testing.T) {
			m := f.make(128)
			// Seed 100 stable keys the readers watch.
			for i := 0; i < 100; i++ {
				k := "r" + strconv.Itoa(i)
				m.Store(k, k)
			}

			done := make(chan struct{})
			var wg sync.WaitGroup

			// 8 readers loop over the stable set until told to stop.
			for r := 0; r < 8; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-done:
							return
						default:
						}
						for i := 0; i < 100; i++ {
							k := "r" + strconv.Itoa(i)
							v, ok := m.Load(k)
							if !ok || v != k {
								t.Errorf("reader Load(%q): got (%q,%v)", k, v, ok)
								return
							}
						}
					}
				}()
			}

			// Writer churns disjoint keys.
			for i := 0; i < 5000; i++ {
				k := "w" + strconv.Itoa(i)
				m.Store(k, k)
			}
			close(done)
			wg.Wait()
		})
	}
}

// TestDelete — every impl implements Delete after v0.2.0.
func TestDelete(t *testing.T) {
	for _, f := range factories {
		t.Run(f.name, func(t *testing.T) {
			m := f.make(2)
			m.Store("a", "1")
			m.Store("b", "2")
			m.Delete("a")
			if v, ok := m.Load("a"); ok {
				t.Fatalf("Load after Delete: got (%q,true), want (_,false)", v)
			}
			if v, ok := m.Load("b"); !ok || v != "2" {
				t.Fatalf("Load(b): got (%q,%v), want (\"2\",true)", v, ok)
			}
			m.Delete("missing") // must not panic
		})
	}
}

// --- ShardedMap-specific tests ---

func TestShardedMap_Len(t *testing.T) {
	m := NewShardedMap[string, string](0)
	if got := m.Len(); got != 0 {
		t.Fatalf("Len on empty: %d, want 0", got)
	}

	const n = 1000
	for i := 0; i < n; i++ {
		m.Store(strconv.Itoa(i), "v")
	}
	if got := m.Len(); got != n {
		t.Fatalf("Len after %d inserts: %d", n, got)
	}

	// Overwrites don't change Len.
	for i := 0; i < n; i++ {
		m.Store(strconv.Itoa(i), "v2")
	}
	if got := m.Len(); got != n {
		t.Fatalf("Len after overwrites: %d, want %d", got, n)
	}

	for i := 0; i < n/2; i++ {
		m.Delete(strconv.Itoa(i))
	}
	if got := m.Len(); got != n/2 {
		t.Fatalf("Len after deletes: %d, want %d", got, n/2)
	}
}

// TestGenericTypes — smoke test that all four impls compile & work with a
// non-string key/value combo. int -> struct is a decent stand-in.
func TestGenericTypes(t *testing.T) {
	type payload struct{ x, y int }

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"MutexMap", func(t *testing.T) {
			m := NewMutexMap[int, payload](0)
			m.Store(1, payload{2, 3})
			if v, ok := m.Load(1); !ok || v != (payload{2, 3}) {
				t.Fatalf("got (%v,%v)", v, ok)
			}
		}},
		{"RWMutexMap", func(t *testing.T) {
			m := NewRWMutexMap[int, payload](0)
			m.Store(1, payload{2, 3})
			if v, ok := m.Load(1); !ok || v != (payload{2, 3}) {
				t.Fatalf("got (%v,%v)", v, ok)
			}
		}},
		{"SyncMap", func(t *testing.T) {
			m := NewSyncMap[int, payload](0)
			m.Store(1, payload{2, 3})
			if v, ok := m.Load(1); !ok || v != (payload{2, 3}) {
				t.Fatalf("got (%v,%v)", v, ok)
			}
		}},
		{"ShardedMap", func(t *testing.T) {
			m := NewShardedMap[int, payload](0)
			m.Store(1, payload{2, 3})
			if v, ok := m.Load(1); !ok || v != (payload{2, 3}) {
				t.Fatalf("got (%v,%v)", v, ok)
			}
			m.Delete(1)
			if _, ok := m.Load(1); ok {
				t.Fatal("Delete had no effect")
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, c.run)
	}
}

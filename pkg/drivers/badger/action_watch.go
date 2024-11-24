// SPDX-License-Identifier: Apache-2.0.

package badger

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/k3s-io/kine/pkg/server"

	badgerdb "github.com/dgraph-io/badger/v4"
)

func (b *Backend) sendEvent(key string, event *server.Event) {
	if event == nil {
		return
	}

	getEventRevision := func(event *server.Event) int64 {
		if event.KV != nil {
			return event.KV.ModRevision
		}

		if event.PrevKV != nil {
			return event.PrevKV.ModRevision
		}

		return -1
	}

	b.watchers.Range(func(k, v any) bool {
		w, ok := v.(*watcher)
		if !ok || w == nil {
			return true
		}

		eventRev := getEventRevision(event)

		if eventRev <= w.startRevision {
			return true
		}

		if w.prefix != "" && !strings.HasPrefix(key, w.prefix) {
			return true
		}

		go func() {
			if atomic.LoadInt32(&w.isClosed) == 1 {
				return
			}

			select {
			case w.events <- []*server.Event{event}:
			default:
			}
		}()

		return true
	})
}

func (b *Backend) Watch(ctx context.Context, prefix string, startRevision int64) server.WatchResult {
	events := make(chan []*server.Event, 1000)
	currentRevision := int64(b.db.MaxVersion())

	id := time.Now().UnixNano()
	w := &watcher{
		id:            id,
		prefix:        prefix,
		startRevision: startRevision,
		events:        events,
	}

	b.watchers.Store(id, w)

	if startRevision > 0 && startRevision < currentRevision {
		go func() {
			if err := b.streamHistoricalEvents(ctx, w, prefix, startRevision, currentRevision); err != nil {
				_ = err
			}
		}()
	}

	go func() {
		<-ctx.Done()

		if atomic.CompareAndSwapInt32(&w.isClosed, 0, 1) {
			b.watchers.Delete(id)
			close(events)
		}
	}()

	return server.WatchResult{
		Events:          events,
		CurrentRevision: currentRevision,
	}
}

func (b *Backend) streamHistoricalEvents(ctx context.Context, w *watcher, prefix string, startRev, endRev int64) error {
	if atomic.LoadInt32(&w.isClosed) == 1 {
		return nil
	}

	keyEvents := make(map[string]*server.KeyValue)

	err := b.db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.AllVersions = true

		// type IteratorOptions struct {
		// 	// PrefetchSize is the number of KV pairs to prefetch while iterating.
		// 	// Valid only if PrefetchValues is true.
		// 	PrefetchSize int
		// 	// PrefetchValues Indicates whether we should prefetch values during
		// 	// iteration and store them.
		// 	PrefetchValues bool
		// 	Reverse        bool // Direction of iteration. False is forward, true is backward.
		// 	AllVersions    bool // Fetch all valid versions of the same key.
		// 	InternalAccess bool // Used to allow internal access to badger keys.

		// 	Prefix  []byte // Only iterate over this given prefix.
		// 	SinceTs uint64 // Only read data that has version > SinceTs.
		// 	// contains filtered or unexported fields
		// }

		it := txn.NewIterator(opts)
		defer it.Close()

		prefixBytes := []byte(prefix)
		it.Seek(prefixBytes)

		for ; it.Valid(); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			item := it.Item()
			key := string(item.Key())
			version := int64(item.Version())

			if !strings.HasPrefix(key, prefix) {
				break
			}

			if version <= startRev || version > endRev {
				continue
			}

			if existing, ok := keyEvents[key]; ok {
				if existing.ModRevision >= version {
					continue
				}
			}

			var value []byte

			err := item.Value(func(val []byte) error {
				value = slices.Clone(val)

				return nil
			})
			if err != nil {
				continue
			}

			kv := &server.KeyValue{
				Key:            key,
				CreateRevision: version,
				ModRevision:    version,
				Value:          value,
			}

			keyOpts := badgerdb.DefaultIteratorOptions
			keyOpts.AllVersions = true
			keyOpts.PrefetchValues = false

			keyIt := txn.NewKeyIterator([]byte(key), keyOpts)
			for keyIt.Rewind(); keyIt.Valid(); keyIt.Next() {
				versionItem := keyIt.Item()

				itemVersion := int64(versionItem.Version())
				if itemVersion <= endRev {
					kv.CreateRevision = min(kv.CreateRevision, itemVersion)
				}
			}
			keyIt.Close()

			if expiresAt := item.ExpiresAt(); expiresAt > 0 {
				currentTime := uint64(time.Now().Unix())
				if expiresAt > currentTime {
					kv.Lease = int64(expiresAt - currentTime)
				}
			}

			keyEvents[key] = kv
		}

		return nil
	})
	if err != nil {
		return err
	}

	var events []*server.Event
	for _, kv := range keyEvents {
		event := &server.Event{
			KV: kv,
		}

		events = append(events, event)
	}

	if len(events) > 0 && atomic.LoadInt32(&w.isClosed) == 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case w.events <- events:
		default:
		}
	}

	return nil
}

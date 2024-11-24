// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

// EventWithRevision is event with it's revision.
type EventWithRevision struct {
	event    *server.Event
	revision int64
}

func (b *Backend) streamEventsFromDisk(ctx context.Context, prefix string, startRevision int64, eventsChan chan<- []*server.Event) error {
	// normalize prefix
	prefix = strings.TrimSuffix(strings.TrimPrefix(prefix, "\xff"), "/")

	// collect all events
	allEvents := make([]EventWithRevision, 0)

	// walk the directory tree to find all relevant events
	err := filepath.WalkDir(b.DataBasePath, func(fullPath string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // next
		}

		if fullPath == b.DataBasePath || !d.Type().IsRegular() {
			return nil // next
		}

		// get the key path
		key, err := filepath.Rel(b.DataBasePath, filepath.Dir(fullPath))
		if err != nil {
			return nil // next
		}

		// check prefix match
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil // next
		}

		// parse info from filename
		info := NewInfo(d.Name())
		if info.IsZero() {
			return nil // next
		}

		// check revision
		if info.ModRevision <= startRevision {
			return nil // next
		}

		// read content
		content, err := os.ReadFile(fullPath)
		if err != nil {
			return nil // next
		}

		// create the event based on type
		eventType := info.GetEventType()

		var event *server.Event

		currentKV := &server.KeyValue{
			Key:            key,
			CreateRevision: info.CreateRevision,
			ModRevision:    info.ModRevision,
			Value:          content,
			Lease:          info.GetLeaseTime(),
		}

		switch eventType {
		case CreateEvent:
			event = &server.Event{
				Create: true,
				KV:     currentKV,
				PrevKV: nil,
			}
		case UpdateEvent:
			prevKV := b.findPreviousKV(key, info.ModRevision)
			event = &server.Event{
				KV:     currentKV,
				PrevKV: prevKV,
			}
		case DeleteEvent:
			// for delete events, we still have PrevKV pointing to the last value
			// but KV should be minimal (just key)
			prevKV := b.findPreviousKV(key, info.ModRevision)
			event = &server.Event{
				Delete: true,
				KV:     &server.KeyValue{Key: key},
				PrevKV: prevKV,
			}
		default:
			return nil // next
		}

		allEvents = append(allEvents, EventWithRevision{
			event:    event,
			revision: info.ModRevision,
		})

		return nil // next
	})
	if err != nil {
		return err
	}

	// sort events by revision
	slices.SortFunc(allEvents, func(a, b EventWithRevision) int {
		return int(a.revision - b.revision)
	})

	// send all events in order
	for _, ev := range allEvents {
		select {
		case <-ctx.Done():
		case eventsChan <- []*server.Event{ev.event}:
		}
	}

	return nil
}

func (b *Backend) findPreviousKV(key string, beforeRevision int64) *server.KeyValue {
	entries, err := os.ReadDir(filepath.Join(b.DataBasePath, key))
	if err != nil {
		return nil
	}

	var (
		bestInfo    Info
		bestContent []byte
	)

	now := time.Now()

	for _, entry := range entries {
		info := NewInfo(entry.Name())
		if info.IsZero() || info.ModRevision >= beforeRevision {
			continue
		}

		// for finding previous KV, we want the latest non-deleted version
		if !info.HasExpired(now) && (bestInfo.IsZero() || info.ModRevision > bestInfo.ModRevision) {
			bestInfo = info

			content, err := os.ReadFile(filepath.Join(b.DataBasePath, key, entry.Name()))
			if err == nil {
				bestContent = content
			}
		}
	}

	if bestInfo.IsZero() {
		return nil
	}

	return &server.KeyValue{
		Key:            key,
		CreateRevision: bestInfo.CreateRevision,
		ModRevision:    bestInfo.ModRevision,
		Value:          bestContent,
		Lease:          bestInfo.GetLeaseTime(),
	}
}

func (b *Backend) sendEvent(key string, event *server.Event) {
	if event == nil {
		return
	}

	getEventRevision := func(event *server.Event) int64 {
		if event.Delete {
			// for delete events, use the revision from PrevKV
			if event.PrevKV != nil {
				return event.PrevKV.ModRevision + 1 // next revision after the deleted item
			}

			return -1
		}

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

		// check revision
		if eventRev <= w.startRevision {
			return true
		}

		// check prefix match
		if w.prefix != "" && !strings.HasPrefix(key, w.prefix) {
			return true
		}

		// send event asynchronously
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

func (b *Backend) watch(ctx context.Context, prefix string, startRevision int64) server.WatchResult {
	events := make(chan []*server.Event, 1000)
	rev := b.ReadCounter()

	id := time.Now().UnixNano()
	w := &watcher{
		id:            id,
		prefix:        prefix,
		startRevision: startRevision,
		events:        events,
	}

	// register watcher
	b.watchers.Store(id, w)

	// stream historical events if needed
	if startRevision < rev {
		go func() {
			if err := b.streamEventsFromDisk(ctx, prefix, startRevision, events); err != nil {
				panic(err)
			}
		}()
	}

	// cleanup on context cancellation
	go func() {
		<-ctx.Done()

		if atomic.CompareAndSwapInt32(&w.isClosed, 0, 1) {
			b.watchers.Delete(id)
			close(events)
		}
	}()

	return server.WatchResult{
		Events:          events,
		CurrentRevision: rev,
	}
}

func (b *Backend) Watch(ctx context.Context, prefix string, startRevision int64) server.WatchResult {
	return b.watch(ctx, prefix, startRevision)
}

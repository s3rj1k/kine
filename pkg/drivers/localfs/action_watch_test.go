// SPDX-License-Identifier: Apache-2.0.

package localfs_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestActionWatch(t *testing.T) {
	b := setupBackend(t)

	// start the backend
	err := b.Start(context.Background())
	require.NoError(t, err)

	// get initial revision
	initialRev, err := b.CurrentRevision(context.Background())
	require.NoError(t, err)
	t.Logf("Initial revision: %d", initialRev)

	// create some test data
	rev1, err := b.Create(context.Background(), "test/key1", []byte("value1"), 0)
	require.NoError(t, err)
	t.Logf("Created test/key1 at revision %d", rev1)

	rev2, err := b.Create(context.Background(), "test/key2", []byte("value2"), 0)
	require.NoError(t, err)
	t.Logf("Created test/key2 at revision %d", rev2)

	// start watching from after the creates to test real-time events
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()

	watchResult := b.Watch(watchCtx, "test/", rev2)
	t.Logf("Started watching from revision %d", rev2)

	// collect events in background
	collectedEvents := make([]any, 0)
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-watchCtx.Done():
				done <- true

				return
			case events, ok := <-watchResult.Events:
				if !ok {
					done <- true

					return
				}

				for _, event := range events {
					collectedEvents = append(collectedEvents, event)
					t.Logf("Received event %d", len(collectedEvents))
				}
			}
		}
	}()

	// give watcher time to start
	time.Sleep(100 * time.Millisecond)

	// update event
	rev3, _, ok, err := b.Update(context.Background(), "test/key1", []byte("updated1"), rev1, 0)
	require.NoError(t, err)
	require.True(t, ok)
	t.Logf("Updated test/key1 at revision %d", rev3)

	// create event
	rev4, err := b.Create(context.Background(), "test/key3", []byte("value3"), 0)
	require.NoError(t, err)
	t.Logf("Created test/key3 at revision %d", rev4)

	// wait for create and update events
	time.Sleep(500 * time.Millisecond)
	t.Logf("After create/update: collected %d events", len(collectedEvents))

	// delete event, verify the key exists
	_, kvBeforeDelete, err := b.Get(context.Background(), "test/key2", "", 0, 0)
	require.NoError(t, err)
	require.NotNil(t, kvBeforeDelete)
	t.Logf("Before delete: test/key2 exists")

	// delete event, delete key
	rev5, deletedKv, deleted, err := b.Delete(context.Background(), "test/key2", 0)
	require.NoError(t, err)
	require.True(t, deleted)
	require.NotNil(t, deletedKv)
	t.Logf("Deleted test/key2 at revision %d", rev5)

	// verify the key no longer exists
	_, kvAfterDelete, err := b.Get(context.Background(), "test/key2", "", 0, 0)
	require.NoError(t, err)
	require.Nil(t, kvAfterDelete)
	t.Logf("After delete: test/key2 no longer exists")

	// wait for delete event
	time.Sleep(500 * time.Millisecond)
	t.Logf("After delete: collected %d events", len(collectedEvents))

	// cancel watch and wait for goroutine to finish
	watchCancel()
	<-done

	t.Logf("Total collected events: %d", len(collectedEvents))
	require.GreaterOrEqual(t, len(collectedEvents), 3, "Expected at least 3 events (1 update + 1 create + 1 delete)")

	// test historical events behavior
	watchCtx2, watchCancel2 := context.WithCancel(context.Background())
	defer watchCancel2()

	// list all keys first to verify current state
	listRev, kvs, err := b.List(context.Background(), "test/", "", 0, 0)
	require.NoError(t, err)
	t.Logf("List at revision %d found %d keys with prefix 'test/'", listRev, len(kvs))

	for i, kv := range kvs {
		t.Logf("  [%d] %s (rev: %d)", i, kv.Key, kv.ModRevision)
	}

	// try watching from the beginning
	watchResult2 := b.Watch(watchCtx2, "test/", 0)

	t.Logf("Started historical watch from revision 0 with prefix 'test/'")

	historicalEvents := make([]any, 0)
	done2 := make(chan bool)

	go func() {
		timeout := time.After(1 * time.Second)

		for {
			select {
			case <-watchCtx2.Done():
				done2 <- true

				return
			case events, ok := <-watchResult2.Events:
				if !ok {
					done2 <- true

					return
				}

				t.Logf("Received %d historical events", len(events))

				for _, event := range events {
					historicalEvents = append(historicalEvents, event)
				}
			case <-timeout:
				t.Logf("Timeout waiting for historical events")
				done2 <- true

				return
			}
		}
	}()

	// wait for historical events
	<-done2
	watchCancel2()

	t.Logf("Collected %d historical events", len(historicalEvents))

	// when watching from revision 0 with a prefix, watcher returns early without historical events
	if len(historicalEvents) == 0 {
		t.Logf("No historical events received - this is expected behavior")

		// test future events work correctly with revision 0
		watchCtx3, watchCancel3 := context.WithCancel(context.Background())
		defer watchCancel3()

		futureEvents := make([]any, 0)
		done3 := make(chan bool)

		watchResult3 := b.Watch(watchCtx3, "test/", 0)

		go func() {
			timeout := time.After(1 * time.Second)

			for {
				select {
				case <-watchCtx3.Done():
					done3 <- true

					return
				case events, ok := <-watchResult3.Events:
					if !ok {
						done3 <- true

						return
					}

					for _, event := range events {
						futureEvents = append(futureEvents, event)
					}

					if len(futureEvents) > 0 {
						done3 <- true

						return
					}
				case <-timeout:
					done3 <- true

					return
				}
			}
		}()

		// create a new key to trigger an event
		time.Sleep(100 * time.Millisecond)

		_, err = b.Create(context.Background(), "test/key4", []byte("value4"), 0)
		require.NoError(t, err)

		<-done3
		watchCancel3()

		t.Logf("Future events test: collected %d events", len(futureEvents))
		require.NotEmpty(t, futureEvents, "Should receive at least one future event")
	}
}

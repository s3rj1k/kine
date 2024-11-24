// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

func (b *Backend) delete(_ context.Context, key string, revision int64) (*server.KeyValue, bool, error) {
	entries, err := os.ReadDir(filepath.Join(b.DataBasePath, key)) // get list of all entries for key
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil // key doesn't exist, not deleted
		}

		return nil, false, err
	}

	now := time.Now()

	var latestInfo Info

	for i := range entries { // find the latest valid version
		val := NewInfo(entries[i].Name())
		if val.IsZero() || val.HasExpired(now) {
			continue
		}

		if latestInfo.IsZero() || val.ModRevision > latestInfo.ModRevision {
			latestInfo = val
		}
	}

	// if no valid entries exist, key is already deleted
	if latestInfo.IsZero() {
		return nil, false, nil
	}

	// check if the revision matches
	if revision > 0 && latestInfo.ModRevision != revision {
		return nil, false, nil // revision mismatch, don't delete
	}

	oldLoc := filepath.Join(b.DataBasePath, key, latestInfo.String())

	// read content from the latest version
	content, err := os.ReadFile(oldLoc)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}

		return nil, false, err
	}

	// create a new deletion revision for this delete operation
	newInfo := Info{
		CreateRevision: latestInfo.CreateRevision,
		ModRevision:    b.IncrementCounter(), // create a new revision for the delete
		CreationTime:   now.Unix(),
		ExpireTime:     now.Unix(), // mark file as expired
	}

	newLoc := filepath.Join(b.DataBasePath, key, newInfo.String())

	// rename the latest file to mark it as deleted
	err = os.Rename(oldLoc, newLoc)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}

		return nil, false, err
	}

	// build the KeyValue for the deleted entry
	kv := &server.KeyValue{
		Key:            key,
		CreateRevision: latestInfo.CreateRevision,
		ModRevision:    newInfo.ModRevision, // use the new deletion revision
		Value:          content,
		Lease:          latestInfo.GetLeaseTime(),
	}

	return kv, true, nil
}

func (b *Backend) Delete(ctx context.Context, key string, revision int64) (
	int64 /*revision*/, *server.KeyValue /*kv*/, bool /*deleted*/, error,
) {
	b.actionsLock.Lock()
	defer b.actionsLock.Unlock()

	kv, deleted, err := b.delete(ctx, key, revision)
	if err != nil {
		return b.ReadCounter(), kv, deleted, err
	}

	if !deleted {
		return b.ReadCounter(), kv, deleted, nil
	}

	if kv == nil {
		// entry was already deleted
		return b.ReadCounter(), nil, true, nil
	}

	b.sendEvent(key, &server.Event{
		Delete: true,
		KV:     &server.KeyValue{Key: key},
		PrevKV: kv,
	})

	return kv.ModRevision, kv, deleted, nil
}

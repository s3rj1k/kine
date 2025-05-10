// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/k3s-io/kine/pkg/server"
)

func (b *Backend) delete(_ context.Context, key string, revision int64) (*server.KeyValue, bool, error) {
	entries, err := os.ReadDir(filepath.Join(b.DataBasePath, key)) // get list of all entries for key
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, true, nil
		}

		return nil, false, err
	}

	var (
		info  Info
		found bool
	)

	for i := range entries { // find entry matching specified revision
		val := NewInfo(entries[i].Name())
		if val.IsZero() || val.HasExpired() {
			continue
		}

		if revision > 0 && val.ModRevision == revision {
			info = val
			found = true
			break
		}

		if revision == 0 {
			info = val
			found = true
		}
	}

	// if no valid entry found, treat as already deleted
	if !found {
		return nil, true, nil
	}

	loc := filepath.Join(b.DataBasePath, key, info.String())

	content, err := os.ReadFile(loc) // read entry content
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, true, nil
		}

		return nil, false, err
	}

	// create a new deletion revision for this delete operation
	newInfo := Info{
		CreateRevision: info.CreateRevision,
		ModRevision:    b.IncrementCounter(), // create a new revision for the delete
		CreationTime:   info.CreationTime,
		ExpireTime:     info.CreationTime, // mark file as expired
	}

	// move the old file to the new name with expired timestamp
	err = os.Rename(loc, filepath.Join(b.DataBasePath, key, newInfo.String()))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, true, nil
		}

		return nil, false, err
	}

	// build the KeyValue for the deleted entry
	kv := &server.KeyValue{
		Key:            key,
		CreateRevision: info.CreateRevision,
		ModRevision:    newInfo.ModRevision, // use the new deletion revision
		Value:          content,
		Lease:          info.GetLeaseTime(),
	}

	return kv, true, nil
}

func (b *Backend) Delete(ctx context.Context, key string, revision int64) (
	int64 /*revision*/, *server.KeyValue /*kv*/, bool /*deleted*/, error,
) {
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

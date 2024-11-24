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

	var info Info

	for i := range entries { // find entry matching specified revision
		val := NewInfo(entries[i].Name())
		if val.IsZero() || val.HasExpired() {
			continue
		}

		if revision > 0 && val.ModRevision == revision {
			info = val

			break
		}

		if revision == 0 {
			info = val
		}
	}

	loc := filepath.Join(b.DataBasePath, key, info.String())

	content, err := os.ReadFile(loc) // read entry content
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, true, nil
		}

		return nil, false, err
	}

	newInfo := Info{
		CreateRevision: info.CreateRevision,
		ModRevision:    info.ModRevision,
		CreationTime:   info.CreationTime,
		ExpireTime:     info.CreationTime, // mark file as expired
	}

	err = os.Rename(loc, filepath.Join(filepath.Dir(loc), newInfo.String())) // expire entry
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, true, nil
		}

		return nil, false, err
	}

	for _, entry := range entries { // expire all older revisions
		val := NewInfo(entry.Name())
		if val.IsZero() || val.HasExpired() || val.ModRevision > revision {
			continue
		}

		val.ExpireTime = val.CreationTime // mark file as expired

		_ = os.Rename(
			filepath.Join(b.DataBasePath, key, entry.Name()),
			filepath.Join(b.DataBasePath, key, val.String()),
		)
	}

	kv := &server.KeyValue{
		Key:            key,
		CreateRevision: info.CreateRevision,
		ModRevision:    info.ModRevision,
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
		return 0, kv, deleted, err
	}

	if !deleted {
		return revision, kv, deleted, nil
	}

	if kv != nil {
		revision = kv.ModRevision
	}

	b.sendEvent(key, &server.Event{
		Delete: true,
		KV:     &server.KeyValue{},
		PrevKV: kv,
	})

	return revision, kv, deleted, nil
}

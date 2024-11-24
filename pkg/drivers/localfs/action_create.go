// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

func (b *Backend) create(_ context.Context, key string, value []byte, createRevision, lease int64) (*server.KeyValue, error) {
	if key == "" {
		return nil, server.ErrNotSupported
	}

	loc := filepath.Join(b.DataBasePath, key) // key location

	err := os.MkdirAll(loc, DefaultDirectoryMode) // ensure key directory existence
	if err != nil {
		return nil, err
	}

	info := Info{
		ModRevision:  b.IncrementCounter(),
		CreationTime: time.Now().Unix(),
	}

	if createRevision == 0 {
		info.CreateRevision = info.ModRevision
	} else {
		info.CreateRevision = createRevision
	}

	if lease > 0 {
		info.ExpireTime = info.CreationTime + lease
	}

	fullPath := filepath.Join(loc, info.String())

	f, err := os.OpenFile(
		fullPath,
		DefaultDataFileOpenFlags,
		DefaultFileMode,
	)
	if err != nil {
		return nil, err
	}

	_, err = f.Write(value)
	if err != nil {
		f.Close()

		return nil, err
	}

	err = f.Close()
	if err != nil {
		return nil, err
	}

	return &server.KeyValue{
		Key:            key,
		CreateRevision: info.CreateRevision,
		ModRevision:    info.ModRevision,
		Value:          value,
		Lease:          lease,
	}, nil
}

func (b *Backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64 /*revision*/, error) {
	b.actionsLock.Lock()
	defer b.actionsLock.Unlock()

	now := time.Now()

	entries, err := os.ReadDir(filepath.Join(b.DataBasePath, key)) // get list of all entries for key
	if err == nil {
		for _, el := range entries {
			if val := NewInfo(el.Name()); val.IsZero() || val.HasExpired(now) {
				continue
			}

			return b.ReadCounter(), server.ErrKeyExists // key exists, at least one entry did not expire
		}
	}

	kv, err := b.create(ctx, key, value, 0, lease)
	if err != nil {
		return b.ReadCounter(), err
	}

	event := &server.Event{
		Create: true,
		KV:     kv,
		PrevKV: nil,
	}

	b.sendEvent(key, event)

	return kv.ModRevision, nil
}

// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"cmp"
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/k3s-io/kine/pkg/server"
)

var ErrFileNotFound = errors.New("blob not found")

func (b *Backend) Get(_ context.Context, key, rangeEnd string, limit, revision int64) (int64 /*revision*/, *server.KeyValue, error) {
	kv := &server.KeyValue{
		Key:            key,
		CreateRevision: cmp.Or(revision, b.ReadCounter()),
		ModRevision:    cmp.Or(revision, b.ReadCounter()),
	}

	keys, err := ReadDirNames(filepath.Join(b.DataBasePath, key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cmp.Or(revision, b.ReadCounter()), nil, nil
		}

		return revision, nil, err
	}

	var info Info

	for i := range keys {
		val := NewInfo(keys[i])
		if val.IsZero() || val.HasExpired() {
			continue
		}

		if val.Rev == revision {
			info = val

			break
		}
	}

	content, err := os.ReadFile(filepath.Join(key, info.String()))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cmp.Or(revision, b.ReadCounter()), kv, ErrFileNotFound
		}

		return revision, kv, err
	}

	kv.CreateRevision = cmp.Or(info.Rev, b.ReadCounter())
	kv.ModRevision = cmp.Or(info.Rev, b.ReadCounter())
	kv.Value = content
	kv.Lease = info.Expires

	return info.Rev, kv, nil
}

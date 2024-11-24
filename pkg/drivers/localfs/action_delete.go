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

func (b *Backend) Delete(_ context.Context, key string, revision int64) (int64 /*revision*/, *server.KeyValue, bool, error) {
	keys, err := ReadDirNames(filepath.Join(b.DataBasePath, key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cmp.Or(revision, b.ReadCounter()), nil, true, nil
		}

		return revision, nil, false, err
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

	loc := filepath.Join(key, info.String())

	content, err := os.ReadFile(loc)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cmp.Or(revision, b.ReadCounter()), nil, true, nil
		}

		return revision, nil, false, err
	}

	kv := &server.KeyValue{
		Key:         key,
		ModRevision: revision,
		Value:       content,
	}

	info.Expires = info.Ctime

	err = os.Rename(loc, filepath.Join(filepath.Dir(loc), info.String()))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cmp.Or(revision, b.ReadCounter()), nil, true, nil
		}

		return revision, nil, false, err
	}

	return revision, kv, true, nil
}

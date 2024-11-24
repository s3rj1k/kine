// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/k3s-io/kine/pkg/server"
)

func (b *Backend) List(_ context.Context, prefix, _ string, limit, revision int64) (int64 /*revision*/, []*server.KeyValue, error) {
	keys, err := os.ReadDir(b.DataBasePath)
	if err != nil {
		return revision, nil, err
	}

	kvs := make([]*server.KeyValue, 0, len(keys))

	lastFoundRev := int64(0)
	prefix = strings.TrimSuffix(prefix, "/")

KEYS:
	for _, el := range keys {
		if !el.IsDir() || el.Type().IsRegular() {
			continue
		}

		key := el.Name()

		if prefix != "" && prefix != "*" && !strings.HasPrefix(key, prefix) {
			continue
		}

		names, err := ReadDirNames(key)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return revision, nil, err
		}

		for i := range names {
			info := NewInfo(names[i])
			if info.IsZero() || info.HasExpired() {
				continue
			}

			content, err := os.ReadFile(filepath.Join(key, names[i]))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}

				return revision, nil, err
			}

			kvs = append(kvs, &server.KeyValue{
				Key:            key,
				CreateRevision: info.Rev,
				ModRevision:    info.Rev,
				Value:          content,
				Lease:          info.Expires,
			})

			lastFoundRev = info.Rev

			if revision == info.Rev {
				break KEYS
			}

			if limit > 0 && len(kvs) >= int(limit) {
				break KEYS
			}
		}
	}

	return lastFoundRev, kvs, nil
}

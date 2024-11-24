// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"cmp"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

func (b *Backend) Count(_ context.Context, prefix, _ string, revision int64) (int64 /*revision*/, int64 /*count*/, error) {
	var count int64

	items, err := os.ReadDir(b.DataBasePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cmp.Or(revision, b.ReadCounter()), count, ErrFileNotFound
		}

		return revision, count, err
	}

	prefix = strings.TrimSuffix(prefix, "/")

	countFunc := func(keys []string, revision int64) int64 {
		for i := range keys {
			val := NewInfo(keys[i])
			if val.IsZero() || val.HasExpired() {
				continue
			}

			if val.Rev == revision {
				break
			}

			count++
		}

		return count
	}

	for _, el := range items {
		if !el.IsDir() || el.Type().IsRegular() {
			continue
		}

		key := el.Name()

		if prefix != "" && prefix != "*" && !strings.HasPrefix(key, prefix) {
			continue
		}

		keys, err := ReadDirNames(filepath.Join(b.DataBasePath, key))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return cmp.Or(revision, b.ReadCounter()), count, ErrFileNotFound
			}

			return revision, count, err
		}

		count += countFunc(keys, revision)
	}

	return revision, count, nil
}

// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

func countNames(key string, revision int64) (int64, error) {
	names, err := ReadDirNames(key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return revision, ErrFileNotFound
		}

		return revision, err
	}

	count, _ := filterNames(names, revision)

	return count, nil
}

func (*Backend) Count(_ context.Context, prefix, _ string, revision int64) (storeRevision int64, count int64, err error) {
	defer func() {
		storeRevision = ReadCounter()
	}()

	dbDirectory := getDataBaseDirectory()

	items, err := os.ReadDir(dbDirectory)
	if err != nil {
		return storeRevision, 0, err
	}

	prefix = strings.TrimSuffix(prefix, "/")

	for _, el := range items {
		if !el.IsDir() || el.Type().IsRegular() {
			continue
		}

		key := el.Name()

		if prefix != "" && prefix != "*" && !strings.HasPrefix(key, prefix) {
			continue
		}

		n, err := countNames(filepath.Join(dbDirectory, key), revision)
		if err != nil {
			return storeRevision, count, err
		}

		count += n
	}

	return storeRevision, count, nil
}

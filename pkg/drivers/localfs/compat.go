// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type (
	empty             struct{}
	set[T comparable] map[T]empty
)

func (*Backend) Compact(ctx context.Context, revision int64) (int64, error) {
	defer func() {
		if revision == 0 {
			revision = ReadCounter()
		}
	}()

	dbDirectory := getDataBaseDirectory()
	dbRecords := make(map[string]set[Info])

	// first pass: collect file information
	if err := filepath.WalkDir(dbDirectory, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if !d.Type().IsRegular() {
			return nil
		}

		dir, file := filepath.Split(path)
		key := filepath.Base(filepath.Dir(dir))

		info, err := NewInfo(file)
		if err != nil {
			return nil
		}

		{
			if dbRecords[key] == nil {
				dbRecords[key] = make(set[Info])
			}

			dbRecords[key][info] = empty{}
		}

		return nil
	}); err != nil {
		return revision, err
	}

	// second pass: delete expired and old revision files
	for key, infos := range dbRecords {
		list := make([]Info, 0, len(infos))

		{
			for el := range infos {
				list = append(list, el)
			}

			slices.SortFunc(list, func(a, b Info) int {
				return int(a.Rev - b.Rev)
			})
		}

		for _, info := range list {
			if info.Expires == 0 || info.Expires > info.Ctime {
				continue
			}

			loc := filepath.Join(dbDirectory, key, info.String())
			_ = os.Remove(loc)
		}

		for _, info := range list {
			if info.Rev >= revision {
				break
			}

			loc := filepath.Join(dbDirectory, key, info.String())
			_ = os.Remove(loc)
		}
	}

	// final pass: remove empty directories
	var emptyDirs []string

	if err := filepath.WalkDir(dbDirectory, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if !d.IsDir() || path == dbDirectory {
			return nil
		}

		if entries, err := os.ReadDir(path); err == nil && len(entries) == 0 {
			emptyDirs = append(emptyDirs, path)
		}

		return nil
	}); err != nil {
		return revision, err
	}

	slices.SortFunc(emptyDirs, func(a, b string) int {
		// reverse sort to handle deeper paths first
		return -1 * strings.Compare(a, b)
	})

	for _, dir := range emptyDirs {
		_ = os.Remove(dir)
	}

	return revision, nil
}

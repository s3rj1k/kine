// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

func isDirEmpty(dirPath string) bool {
	entries, err := os.ReadDir(dirPath)

	return (err == nil && len(entries) == 0)
}

func (b *Backend) cleanupEmptyDirs() error {
	var dirs []string

	if err := filepath.WalkDir(b.DataBasePath, func(fullPath string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if fullPath == b.DataBasePath || !d.IsDir() {
			return nil // next
		}

		dirs = append(dirs, fullPath)

		return nil // next
	}); err != nil {
		return err
	}

	slices.SortFunc(dirs, func(a, b string) int {
		// reverse sort to handle deeper paths first
		return -1 * strings.Compare(a, b)
	})

	for _, dir := range dirs {
		if isDirEmpty(dir) {
			_ = os.Remove(dir)
		}
	}

	return nil
}

func (b *Backend) compact(_ context.Context, revision int64) (int64 /*revision*/, error) {
	var filesToDelete []string

	// collect all files by key to determine what to delete
	keyInfos := make(map[string][]Info)

	// walk the directory tree to collect all infos
	err := filepath.WalkDir(b.DataBasePath, func(fullPath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if fullPath == b.DataBasePath || !d.Type().IsRegular() {
			return nil // next
		}

		info := NewInfo(fullPath)
		if info.IsZero() {
			return nil // we only want to handle valid `Info` paths
		}

		key, err := filepath.Rel(b.DataBasePath, filepath.Dir(fullPath))
		if err != nil {
			return nil // next
		}

		keyInfos[key] = append(keyInfos[key], info)

		return nil // next
	})
	if err != nil {
		return revision, err
	}

	now := time.Now()

	// process each key to determine what to delete
	for key, infos := range keyInfos {
		// sort by ModRevision in descending order
		slices.SortFunc(infos, func(a, b Info) int {
			return int(b.ModRevision - a.ModRevision)
		})

		// find the latest expired entry
		var expiredCutoffRevision int64 = -1
		for _, info := range infos {
			if info.HasExpired(now) && info.ModRevision > expiredCutoffRevision {
				expiredCutoffRevision = info.ModRevision
			}
		}

		// delete all files that should be compacted
		for _, info := range infos {
			if info.ModRevision <= revision || // standard compaction
				info.HasExpired(now) || // expired files
				info.ModRevision <= expiredCutoffRevision { // files older than the latest expired entry
				fullPath := filepath.Join(b.DataBasePath, key, info.String())
				filesToDelete = append(filesToDelete, fullPath)
			}
		}
	}

	// sort the files by filename in ascending order
	slices.Sort(filesToDelete)

	// delete the files in order
	for _, fullPath := range filesToDelete {
		if err := os.Remove(fullPath); err == nil {
			parent := filepath.Dir(fullPath)

			if isDirEmpty(parent) {
				_ = os.Remove(parent)
			}
		}
	}

	return revision, b.cleanupEmptyDirs()
}

func (b *Backend) Compact(ctx context.Context, revision int64) (int64 /*revision*/, error) {
	return b.compact(ctx, revision)
}

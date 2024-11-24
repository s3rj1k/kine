// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

func (b *Backend) list(_ context.Context, prefix, _ string, _, revision int64, withContent bool) (
	[]*server.KeyValue /*kvs*/, error,
) {
	// normalize prefix
	prefix = strings.TrimSuffix(
		strings.TrimPrefix(prefix, "\xff"),
		"/",
	)

	// collect all Info objects by key
	keyInfos := make(map[string][]Info)

	// walk the directory tree to collect all infos
	err := filepath.WalkDir(b.DataBasePath, func(fullPath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if fullPath == b.DataBasePath || !d.Type().IsRegular() {
			return nil // next
		}

		// get the key path (directory relative to base)
		key, err := filepath.Rel(b.DataBasePath, filepath.Dir(fullPath))
		if err != nil {
			return nil // next
		}

		// check if key matches prefix
		if prefix != "" && prefix != "/" && !strings.HasPrefix(key, prefix) {
			return nil // next
		}

		// parse info from filename
		info := NewInfo(d.Name())
		if info.IsZero() {
			return nil // next
		}

		keyInfos[key] = append(keyInfos[key], info)

		return nil // next
	})
	if err != nil && !errors.Is(err, filepath.SkipAll) {
		return nil, err
	}

	// process each key to find valid KeyValue
	validKVs := make([]*server.KeyValue, 0)
	now := time.Now()

	for key, infos := range keyInfos {
		// sort by ModRevision in descending order (newest first)
		slices.SortFunc(infos, func(a, b Info) int {
			return int(b.ModRevision - a.ModRevision)
		})

		var selectedInfo Info

		if revision == 0 {
			// for latest revision, find the most recent non-expired entry
			for _, info := range infos {
				if !info.HasExpired(now) {
					selectedInfo = info

					break
				}
			}
		} else {
			// for specific revision, find the closest entry that doesn't exceed the requested revision
			for _, info := range infos {
				if info.ModRevision <= revision {
					// if this specific revision is expired, treat as deleted
					if info.HasExpired(now) {
						break
					}

					selectedInfo = info

					break
				}
			}
		}

		if selectedInfo.IsZero() {
			continue // no valid entry for this key
		}

		var content []byte

		if withContent {
			content, err = os.ReadFile(filepath.Join(b.DataBasePath, key, selectedInfo.String()))
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}

				return nil, err
			}
		}

		kv := &server.KeyValue{
			Key:            key,
			CreateRevision: selectedInfo.CreateRevision,
			ModRevision:    selectedInfo.ModRevision,
			Value:          content,
			Lease:          selectedInfo.GetLeaseTime(),
		}

		validKVs = append(validKVs, kv)
	}

	// sort all events by revision
	slices.SortFunc(validKVs, func(a, b *server.KeyValue) int {
		return int(a.ModRevision - b.ModRevision)
	})

	return validKVs, nil
}

func (b *Backend) List(ctx context.Context, prefix, startKey string, limit, revision int64) (
	int64 /*revision*/, []*server.KeyValue /*kvs*/, error,
) {
	b.actionsLock.Lock()
	defer b.actionsLock.Unlock()

	kvs, err := b.list(ctx, prefix, startKey, limit, revision, true)
	if err != nil {
		return b.ReadCounter(), nil, err
	}

	// sort by key for consistent ordering
	slices.SortFunc(kvs, func(a, b *server.KeyValue) int {
		return strings.Compare(a.Key, b.Key)
	})

	// apply startKey filter if provided
	if startKey != "" {
		for i := range kvs {
			if kvs[i].Key >= startKey {
				kvs = slices.Clip(kvs[i:])

				break
			}
		}
	}

	// apply limit if provided
	if limit > 0 && len(kvs) > int(limit) {
		kvs = slices.Clip(kvs[:limit])
	}

	currentRev := b.ReadCounter()

	return currentRev, kvs, nil
}

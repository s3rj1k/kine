// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

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

	// collect all Info objects by key to handle expiration logic
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
	m := make(map[string]*server.KeyValue)

	for key, infos := range keyInfos {
		// sort by ModRevision in descending order
		slices.SortFunc(infos, func(a, b Info) int {
			return int(b.ModRevision - a.ModRevision)
		})

		// find the latest expired entry to determine the cutoff
		var expiredCutoffRevision int64 = -1
		for _, info := range infos {
			if info.HasExpired() && info.ModRevision > expiredCutoffRevision {
				expiredCutoffRevision = info.ModRevision
			}
		}

		// find the latest valid entry
		var selectedInfo Info
		for _, info := range infos {
			if !info.HasExpired() && info.ModRevision > expiredCutoffRevision {
				selectedInfo = info

				break
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

		m[key] = kv
	}

	kvs := make([]*server.KeyValue, 0, len(m))

	for _, kv := range m {
		kvs = append(kvs, kv)
	}

	// sort all events by revision
	slices.SortFunc(kvs, func(a, b *server.KeyValue) int {
		return int(a.ModRevision - b.ModRevision)
	})

	if revision == 0 {
		return kvs, nil
	}

	// filter by revision
	filtered := make([]*server.KeyValue, 0)
	for i := range kvs {
		if kvs[i].ModRevision <= revision {
			filtered = append(filtered, kvs[i])
		}
	}

	return filtered, nil
}

func (b *Backend) List(ctx context.Context, prefix, startKey string, limit, revision int64) (
	int64 /*revision*/, []*server.KeyValue /*kvs*/, error,
) {
	kvs, err := b.list(ctx, prefix, startKey, limit, revision, true)

	slices.SortFunc(kvs, func(a, b *server.KeyValue) int {
		return strings.Compare(a.Key, b.Key)
	})

	if startKey != "" {
		for i := range kvs {
			if kvs[i].Key >= startKey {
				kvs = slices.Clip(kvs[i:])
				break
			}
		}
	}

	if limit > 0 && len(kvs) > int(limit) {
		kvs = slices.Clip(kvs[:limit])
	}

	currentRev := b.ReadCounter()

	return currentRev, kvs, err
}

// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"

	"github.com/k3s-io/kine/pkg/server"
)

func (b *Backend) get(_ context.Context, key, _ string, limit, revision int64) (*server.KeyValue, error) {
	entries, err := os.ReadDir(filepath.Join(b.DataBasePath, key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, err
	}

	// convert to Info objects and sort by revision
	infos := make([]Info, 0, len(entries))
	for _, entry := range entries {
		info := NewInfo(entry.Name())
		if info.IsZero() {
			continue
		}

		infos = append(infos, info)
	}

	// sort by ModRevision in descending order
	slices.SortFunc(infos, func(a, b Info) int {
		return int(b.ModRevision - a.ModRevision)
	})

	var selectedInfo Info

	if revision == 0 {
		// get the latest non-expired version
		for _, info := range infos {
			if !info.HasExpired() {
				selectedInfo = info

				break
			}
		}
	} else {
		// get the specific revision or the latest before it
		for _, info := range infos {
			if info.ModRevision <= revision && !info.HasExpired() {
				selectedInfo = info

				break
			}
		}
	}

	if selectedInfo.IsZero() {
		return nil, nil
	}

	content, err := os.ReadFile(filepath.Join(b.DataBasePath, key, selectedInfo.String()))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, err
	}

	kv := &server.KeyValue{
		Key:            key,
		CreateRevision: selectedInfo.CreateRevision,
		ModRevision:    selectedInfo.ModRevision,
		Value:          content,
		Lease:          selectedInfo.GetLeaseTime(),
	}

	return kv, nil
}

func (b *Backend) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (
	int64 /*revision*/, *server.KeyValue /*kv*/, error,
) {
	kv, err := b.get(ctx, key, rangeEnd, limit, revision)
	currentRev := b.ReadCounter()

	if err != nil {
		return currentRev, nil, err
	}

	if kv == nil {
		return currentRev, nil, nil
	}

	return currentRev, kv, nil
}

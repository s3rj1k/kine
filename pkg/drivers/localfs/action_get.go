// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

func (b *Backend) get(_ context.Context, key, rangeEnd string, revision, limit int64) (*server.KeyValue, error) {
	entries, err := os.ReadDir(filepath.Join(b.DataBasePath, key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	// convert to Info objects
	infos := make([]Info, 0, len(entries))

	for _, entry := range entries {
		info := NewInfo(entry.Name())
		if info.IsZero() {
			continue
		}

		infos = append(infos, info)
	}

	// if no valid entries found
	if len(infos) == 0 {
		return nil, nil
	}

	// always sort by ModRevision in descending order for consistency
	slices.SortFunc(infos, func(a, b Info) int {
		return int(b.ModRevision - a.ModRevision)
	})

	now := time.Now()

	var (
		selectedInfo Info
		found        bool
	)

	if revision == 0 {
		// for latest revision, get the newest non-expired entry
		for _, info := range infos {
			// if we encounter a delete marker as the latest entry, the key is deleted
			if info.ExpireTime > 0 && info.ExpireTime <= info.CreationTime {
				return nil, nil
			}

			// skip expired entries
			if info.HasExpired(now) {
				continue
			}

			selectedInfo = info
			found = true
			break
		}
	} else {
		// get the highest revision that's <= requested revision
		var highestRevision int64 = -1

		for _, info := range infos {
			if info.ModRevision <= revision && info.ModRevision > highestRevision {
				highestRevision = info.ModRevision
				selectedInfo = info
				found = true
			}
		}

		// check if the selected entry is a delete marker
		if found && selectedInfo.ExpireTime > 0 && selectedInfo.ExpireTime <= selectedInfo.CreationTime {
			return nil, nil
		}
	}

	// no valid entry found
	if !found {
		return nil, nil
	}

	// read the file content
	content, err := os.ReadFile(filepath.Join(b.DataBasePath, key, selectedInfo.String()))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	// create and return the KeyValue
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
	b.actionsLock.Lock()
	defer b.actionsLock.Unlock()

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

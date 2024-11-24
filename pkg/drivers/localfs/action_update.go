// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"context"

	"github.com/k3s-io/kine/pkg/server"
)

func (b *Backend) update(ctx context.Context, key string, value []byte, revision, lease int64) (
	*server.KeyValue /*prevKV*/, *server.KeyValue /*newKV*/, bool /*updated*/, error,
) {
	prevKV, err := b.get(ctx, key, "", 0, revision)
	if err != nil {
		return prevKV, nil, false, err
	}

	// if key doesn't exist, return false without error
	if prevKV == nil || prevKV.ModRevision == 0 {
		return nil, nil, false, nil
	}

	// if revision doesn't match, return the current value and false
	if revision > 0 && prevKV.ModRevision != revision {
		return prevKV, nil, false, nil
	}

	newKV, err := b.create(ctx, key, value, prevKV.CreateRevision, lease)
	if err != nil {
		return prevKV, nil, false, err
	}

	return prevKV, newKV, true, nil
}

func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (
	int64 /*revision*/, *server.KeyValue /*newKV*/, bool /*updated*/, error,
) {
	b.actionsLock.Lock()
	defer b.actionsLock.Unlock()

	prevKV, newKV, updated, err := b.update(ctx, key, value, revision, lease)
	if err != nil {
		return b.ReadCounter(), nil, updated, err
	}

	if !updated {
		// if we have a previous value but update wasn't performed, return its revision
		if prevKV != nil {
			return prevKV.ModRevision, prevKV, updated, nil
		}

		return b.ReadCounter(), nil, updated, nil
	}

	b.sendEvent(key, &server.Event{
		KV:     newKV,
		PrevKV: prevKV,
	})

	return newKV.ModRevision, newKV, updated, nil
}

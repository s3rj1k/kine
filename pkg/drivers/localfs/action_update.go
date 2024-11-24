// SPDX-License-Identifier: Apache-2.0

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

	if prevKV.ModRevision == 0 {
		return prevKV, nil, false, server.ErrNotSupported
	}

	if revision > 0 && prevKV.ModRevision != revision {
		return prevKV, nil, false, server.ErrFutureRev
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
	prevKV, newKV, updated, err := b.update(ctx, key, value, revision, lease)
	if err != nil {
		return revision, nil, updated, err
	}

	if !updated {
		return prevKV.ModRevision, nil, updated, nil
	}

	rev := max(newKV.ModRevision, prevKV.ModRevision)

	b.sendEvent(key, &server.Event{
		KV:     newKV,
		PrevKV: prevKV,
	})

	return rev, newKV, updated, nil
}

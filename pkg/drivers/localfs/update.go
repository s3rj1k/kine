// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"

	"github.com/k3s-io/kine/pkg/server"
)

var ErrRevisionMismatch = errors.New("revision mismatch")

func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	rev, kv, err := b.Get(ctx, key, "", 0, revision)
	if err != nil {
		if errors.Is(err, ErrFileNotFound) {
			return 0, nil, false, nil
		}

		return 0, nil, false, err
	}

	if kv.ModRevision != revision {
		return rev, kv, false, ErrRevisionMismatch
	}

	if kv.CreateRevision == 0 {
		kv.CreateRevision = rev
	}

	newRev, err := b.Create(ctx, key, value, lease)
	if err != nil {
		return 0, nil, false, err
	}

	return newRev, &server.KeyValue{
		Key:            key,
		CreateRevision: kv.CreateRevision,
		ModRevision:    newRev,
		Value:          value,
		Lease:          kv.Lease,
	}, true, nil
}

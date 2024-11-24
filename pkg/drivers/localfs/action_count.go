// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"context"
	"slices"
	"strings"

	"github.com/k3s-io/kine/pkg/server"
)

func (b *Backend) Count(ctx context.Context, prefix, startKey string, revision int64) (
	int64 /*revision*/, int64 /*count*/, error,
) {
	b.actionsLock.Lock()
	defer b.actionsLock.Unlock()

	kvs, err := b.list(ctx, prefix, startKey, 0, revision, false)

	slices.SortFunc(kvs, func(a, b *server.KeyValue) int {
		return strings.Compare(a.Key, b.Key)
	})

	if startKey != "" {
		for i := range kvs {
			if kvs[i].Key == startKey {
				kvs = slices.Clip(kvs[i:])

				break
			}
		}
	}

	return b.ReadCounter(), int64(len(kvs)), err
}

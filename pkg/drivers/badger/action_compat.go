// SPDX-License-Identifier: Apache-2.0.

package badger

import (
	"cmp"
	"context"
	"errors"

	badgerdb "github.com/dgraph-io/badger/v4"
)

const DiscardRatio = 0.5

func (b *Backend) Compact(_ context.Context, revision int64) (int64 /*revision*/, error) {
	if revision <= 0 {
		return cmp.Or(revision, int64(b.db.MaxVersion())), nil
	}

	b.db.SetDiscardTs(uint64(revision))

	if err := b.db.RunValueLogGC(DiscardRatio); err != nil {
		if errors.Is(err, badgerdb.ErrNoRewrite) {
			return revision, nil
		}

		if errors.Is(err, badgerdb.ErrRejected) {
			return cmp.Or(revision, int64(b.db.MaxVersion())), err
		}

		return cmp.Or(revision, int64(b.db.MaxVersion())), err
	}

	return cmp.Or(revision, int64(b.db.MaxVersion())), nil
}

// SPDX-License-Identifier: Apache-2.0.

package badger

import (
	"context"
)

func (b *Backend) Start(ctx context.Context) error {
	// https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	_, err := b.Create(ctx, "registry/health", []byte(`{"health":"true"}`), 0)

	return err
}

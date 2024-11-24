// SPDX-License-Identifier: Apache-2.0.

package localfs

import (
	"context"
)

func (b *Backend) start(_ context.Context) error {
	// https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	_, err := b.Create(context.Background(), "registry/health", []byte(`{"health":"true"}`), 0)

	return err
}

func (b *Backend) Start(ctx context.Context) error {
	return b.start(ctx)
}

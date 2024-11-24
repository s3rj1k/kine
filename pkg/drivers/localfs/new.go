// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
)

type Backend struct{}

var backend *Backend

var (
	// ensure Backend implements server.Backend.
	_ server.Backend = (&Backend{})
)

func New(ctx context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	if !filepath.IsAbs(cfg.DataSourceName) {
		return false, nil, fmt.Errorf("database path must be absolute: %s", cfg.DataSourceName)
	}

	if err := os.Setenv(DataBasePathEnvironKey, cfg.DataSourceName); err != nil {
		return false, nil, fmt.Errorf("database path error: %w", err)
	}

	backend = new(Backend)

	return true, backend, nil
}

func init() {
	drivers.Register("localfs", New)
}

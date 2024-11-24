// SPDX-License-Identifier: Apache-2.0.

package localfs_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/k3s-io/kine/pkg/drivers/localfs"
)

func TestNoneThreadSafeCounter(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test_counter")

	value := localfs.NoneThreadSafeReadCounter("/proc/self/test_counter")
	require.Equal(t, int64(0), value, "Reading inaccessible counter should return 0")

	value = localfs.NoneThreadSafeIncrementCounter("/proc/self/test_counter")
	require.Equal(t, int64(-1), value, "Incrementing inaccessible counter should return -1")

	value = localfs.NoneThreadSafeReadCounter(tmpFile)
	require.Equal(t, int64(0), value, "Reading non-existent counter should return 0")

	value = localfs.NoneThreadSafeIncrementCounter(tmpFile)
	require.Equal(t, int64(0), value, "First increment should return 0")

	value = localfs.NoneThreadSafeReadCounter(tmpFile)
	require.Equal(t, int64(0), value, "Counter should be 0 after first increment")

	value = localfs.NoneThreadSafeIncrementCounter(tmpFile)
	require.Equal(t, int64(1), value, "Second increment should return 1")

	value = localfs.NoneThreadSafeReadCounter(tmpFile)
	require.Equal(t, int64(1), value, "Counter should be 1 after second increment")
}

// SPDX-License-Identifier: Apache-2.0.

package localfs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/k3s-io/kine/pkg/drivers/localfs"
)

func TestNewInfo(t *testing.T) {
	t.Run("Valid path", func(t *testing.T) {
		loc := "12345.67890.1620000000.1620086400"

		info := localfs.NewInfo(loc)
		require.Equal(t, int64(12345), info.CreateRevision)
		require.Equal(t, int64(67890), info.ModRevision)
		require.Equal(t, int64(1620000000), info.CreationTime)
		require.Equal(t, int64(1620086400), info.ExpireTime)
	})

	t.Run("Invalid paths", func(t *testing.T) {
		locs := []string{
			"invalid",
			"12345.67890.1620000000",     // missing part
			"12345.67890.abc.1620086400", // non-numeric part
			"",
		}

		for _, loc := range locs {
			t.Run(loc, func(t *testing.T) {
				info := localfs.NewInfo(loc)
				require.True(t, info.IsZero(), "Expected zero Info for invalid path: %s", loc)
			})
		}
	})

	t.Run("Full file path", func(t *testing.T) {
		loc := "/some/directory/12345.67890.1620000000.1620086400"

		info := localfs.NewInfo(loc)
		require.Equal(t, int64(12345), info.CreateRevision)
		require.Equal(t, int64(67890), info.ModRevision)
		require.Equal(t, int64(1620000000), info.CreationTime)
		require.Equal(t, int64(1620086400), info.ExpireTime)
	})
}

func TestInfoString(t *testing.T) {
	info := localfs.Info{
		CreateRevision: 12345,
		ModRevision:    67890,
		CreationTime:   1620000000,
		ExpireTime:     1620086400,
	}

	expected := "00000000000000012345.00000000000000067890.1620000000.1620086400"
	require.Equal(t, expected, info.String())
}

func TestInfoIsZero(t *testing.T) {
	tests := []struct {
		name   string
		info   localfs.Info
		isZero bool
	}{
		{
			name: "non-zero info",
			info: localfs.Info{
				CreateRevision: 12345,
				ModRevision:    67890,
				CreationTime:   1620000000,
				ExpireTime:     1620086400,
			},
			isZero: false,
		},
		{
			name: "zero create revision",
			info: localfs.Info{
				CreateRevision: 0,
				ModRevision:    67890,
				CreationTime:   1620000000,
				ExpireTime:     1620086400,
			},
			isZero: true,
		},
		{
			name: "zero mod revision",
			info: localfs.Info{
				CreateRevision: 12345,
				ModRevision:    0,
				CreationTime:   1620000000,
				ExpireTime:     1620086400,
			},
			isZero: true,
		},
		{
			name: "zero creation time",
			info: localfs.Info{
				CreateRevision: 12345,
				ModRevision:    67890,
				CreationTime:   0,
				ExpireTime:     1620086400,
			},
			isZero: true,
		},
		{
			name: "zero expire time",
			info: localfs.Info{
				CreateRevision: 12345,
				ModRevision:    67890,
				CreationTime:   1620000000,
				ExpireTime:     0,
			},
			isZero: false, // expire time = 0 doesn't make it zero
		},
		{
			name:   "completely zero info",
			info:   localfs.Info{},
			isZero: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.isZero, tt.info.IsZero())
		})
	}
}

func TestInfoHasExpired(t *testing.T) {
	now := time.Now()

	tests := []struct {
		checkTime time.Time
		name      string
		info      localfs.Info
		expected  bool
	}{
		{
			name: "not expired",
			info: localfs.Info{
				CreationTime: now.Unix(),
				ExpireTime:   now.Unix() + 3600, // 1 hour in the future
			},
			checkTime: now,
			expected:  false,
		},
		{
			name: "expired",
			info: localfs.Info{
				CreationTime: now.Unix(),
				ExpireTime:   now.Unix() - 3600, // 1 hour in the past
			},
			checkTime: now,
			expected:  true,
		},
		{
			name: "exact same time",
			info: localfs.Info{
				CreationTime: now.Unix(),
				ExpireTime:   now.Unix(),
			},
			checkTime: now,
			expected:  true, // consider expired when equal
		},
		{
			name: "no expiry",
			info: localfs.Info{
				CreationTime: now.Unix(),
				ExpireTime:   0,
			},
			checkTime: now,
			expected:  false, // no expiry time means not expired
		},
		{
			name: "zero check time",
			info: localfs.Info{
				CreationTime: now.Unix(),
				ExpireTime:   now.Unix() + 3600,
			},
			checkTime: time.Time{}, // zero time
			expected:  false,       // zero time means not expired
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.info.HasExpired(tt.checkTime))
		})
	}
}

func TestInfoGetLeaseTime(t *testing.T) {
	tests := []struct {
		name     string
		info     localfs.Info
		expected int64
	}{
		{
			name: "positive lease time",
			info: localfs.Info{
				CreationTime: 1620000000,
				ExpireTime:   1620086400, // 24 hours later
			},
			expected: 86400, // 24 hours in seconds
		},
		{
			name: "zero lease time",
			info: localfs.Info{
				CreationTime: 1620000000,
				ExpireTime:   1620000000,
			},
			expected: 0,
		},
		{
			name: "negative lease time (should return 0)",
			info: localfs.Info{
				CreationTime: 1620086400,
				ExpireTime:   1620000000, // 24 hours earlier
			},
			expected: 0, // should be capped at 0
		},
		{
			name: "no expiry time",
			info: localfs.Info{
				CreationTime: 1620000000,
				ExpireTime:   0,
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.info.GetLeaseTime())
		})
	}
}

func TestInfoGetEventType(t *testing.T) {
	tests := []struct {
		name     string
		info     localfs.Info
		expected int
	}{
		{
			name: "create event - equal revisions",
			info: localfs.Info{
				CreateRevision: 12345,
				ModRevision:    12345,
				CreationTime:   1620000000,
				ExpireTime:     1620086400,
			},
			expected: localfs.CreateEvent,
		},
		{
			name: "update event - create < mod",
			info: localfs.Info{
				CreateRevision: 12345,
				ModRevision:    67890,
				CreationTime:   1620000000,
				ExpireTime:     1620086400,
			},
			expected: localfs.UpdateEvent,
		},
		{
			name: "delete event",
			info: localfs.Info{
				CreateRevision: 12345,
				ModRevision:    67890,
				CreationTime:   1620086400,
				ExpireTime:     1620000000, // expired (earlier than creation)
			},
			expected: localfs.DeleteEvent,
		},
		{
			name: "delete event - equal times",
			info: localfs.Info{
				CreateRevision: 12345,
				ModRevision:    67890,
				CreationTime:   1620000000,
				ExpireTime:     1620000000, // expired (equal to creation)
			},
			expected: localfs.DeleteEvent,
		},
		{
			name:     "unknown event - zero info",
			info:     localfs.Info{},
			expected: localfs.UnknownEvent,
		},
		{
			name: "unknown event - create > mod",
			info: localfs.Info{
				CreateRevision: 67890,
				ModRevision:    12345,
				CreationTime:   1620000000,
				ExpireTime:     1620086400,
			},
			expected: localfs.UnknownEvent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.info.GetEventType())
		})
	}
}

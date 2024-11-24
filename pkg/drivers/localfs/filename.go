// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

type Info struct {
	Rev     int64
	Ctime   int64
	Expires int64
}

func NewInfo(loc string) (info Info) {
	parts := strings.Split(filepath.Base(loc), ".")
	if len(parts) < 3 {
		return Info{}
	}

	rev, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return Info{}
	}

	ctime, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return Info{}
	}

	expires, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return Info{}
	}

	return Info{
		Rev:     rev,
		Ctime:   ctime,
		Expires: expires,
	}
}

func (i Info) String() string {
	return fmt.Sprintf("%020d.%d.%d", i.Rev, i.Ctime, i.Expires)
}

func (i Info) IsZero() bool {
	return i.Rev == 0 || i.Ctime == 0
}

func (i Info) HasExpired() bool {
	return i.Expires != 0 && i.Expires <= i.Ctime
}

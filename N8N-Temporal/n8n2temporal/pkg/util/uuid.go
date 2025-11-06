package util

import (
	"github.com/google/uuid"
	"strconv"
	"time"
)

func UUID() string {
	uid, err := uuid.NewUUID()
	if err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return strconv.FormatInt(int64(uid.ID()), 10)
}

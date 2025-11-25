package util

import (
	"github.com/google/uuid"
)

func UUID() string {
	return uuid.New().String()
	//uid, err := uuid.New().String()
	//if err != nil {
	//	return strconv.FormatInt(time.Now().UnixNano(), 10)
	//}
	//return strconv.FormatInt(int64(uid.ID()), 10)
}

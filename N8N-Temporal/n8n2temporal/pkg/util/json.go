package util

import (
	"encoding/json"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func PbToMap(pb proto.Message) (map[string]interface{}, error) {
	// 先转为 JSON
	data, err := protojson.Marshal(pb)
	if err != nil {
		return nil, err
	}
	// 再转为 map
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	return result, err
}

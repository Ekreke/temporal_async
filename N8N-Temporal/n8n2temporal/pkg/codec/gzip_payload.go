package codec

import (
	"bytes"
	"compress/gzip"
	"io"

	commonpb "go.temporal.io/api/common/v1"
)

// GzipPayloadCodec 自动压缩超过阈值的 Payload
type GzipPayloadCodec struct {
	MinBytes int // 压缩阈值，例如 1024 (1KB)
}

// NewGzipPayloadCodec 创建实例
func NewGzipPayloadCodec(minBytes int) *GzipPayloadCodec {
	if minBytes <= 0 {
		minBytes = 1024 // 默认 1KB
	}
	return &GzipPayloadCodec{MinBytes: minBytes}
}

// Encode 实现加密/压缩逻辑 (ToPayloads 后调用)
func (c *GzipPayloadCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		// 复制 Payload 以避免修改原始数据
		newP := &commonpb.Payload{
			Metadata: make(map[string][]byte, len(p.Metadata)),
			Data:     p.Data,
		}
		for k, v := range p.Metadata {
			newP.Metadata[k] = v
		}

		// 如果数据超过阈值，进行压缩
		if len(p.Data) > c.MinBytes {
			var b bytes.Buffer
			w := gzip.NewWriter(&b)
			if _, err := w.Write(p.Data); err != nil {
				return nil, err
			}
			if err := w.Close(); err != nil {
				return nil, err
			}

			newP.Data = b.Bytes()
			// 标记为 gzip 编码，并保留原始编码以便解码时恢复
			newP.Metadata["encoding"] = []byte("binary/gzip") // Temporal 建议使用 binary/ 前缀
			// 也可以选择将原始 encoding 存入另一个 metadata 字段，
			// 但通常 DataConverter 在 Decode 后会自己检查内容或由上层处理。
			// 这里我们简单地覆盖 encoding，Decode 时再改回来是不行的，
			// 因为 DataConverter 需要知道原始格式（如 json/protobuf）。
			// 所以最佳实践是：保留原始 encoding，用额外的 metadata 标记压缩。

			// 更稳妥的做法：
			newP.Metadata["original-encoding"] = p.Metadata["encoding"]
			newP.Metadata["encoding"] = []byte("binary/gzip")
		}
		result[i] = newP
	}
	return result, nil
}

// Decode 实现解密/解压逻辑 (FromPayloads 前调用)
func (c *GzipPayloadCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		newP := &commonpb.Payload{
			Metadata: make(map[string][]byte, len(p.Metadata)),
			Data:     p.Data,
		}
		for k, v := range p.Metadata {
			newP.Metadata[k] = v
		}

		// 检查是否是 gzip 编码
		if string(p.Metadata["encoding"]) == "binary/gzip" {
			r, err := gzip.NewReader(bytes.NewReader(p.Data))
			if err != nil {
				return nil, err
			}
			decompressed, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			if err := r.Close(); err != nil {
				return nil, err
			}

			newP.Data = decompressed
			// 恢复原始编码
			if orig, ok := p.Metadata["original-encoding"]; ok {
				newP.Metadata["encoding"] = orig
				delete(newP.Metadata, "original-encoding")
			} else {
				// 如果没有原始编码记录，通常回退到 json/plain
				newP.Metadata["encoding"] = []byte("json/plain")
			}
		}
		result[i] = newP
	}
	return result, nil
}

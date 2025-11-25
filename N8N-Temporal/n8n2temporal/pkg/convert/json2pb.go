package convert

import (
	"bytes"
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.acme.red/wego/pkg/utils/combinMap/v2"
	"google.golang.org/protobuf/proto"

	"github.com/bufbuild/protocompile"

	"github.com/google/go-github/v66/github"
	"golang.org/x/oauth2"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	desc "google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
)

var pbJsonMarshaler = protojson.MarshalOptions{UseProtoNames: true}

// JsonPB json和PB的相互转换
type JsonPB struct {
	PBFile         string                    `json:"pb_file"` // pb文件路径
	Token          string                    `json:"token"`   // github token，用于提取pb
	fileDescriptor *desc.FileDescriptorProto // 文件描述符
	Path           []string                  `json:"path"`
}

func NewJsonPB(PBFile, Token string, path []string) *JsonPB {
	return &JsonPB{
		PBFile: PBFile,
		Token:  Token,
		Path:   path,
	}
}

// JSONToAnyPB JSON转换成指定PB(返回anyPb)
func (j *JsonPB) JSONToAnyPB(ctx context.Context, messageName string, jsonData []byte) (*anypb.Any, error) {
	fd, err := loadPbDescriptor(ctx, j.PBFile, j.Token)
	if err != nil {
		return nil, fmt.Errorf("failed to load protobuf descriptor: %v", err)
	}
	msgDesc := fd.Messages().ByName(protoreflect.Name(messageName))
	// 动态创建消息并反序列化
	msg := dynamicpb.NewMessage(msgDesc)
	if err := protojson.Unmarshal(jsonData, msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal json data: %w", err)
	}
	if len(j.Path) != 0 {
		dst := dynamicpb.NewMessage(msgDesc)
		mask, err := fieldmaskpb.New(msg, j.Path...)
		if err != nil {
			return nil, err
		}
		Update(mask, dst, msg)
		msg = dst
	}
	// 序列化为 Any
	bin, err := anypb.New(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}
	return bin, nil
}

// AnyPBToJSON anyPb的数据转换成json
func (j *JsonPB) AnyPBToJSON(ctx context.Context, messageName string, pbData *anypb.Any) ([]byte, error) {
	// 查找输入消息描述符
	pb, err := loadPbDescriptor(ctx, j.PBFile, j.Token)
	if err != nil {
		return nil, fmt.Errorf("failed to load protobuf descriptor: %v", err)
	}
	msgDesc := pb.Messages().ByName(protoreflect.Name(messageName))
	// 将JSON反序列化为动态消息
	msg := dynamicpb.NewMessage(msgDesc)
	if err := proto.Unmarshal(pbData.GetValue(), msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal json data: %w", err)
	}
	if len(j.Path) != 0 {
		dst := dynamicpb.NewMessage(msgDesc)
		mask, err := fieldmaskpb.New(msg, j.Path...)
		if err != nil {
			return nil, err
		}
		Update(mask, dst, msg)
		msg = dst
	}
	return pbJsonMarshaler.Marshal(msg)
}

// 缓存已经加载的文件描述符
var descriptorCache = map[string]protoreflect.FileDescriptor{}

// convertToGithubCont 解析GitHub地址
type githubContent struct {
	Scheme string `json:"scheme"` // 协议
	Host   string `json:"host"`   // host地址
	Owner  string `json:"owner"`  // 分组名
	Repo   string `json:"repo"`   // 项目名
	Path   string `json:"path"`   // 文件路径
	Branch string `json:"branch"` // 分支名称
}

// loadPbDescriptor 根据pb文件地址，动态加载Pb文件描述符（目前仅支持内网github）
func loadPbDescriptor(ctx context.Context, pbFileURL string, token string) (protoreflect.FileDescriptor, error) {
	// 走缓存
	if v, ok := descriptorCache[pbFileURL]; ok {
		return v, nil
	}
	// 构建github资源请求
	gHubCont, err := convertToGithubCont(pbFileURL)
	if err != nil {
		return nil, fmt.Errorf("convertToGithubCont err: %w", err)
	}
	// 下载github中pb文件
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
	tc := oauth2.NewClient(ctx, ts)
	baseUrl := fmt.Sprintf("%s://%s/api/v3/", gHubCont.Scheme, gHubCont.Host)
	client, err := github.NewClient(tc).WithEnterpriseURLs(baseUrl, "")
	if err != nil {
		return nil, fmt.Errorf("new github client err: %w", err)
	}
	file, _, _, err := client.Repositories.GetContents(ctx, gHubCont.Owner, gHubCont.Repo, gHubCont.Path, &github.RepositoryContentGetOptions{Ref: gHubCont.Branch})
	if err != nil {
		return nil, fmt.Errorf("github get repo err: %w", err)
	}
	content, err := file.GetContent()
	if err != nil {
		return nil, fmt.Errorf("github get content err: %w", err)
	}
	// 使用 protocompile 解析
	compiler := protocompile.Compiler{
		Resolver: &protocompile.SourceResolver{
			Accessor: func(path string) (io.ReadCloser, error) {
				return buildAccessor(ctx, gHubCont.Path, content)(path)
			},
		},
	}

	// 编译
	files, err := compiler.Compile(ctx, gHubCont.Path)
	if err != nil {
		return nil, fmt.Errorf("protocompile error: %w", err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no file descriptor found")
	}
	fd := files[0]
	descriptorCache[pbFileURL] = fd
	return fd, nil
}

// convertToGithubCont 将 GitHub URL 转换为 github库请求结构体
func convertToGithubCont(webURL string) (*githubContent, error) {
	// 示例输入: https://github.acme.red/mapper/idl/blob/main/proto/mapper/taskargs/v1/args.proto
	u, err := url.Parse(webURL)
	if err != nil {
		return nil, err
	}
	uPath := strings.Split(u.Path, "/")
	var pb = &githubContent{
		Scheme: u.Scheme,
		Host:   u.Host,
	}
	var gPath = make([]string, 0)
	for _, pth := range uPath {
		if pth == "" || pth == "blob" {
			continue
		}
		if pb.Owner == "" {
			pb.Owner = pth
			continue
		}
		if pb.Repo == "" {
			pb.Repo = pth
			continue
		}
		if pb.Branch == "" {
			pb.Branch = pth
			continue
		}
		gPath = append(gPath, pth)
	}
	pb.Path = strings.Join(gPath, "/")
	return pb, nil
}

var buildAccessorCache combinMap.MySyncMap[string, []byte]

// buildAccessor 数据访问器
func buildAccessor(ctx context.Context, mainName string, mainContent string) func(path string) (io.ReadCloser, error) {
	client := http.DefaultClient
	buildAccessorCache.Store(mainName, []byte(mainContent))
	fetch := func(url string) ([]byte, error) {
		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		if resp.StatusCode != 200 {
			b, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
		}
		return io.ReadAll(resp.Body)
	}

	return func(filename string) (io.ReadCloser, error) {
		if b, ok := buildAccessorCache.Load(filename); ok {
			return io.NopCloser(bytes.NewReader(b)), nil
		}
		var u string
		if strings.HasPrefix(filename, "google/protobuf/") {
			u = "https://raw.githubusercontent.com/protocolbuffers/protobuf/main/src/" + filename
		} else if strings.HasPrefix(filename, "google/api/") || strings.HasPrefix(filename, "google/rpc/") {
			u = "https://raw.githubusercontent.com/googleapis/googleapis/master/" + filename
		} else {
			return nil, fmt.Errorf("no accessor rule for %s", filename)
		}
		b, err := fetch(u)
		if err != nil {
			return nil, err
		}
		buildAccessorCache.Store(filename, b)
		return io.NopCloser(bytes.NewReader(b)), nil
	}
}

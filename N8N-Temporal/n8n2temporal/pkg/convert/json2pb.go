package convert

import (
	"bytes"
	"context"
	"fmt"
	"github.acme.red/wego/pkg/utils/combinMap/v2"
	"github.com/google/go-github/v66/github"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"golang.org/x/oauth2"
	"google.golang.org/protobuf/types/known/anypb"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// JsonPB json和PB的相互转换
type JsonPB struct {
	PBFile         string               `json:"pb_file"` // pb文件路径
	Token          string               `json:"token"`   // github token，用于提取pb
	fileDescriptor *desc.FileDescriptor // 文件描述符
}

func NewJsonPB(PBFile, Token string) *JsonPB {
	return &JsonPB{
		PBFile: PBFile,
		Token:  Token,
	}
}

// SetFileDescriptor 设置文件描述符
func (j *JsonPB) SetFileDescriptor(d *desc.FileDescriptor) {
	j.fileDescriptor = d
}

// GetFileDescriptor 获取文件描述符
func (j *JsonPB) GetFileDescriptor(ctx context.Context) (*desc.FileDescriptor, error) {
	if j.fileDescriptor != nil {
		return j.fileDescriptor, nil
	}
	return loadPbDescriptor(ctx, j.PBFile, j.Token)
}

// GetMessageDescriptor 获取消息描述符
func (j *JsonPB) GetMessageDescriptor(ctx context.Context, msgName string) (*desc.MessageDescriptor, error) {
	fd, err := j.GetFileDescriptor(ctx)
	if err != nil {
		return nil, err
	}
	return findMessageDescriptor(fd, msgName)
}

// JSONToAnyPB JSON转换成指定PB(返回anyPb)
func (j *JsonPB) JSONToAnyPB(ctx context.Context, messageName string, jsonData []byte) (*anypb.Any, error) {
	// 查找输入消息描述符
	inputMsgDesc, err := j.GetMessageDescriptor(ctx, messageName)
	if err != nil {
		return nil, fmt.Errorf("failed to find message descriptor: %w", err)
	}
	// 将JSON反序列化为动态消息
	inputMsg := dynamic.NewMessage(inputMsgDesc)
	if err := inputMsg.UnmarshalJSON(jsonData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal json data: %w", err)
	}
	binaryData, err := inputMsg.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json data: %w", err)
	}
	// 构建anypb.Any
	typeURL := fmt.Sprintf("type.googleapis.com/%s", inputMsgDesc.GetFullyQualifiedName())
	anyData := &anypb.Any{
		TypeUrl: typeURL,
		Value:   binaryData,
	}
	return anyData, nil
}

// AnyPBToJSON anyPb的数据转换成json
func (j *JsonPB) AnyPBToJSON(ctx context.Context, messageName string, pbData *anypb.Any) ([]byte, error) {
	// 查找输入消息描述符
	inputMsgDesc, err := j.GetMessageDescriptor(ctx, messageName)
	if err != nil {
		return nil, fmt.Errorf("failed to find message descriptor: %w", err)
	}
	// 将JSON反序列化为动态消息
	inputMsg := dynamic.NewMessage(inputMsgDesc)
	if err := inputMsg.Unmarshal(pbData.GetValue()); err != nil {
		return nil, fmt.Errorf("failed to unmarshal json data: %w", err)
	}
	return inputMsg.MarshalJSON()
}

// 解析jithub地址，分块存储
type githubContent struct {
	Scheme string `json:"scheme"` // 协议
	Host   string `json:"host"`   // host地址
	Owner  string `json:"owner"`  // 分组名
	Repo   string `json:"repo"`   // 项目名
	Path   string `json:"path"`   // 文件路径
	Branch string `json:"branch"` // 分支名称
}

// 缓存已经下载过的PB链接
var descriptorCache = map[string]*desc.FileDescriptor{}

// loadPbDescriptor 根据pb文件地址，动态加载Pb文件描述符（目前仅支持内网github）
func loadPbDescriptor(ctx context.Context, pbFileURL string, token string) (*desc.FileDescriptor, error) {
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
	// 解析内容
	parser := &protoparse.Parser{
		Accessor: buildAccessor(ctx, pbFileURL, content),
	}
	fds, err := parser.ParseFiles(pbFileURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse protobuf file: %w", err)
	}
	if len(fds) == 0 {
		return nil, fmt.Errorf("no file descriptors found")
	}
	// 写入缓存
	descriptorCache[pbFileURL] = fds[0]
	return fds[0], nil
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

// findMessageDescriptor 根据消息名称查找消息描述符
func findMessageDescriptor(fd *desc.FileDescriptor, messageName string) (*desc.MessageDescriptor, error) {
	// 首先尝试直接查找
	md := fd.FindMessage(messageName)
	if md != nil {
		return md, nil
	}
	// 如果没有找到，尝试在所有消息类型中查找
	messages := fd.GetMessageTypes()
	for _, msg := range messages {
		if msg.GetName() == messageName {
			return msg, nil
		}
		// 递归查找嵌套消息
		if nestedMsg := findNestedMessage(msg, messageName); nestedMsg != nil {
			return nestedMsg, nil
		}
	}
	return nil, fmt.Errorf("message %s not found in protobuf file", messageName)
}

// findNestedMessage 寻找嵌套消息内容
func findNestedMessage(parent *desc.MessageDescriptor, messageName string) *desc.MessageDescriptor {
	for _, nested := range parent.GetNestedMessageTypes() {
		if nested.GetName() == messageName {
			return nested
		}
		if result := findNestedMessage(nested, messageName); result != nil {
			return result
		}
	}
	return nil
}

// 中间import数据暂存
var buildAccessorCache combinMap.MySyncMap[string, []byte]

// buildAccessor 数据访问器
func buildAccessor(ctx context.Context, mainName string, mainContent string) protoparse.FileAccessor {
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

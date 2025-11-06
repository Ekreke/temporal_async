package node

import (
	"context"
	"fmt"
	taskv2 "github.acme.red/backendhub/idl/gen/go/mapper/task/v2"
	workflowv2 "github.acme.red/backendhub/idl/gen/go/mapper/workflow/v2"
	taskargsv1 "github.acme.red/mapper/idl/gen/go/mapper/taskargs/v1"
	"github.com/bytedance/sonic"
	"google.golang.org/protobuf/types/known/anypb"
	"net"
	"strings"
	"time"
)

var _ Activity = (*DomainResolveActivity)(nil)

// DomainResolveActivity 域名解析 Activity
type DomainResolveActivity struct {
	grpcCli taskv2.TaskManagerServiceClient
	*BaseActivity
}

// NewDomainResolveActivity 创建新的域名解析节点
func NewDomainResolveActivity(grpcClient taskv2.TaskManagerServiceClient) *DomainResolveActivity {
	activity := &DomainResolveActivity{
		grpcCli: grpcClient,
	}
	return activity
}

// RegisterDomainResolve 注册域名解析作为activity节点
func (a *DomainResolveActivity) RegisterDomainResolve(ctx context.Context, input *ActivityInput,
	express *ExpressionEvaluator, node WkFLowNode) (*ActivityOutput, error) {
	if a.grpcCli == nil {
		return nil, fmt.Errorf("RegisterDomainResolve Error, grpc client not initialized")
	}
	a.BaseActivity = &BaseActivity{
		NodeInfo:            &node,
		expressionEvaluator: express,
	}
	return a.Execute(ctx, input)
}

// GetNodeInfo 获取当前节点信息
func (a *DomainResolveActivity) GetNodeInfo() *WkFLowNode {
	return a.NodeInfo
}

// Execute 执行节点逻辑（实现NodeActivity接口）
func (a *DomainResolveActivity) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	return a.ExecuteWithExecuteTiming(ctx, input, a.executeDomainResolve)
}

// ValidateInput 验证输入参数（实现NodeActivity接口）
func (a *DomainResolveActivity) ValidateInput(input *ActivityInput) error {
	// 基础验证
	if err := a.BaseActivity.ValidateInput(input); err != nil {
		return err
	}
	// 检查域名参数
	domain := a.extractDomain(input)
	if domain == "" {
		return fmt.Errorf("缺少域名参数，请提供domain、url或host参数")
	}
	return nil
}

// executeDomainResolve 内部域名解析逻辑
func (a *DomainResolveActivity) executeDomainResolve(input *ActivityInput) (map[string]interface{}, error) {
	// 验证参数
	if err := a.ValidateInput(input); err != nil {
		return nil, err
	}
	// 元数据标签
	metaData, _ := sonic.Marshal(input.SignalInput.Metadata)
	var label = map[string]string{}
	_ = sonic.Unmarshal(metaData, &label)
	// 解析parameters中的各个变量值，进行正确赋值
	nodeParam := make(map[string]interface{})
	for k, v := range input.Parameters {
		val, ok := v.(string)
		if !ok {
			nodeParam[k] = v
			continue
		}
		evalData, err := a.GetExpressionEvaluator().EvaluateExpression(val, input.SignalInput.Data)
		if err != nil {
			return nil, err
		}
		nodeParam[k] = evalData
	}
	// 将入参转换成anyPb
	nodeData, err := sonic.Marshal(nodeParam)
	if err != nil {
		return nil, err
	}
	data := &taskargsv1.DomainResolveTaskData{}
	err = sonic.Unmarshal(nodeData, &data)
	if err != nil {
		return nil, err
	}
	args, err := anypb.New(data)
	if err != nil {
		return nil, err
	}
	// 请求参数构建
	task := &taskv2.Task{
		UniqueId:       input.UniqueId,                                        // 单个任务的唯一ID
		Kind:           workflowv2.TaskKind_TASK_KIND_DOMAIN_RESOLVE.String(), // 任务类型，对应POC、爬虫等枚举
		ParentUniqueId: input.SignalInput.NodeName,                            // 父ID，上一个节点的ID（信号节点接收到的就是上一个节点的执行结果）
		GroupId:        input.ExecutionID,                                     // 组ID用于区分流水线,当前正在运行的流水线RunID
		Args:           args,                                                  // 参数，节点执行的参数
		Labels:         label,                                                 // 标签，透传
		WorkerSelector: map[string]string{},                                   // 工作节点选择，暂时为空
	}
	tasks := []*taskv2.Task{task}
	// 执行调度查询
	scRes, err := a.SendSchedule(context.Background(), tasks)
	if err != nil {
		return nil, err
	}
	// 需要返回新增的节点数（ len(scRes) ） 和 已完成的节点数（ 1 ）
	res := map[string]interface{}{
		"task_ids":        scRes,
		"add_task_num":    len(scRes),
		"finish_task_num": 1,
	}
	return res, nil
}

// extractDomain 从输入中提取域名
func (a *DomainResolveActivity) extractDomain(input *ActivityInput) string {
	// 优先从参数中获取
	domain := a.GetStringParameter(input.Parameters, "domain")
	if domain != "" {
		return domain
	}
	// 从参数中获取url
	if url := a.GetStringParameter(input.Parameters, "url"); url != "" {
		// 从URL中提取域名
		parsed := strings.TrimPrefix(url, "http://")
		parsed = strings.TrimPrefix(parsed, "https://")
		if slashIndex := strings.Index(parsed, "/"); slashIndex != -1 {
			domain = parsed[:slashIndex]
		} else {
			domain = parsed
		}
		return domain
	}
	// 从参数中获取host
	if host := a.GetStringParameter(input.Parameters, "host"); host != "" {
		return host
	}
	// 从输入数据中获取
	if input.InputData != nil {
		// 尝试多种方式获取域名
		if domainValue, exists := input.InputData["domain"]; exists {
			if d, ok := domainValue.(string); ok {
				domain = d
			}
		} else if urlValue, exists := input.InputData["url"]; exists {
			if url, ok := urlValue.(string); ok {
				// 从URL中提取域名
				parsed := strings.TrimPrefix(url, "http://")
				parsed = strings.TrimPrefix(parsed, "https://")
				if slashIndex := strings.Index(parsed, "/"); slashIndex != -1 {
					domain = parsed[:slashIndex]
				} else {
					domain = parsed
				}
			}
		} else if hostValue, exists := input.InputData["host"]; exists {
			if host, ok := hostValue.(string); ok {
				domain = host
			}
		}
	}
	return domain
}

// resolveDomain 执行实际域名解析
func (a *DomainResolveActivity) resolveDomain(domain string) (map[string]interface{}, error) {
	result := map[string]interface{}{
		"domain":    domain,
		"resolved":  true,
		"timestamp": time.Now(),
	}
	// 解析A记录（IPv4地址）
	ips, err := net.LookupIP(domain)
	if err != nil {
		return nil, fmt.Errorf("DNS查询失败: %v", err)
	}
	var ipv4Addresses []string
	var ipv6Addresses []string
	for _, ip := range ips {
		if ip.To4() != nil {
			ipv4Addresses = append(ipv4Addresses, ip.String())
		} else {
			ipv6Addresses = append(ipv6Addresses, ip.String())
		}
	}
	if len(ipv4Addresses) > 0 {
		result["ip"] = ipv4Addresses[0] // 返回第一个IPv4地址
		result["ipv4"] = ipv4Addresses
	}
	if len(ipv6Addresses) > 0 {
		result["ipv6"] = ipv6Addresses
	}
	// 解析MX记录（邮件服务器）
	mxRecords, err := net.LookupMX(domain)
	if err == nil && len(mxRecords) > 0 {
		var mxServers []string
		for _, mx := range mxRecords {
			mxServers = append(mxServers, mx.Host)
		}
		result["mx"] = mxServers
	}

	// 解析NS记录（名称服务器）
	nsRecords, err := net.LookupNS(domain)
	if err == nil && len(nsRecords) > 0 {
		var nsServers []string
		for _, ns := range nsRecords {
			nsServers = append(nsServers, ns.Host)
		}
		result["ns"] = nsServers
	}

	// 解析TXT记录
	txtRecords, err := net.LookupTXT(domain)
	if err == nil && len(txtRecords) > 0 {
		result["txt"] = txtRecords
	}

	// 解析CNAME记录
	cname, err := net.LookupCNAME(domain)
	if err == nil && cname != "" {
		result["cname"] = cname
	}

	// 添加反向DNS查询（如果提供了IP）
	if len(ipv4Addresses) > 0 {
		reverseDNS, err := net.LookupAddr(ipv4Addresses[0])
		if err == nil && len(reverseDNS) > 0 {
			result["reverseDNS"] = reverseDNS[0]
		}
	}

	return result, nil
}

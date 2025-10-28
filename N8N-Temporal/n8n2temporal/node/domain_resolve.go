package node

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"
)

// DomainResolveActivity 域名解析 Activity
type DomainResolveActivity struct {
	*BaseActivity
}

// NewDomainResolveActivity 创建新的域名解析节点
func NewDomainResolveActivity(express *ExpressionEvaluator) *DomainResolveActivity {
	activity := &DomainResolveActivity{
		BaseActivity: &BaseActivity{
			NodeInfo: &ActivityInfo{
				ID:          "domain_resolve",
				Name:        "Domain Resolve",
				Type:        "DNS.domainResolve",
				Description: "Resolve DNS records for domain names",
				Version:     "1.0.0",
				Category:    "network",
				Icon:        "🌐",
			},
			expressionEvaluator: express,
		},
	}
	return activity
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
	// 从输入数据中提取域名
	domain := a.extractDomain(input)
	if domain == "" {
		// 如果还是没有域名，使用默认值
		domain = "example.com"
	}

	// 执行DNS查询
	result, err := a.resolveDomain(domain)
	if err != nil {
		return nil, fmt.Errorf("域名解析失败: %v", err)
	}

	resultData := map[string]interface{}{
		"domain":      domain,
		"dns_records": result,
		"resolved":    true,
	}

	return resultData, nil
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

// ExecuteDomainResolve 执行域名解析（向后兼容）
func (a *DomainResolveActivity) ExecuteDomainResolve(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
	// 转换旧格式到新格式
	nodeInput := &ActivityInput{
		NodeID:      getStringValue(input, "nodeId"),
		NodeName:    getStringValue(input, "nodeName"),
		NodeType:    "DNS.domainResolve",
		InputData:   getInputData(input),
		Parameters:  getParameters(input),
		WorkflowID:  getStringValue(input, "workflowId"),
		ExecutionID: getStringValue(input, "executionId"),
	}

	// 执行新接口
	output, err := a.Execute(ctx, nodeInput)
	if err != nil {
		return nil, err
	}

	// 转换输出格式
	if output.Success {
		return map[string]interface{}{
			"success": true,
			"data":    output.Data,
		}, nil
	} else {
		return map[string]interface{}{
			"success": false,
			"error":   output.Error,
		}, nil
	}
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

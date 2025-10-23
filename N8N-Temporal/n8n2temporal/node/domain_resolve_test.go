package node

import (
	"testing"
)

// TestDomainResolveActivity 测试域名解析节点
func TestDomainResolveActivity(t *testing.T) {
	activity := NewDomainResolveActivity()

	tests := []struct {
		name         string
		input        *ActivityInput
		expectError  bool
		expectDomain string
	}{
		{
			name: "解析域名",
			input: &ActivityInput{
				NodeType: "DNS.domainResolve",
				Parameters: map[string]interface{}{
					"domain": "google.com",
				},
				InputData: map[string]interface{}{},
			},
			expectError:  false,
			expectDomain: "google.com",
		},
		{
			name: "从URL中解析域名",
			input: &ActivityInput{
				NodeType: "DNS.domainResolve",
				Parameters: map[string]interface{}{
					"url": "https://www.example.com/path",
				},
				InputData: map[string]interface{}{},
			},
			expectError:  false,
			expectDomain: "www.example.com",
		},
		{
			name: "从输入数据中解析域名",
			input: &ActivityInput{
				NodeType:   "DNS.domainResolve",
				Parameters: map[string]interface{}{},
				InputData: map[string]interface{}{
					"host": "github.com",
				},
			},
			expectError:  false,
			expectDomain: "github.com",
		},
		{
			name: "缺少域名参数",
			input: &ActivityInput{
				NodeType:   "DNS.domainResolve",
				Parameters: map[string]interface{}{},
				InputData:  map[string]interface{}{},
			},
			expectError:  false, // 使用默认域名
			expectDomain: "example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 验证输入
			err := activity.ValidateInput(tt.input)
			if err != nil {
				t.Fatalf("输入验证失败: %v", err)
			}

			// 执行逻辑
			result, err := activity.executeDomainResolve(tt.input)
			if tt.expectError {
				if err == nil {
					t.Errorf("期望执行失败，但执行成功了")
				}
				return
			}

			if err != nil {
				t.Fatalf("执行失败: %v", err)
			}

			// 验证输出
			if domain, exists := result["domain"]; exists {
				if domain != tt.expectDomain {
					t.Errorf("期望域名为 '%s', 实际为 '%v'", tt.expectDomain, domain)
				}
			} else {
				t.Errorf("期望输出中包含 domain 字段")
			}

			// 验证解析标记
			if resolved, exists := result["resolved"]; exists {
				if !resolved.(bool) {
					t.Errorf("期望解析标记为 true")
				}
			} else {
				t.Errorf("期望输出中包含 resolved 字段")
			}
		})
	}
}

// TestDomainResolveActivityNodeInfo 测试节点信息
func TestDomainResolveActivityNodeInfo(t *testing.T) {
	activity := NewDomainResolveActivity()
	info := activity.GetNodeInfo()

	if info.ID != "domain_resolve" {
		t.Errorf("期望节点ID为 'domain_resolve', 实际为 '%s'", info.ID)
	}

	if info.Name != "Domain Resolve" {
		t.Errorf("期望节点名称为 'Domain Resolve', 实际为 '%s'", info.Name)
	}

	if info.Type != "DNS.domainResolve" {
		t.Errorf("期望节点类型为 'DNS.domainResolve', 实际为 '%s'", info.Type)
	}

	if info.Category != "network" {
		t.Errorf("期望节点分类为 'network', 实际为 '%s'", info.Category)
	}

	// 参数元数据已移除，不再需要测试这些字段
}

// TestExtractDomain 测试域名提取逻辑
func TestExtractDomain(t *testing.T) {
	activity := NewDomainResolveActivity()

	tests := []struct {
		name         string
		input        *ActivityInput
		expectDomain string
	}{
		{
			name: "从参数获取域名",
			input: &ActivityInput{
				Parameters: map[string]interface{}{
					"domain": "test.com",
				},
			},
			expectDomain: "test.com",
		},
		{
			name: "从输入数据获取域名",
			input: &ActivityInput{
				Parameters: map[string]interface{}{},
				InputData: map[string]interface{}{
					"domain": "from-data.com",
				},
			},
			expectDomain: "from-data.com",
		},
		{
			name: "从URL提取域名",
			input: &ActivityInput{
				Parameters: map[string]interface{}{},
				InputData: map[string]interface{}{
					"url": "https://api.example.com/v1",
				},
			},
			expectDomain: "api.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			domain := activity.extractDomain(tt.input)
			if domain != tt.expectDomain {
				t.Errorf("期望域名为 '%s', 实际为 '%s'", tt.expectDomain, domain)
			}
		})
	}
}

# Activity 节点详细文档

本文档详细介绍了 N8N to Temporal 工作流转换引擎中所有可用的 Activity 节点类型、参数配置、使用方法和最佳实践。

## 📋 目录

- [节点架构概述](#节点架构概述)
- [基础节点类型](#基础节点类型)
- [逻辑控制节点](#逻辑控制节点)
- [数据处理节点](#数据处理节点)
- [网络服务节点](#网络服务节点)
- [自定义节点扩展](#自定义节点扩展)
- [节点开发指南](#节点开发指南)

## 🏗️ 节点架构概述

### 统一接口设计

所有节点都实现统一的 `NodeActivity` 接口，确保一致的行为和可扩展性：

```go
type NodeActivity interface {
    GetNodeInfo() *NodeInfo           // 获取节点元信息
    Execute(ctx context.Context, input *NodeInput) (*NodeOutput, error)  // 执行节点逻辑
    ValidateInput(input *NodeInput) error  // 验证输入参数
    GetLogger(ctx context.Context) log.Logger  // 获取日志记录器
}
```

### 核心数据结构

```go
// NodeInfo 节点元信息
type NodeInfo struct {
    ID          string        `json:"id"`           // 节点唯一标识
    Name        string        `json:"name"`         // 节点显示名称
    Type        string        `json:"type"`         // 节点类型标识
    Description string        `json:"description"`  // 节点描述
    Version     string        `json:"version"`      // 节点版本
    Category    string        `json:"category"`     // 节点分类
    Icon        string        `json:"icon"`         // 节点图标
    Inputs      []NodePort    `json:"inputs"`       // 输入端口定义
    Outputs     []NodePort    `json:"outputs"`      // 输出端口定义
    Parameters  []NodeParameter `json:"parameters"` // 参数定义
}

// NodeInput 节点输入数据
type NodeInput struct {
    NodeID      string                 `json:"nodeId"`      // 节点ID
    NodeName    string                 `json:"nodeName"`    // 节点名称
    NodeType    string                 `json:"nodeType"`    // 节点类型
    InputData   map[string]interface{} `json:"inputData"`   // 输入数据
    Parameters  map[string]interface{} `json:"parameters"`  // 节点参数
    WorkflowID  string                 `json:"workflowId"`  // 工作流ID
    ExecutionID string                 `json:"executionId"` // 执行实例ID
}

// NodeOutput 节点输出数据
type NodeOutput struct {
    NodeID      string                 `json:"nodeId"`      // 节点ID
    NodeName    string                 `json:"nodeName"`    // 节点名称
    NodeType    string                 `json:"nodeType"`    // 节点类型
    Success     bool                   `json:"success"`     // 执行是否成功
    Data        map[string]interface{} `json:"data"`        // 输出数据
    Error       string                 `json:"error"`       // 错误信息
    ProcessedAt time.Time              `json:"processedAt"` // 处理时间
    Metadata    map[string]interface{} `json:"metadata"`    // 元数据
}
```

## 🔧 基础节点类型

### 手动触发节点

**节点类型**: `n8n-nodes-base.manualTrigger`

**功能描述**: 手动触发工作流执行的起始节点，通常作为工作流的入口点。

#### 配置参数

该节点不需要任何参数，直接传递输入数据作为工作流的起始数据。

#### 输入数据

```json
{
  "triggerData": {
    "message": "Hello World",
    "timestamp": 1634567890
  }
}
```

#### 输出数据

```json
{
  "success": true,
  "data": {
    "triggerData": {
      "message": "Hello World",
      "timestamp": 1634567890
    }
  },
  "processedAt": "2023-10-18T10:30:00Z",
  "metadata": {
    "executionTime": 1634567890,
    "nodeVersion": "1.0.0"
  }
}
```

#### 使用示例

```json
{
  "id": "manual-trigger-1",
  "name": "手动触发",
  "type": "n8n-nodes-base.manualTrigger",
  "typeVersion": 1,
  "position": [240, 300],
  "parameters": {}
}
```

#### 工作流集成

```go
// 在 workflow.go 中的处理逻辑
case "n8n-nodes-base.manualTrigger":
    // 手动触发节点，直接返回输入数据
    result.Success = true
    result.Data = inputData
```

---

## 🔀 逻辑控制节点

### IF 条件判断节点

**节点类型**: `n8n-nodes-base.if`

**功能描述**: 根据条件判断结果选择执行分支，支持 AND/OR 逻辑组合器。

#### 配置参数

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| conditions | object | ✅ | - | 条件配置对象 |

##### conditions 对象结构

```json
{
  "options": {
    "combinator": "and|or",  // 条件组合器
    "conditions": [
      {
        "id": "cond-1",
        "leftValue": "{{field}}",  // 左值，支持字段引用
        "rightValue": "value",     // 右值
        "operator": "equals",      // 操作符
        "typeValidation": "loose", // 类型验证方式
        "version": 1
      }
    ]
  }
}
```

##### 支持的操作符

| 操作符 | 描述 | 示例 |
|--------|------|------|
| `equals`, `==` | 等于 | `"{{status}}" equals "active"` |
| `not_equals`, `!=` | 不等于 | `"{{status}}" not_equals "inactive"` |
| `contains` | 包含 | `"{{text}}" contains "keyword"` |
| `not_contains` | 不包含 | `"{{text}}" not_contains "spam"` |
| `starts_with` | 以...开始 | `"{{url}}" starts_with "https"` |
| `ends_with` | 以...结束 | `"{{file}}" ends_with ".pdf"` |
| `greater_than`, `>` | 大于 | `"{{count}}" greater_than 10` |
| `greater_than_or_equal`, `>=` | 大于等于 | `"{{score}}" >= 80` |
| `less_than`, `<` | 小于 | `"{{age}}" less_than 18` |
| `less_than_or_equal`, `<=` | 小于等于 | `"{{price}}" <= 100` |
| `is_empty` | 为空 | `"{{field}}" is_empty` |
| `is_not_empty` | 不为空 | `"{{field}}" is_not_empty` |

##### 条件组合器

- **and**: 所有条件都必须为真
- **or**: 任意一个条件为真即可

#### 输入数据示例

```json
{
  "nodeId": "if-node-1",
  "nodeName": "条件判断",
  "nodeType": "n8n-nodes-base.if",
  "parameters": {
    "conditions": {
      "options": {
        "combinator": "and",
        "conditions": [
          {
            "id": "cond-1",
            "leftValue": "{{status}}",
            "rightValue": "active",
            "operator": "equals",
            "typeValidation": "loose",
            "version": 1
          },
          {
            "id": "cond-2",
            "leftValue": "{{score}}",
            "rightValue": 80,
            "operator": "greater_than_or_equal",
            "typeValidation": "loose",
            "version": 1
          }
        ]
      }
    }
  },
  "inputData": {
    "status": "active",
    "score": 85,
    "user": "alice"
  }
}
```

#### 输出数据格式

```json
{
  "success": true,
  "data": {
    "matchedIndex": 0,  // 匹配的规则索引：0=main分支，1=alternative分支
    "output": {
      "conditionMet": true,
      "conditions": [...],
      "combinator": "and",
      "inputData": {...},
      "evaluationTime": 1634567890
    },
    "conditions": [...],
    "combinator": "and",
    "conditionMet": true,
    "inputData": {...}
  },
  "processedAt": "2023-10-18T10:30:00Z"
}
```

#### 分支逻辑

- **matchedIndex = 0**: 条件为真，执行 main 分支
- **matchedIndex = 1**: 条件为假，执行 alternative 分支

#### 使用示例

```json
{
  "id": "if-node-1",
  "name": "用户状态检查",
  "type": "n8n-nodes-base.if",
  "typeVersion": 1,
  "position": [460, 300],
  "parameters": {
    "conditions": {
      "options": {
        "combinator": "and",
        "conditions": [
          {
            "id": "cond-1",
            "leftValue": "{{user.status}}",
            "rightValue": "active",
            "operator": "equals",
            "typeValidation": "loose",
            "version": 1
          }
        ]
      }
    }
  }
}
```

---

### Switch 分支选择节点

**节点类型**: `n8n-nodes-base.switch`

**功能描述**: 基于多个规则条件选择执行分支，支持复杂的条件组合和分支映射。

#### 配置参数

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| rules | object | ✅ | - | 规则配置对象 |

##### rules 对象结构

```json
{
  "values": [
    {
      "id": "rule-1",                    // 规则ID
      "outputValue": "branch-name",      // 分支名称
      "conditions": [                    // 规则条件列表
        {
          "id": "cond-1",
          "leftValue": "{{field}}",
          "rightValue": "value",
          "operator": "equals"
        }
      ]
    }
  ]
}
```

#### 分支选择逻辑

1. **顺序评估**: 按照规则在数组中的顺序依次评估
2. **AND 逻辑**: 每个规则内的所有条件必须同时满足（AND 逻辑）
3. **首次匹配**: 返回第一个匹配的规则索引
4. **默认分支**: 如果没有规则匹配，返回 -1（默认分支）

#### 输入数据示例

```json
{
  "nodeId": "switch-node-1",
  "nodeName": "路由选择",
  "nodeType": "n8n-nodes-base.switch",
  "parameters": {
    "rules": {
      "values": [
        {
          "id": "rule-vip",
          "outputValue": "vip-branch",
          "conditions": [
            {
              "id": "cond-vip",
              "leftValue": "{{user.level}}",
              "rightValue": "vip",
              "operator": "equals"
            }
          ]
        },
        {
          "id": "rule-premium",
          "outputValue": "premium-branch",
          "conditions": [
            {
              "id": "cond-premium",
              "leftValue": "{{user.level}}",
              "rightValue": "premium",
              "operator": "equals"
            }
          ]
        }
      ]
    }
  },
  "inputData": {
    "user": {
      "id": "12345",
      "level": "vip",
      "score": 95
    }
  }
}
```

#### 输出数据格式

```json
{
  "success": true,
  "data": {
    "matchedIndex": 0,  // 匹配的规则索引（从0开始）
    "selectedBranch": "vip-branch",  // 选中的分支名称
    "matchedRule": "rule-vip",       // 匹配的规则ID
    "rules": [...],                  // 所有规则定义
    "inputData": {...}               // 输入数据
  },
  "processedAt": "2023-10-18T10:30:00Z"
}
```

#### 分支映射

在 N8N 工作流中，`matchedIndex` 对应连接关系中的分支索引：

```json
{
  "connections": {
    "路由选择": {
      "main": [
        [
          {"node": "VIP处理", "type": "main", "index": 0},
          {"node": "Premium处理", "type": "main", "index": 1},
          {"node": "默认处理", "type": "main", "index": 2}
        ]
      ]
    }
  }
}
```

- **matchedIndex = 0**: 执行 "VIP处理" 节点
- **matchedIndex = 1**: 执行 "Premium处理" 节点
- **matchedIndex = 2**: 执行 "默认处理" 节点

#### 使用示例

```json
{
  "id": "switch-1",
  "name": "优先级路由",
  "type": "n8n-nodes-base.switch",
  "typeVersion": 1,
  "position": [680, 300],
  "parameters": {
    "rules": {
      "values": [
        {
          "id": "high-priority",
          "outputValue": "high",
          "conditions": [
            {
              "id": "cond-high",
              "leftValue": "{{priority}}",
              "rightValue": "high",
              "operator": "equals"
            }
          ]
        },
        {
          "id": "medium-priority",
          "outputValue": "medium",
          "conditions": [
            {
              "id": "cond-medium",
              "leftValue": "{{priority}}",
              "rightValue": "medium",
              "operator": "equals"
            }
          ]
        }
      ]
    }
  }
}
```

---

## 🐍 数据处理节点

### Python 代码执行节点

**节点类型**: `n8n-nodes-base.code`

**功能描述**: 执行 Python 代码进行数据处理和计算。

#### 配置参数

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| pythonCode | string | ✅ | - | 要执行的 Python 代码 |

#### Python 代码环境

- **Python 版本**: 系统默认 Python 版本
- **输入数据**: 通过 `input_data` 变量访问
- **输出数据**: 通过 `print()` 或标准输出返回
- **错误处理**: 异常会作为节点错误返回

#### 输入数据示例

```json
{
  "nodeId": "python-1",
  "nodeName": "数据处理",
  "nodeType": "n8n-nodes-base.code",
  "parameters": {
    "pythonCode": "import json\n\n# 获取输入数据\ndata = input_data\nnumbers = data.get('numbers', [])\n\n# 计算平均值\nif numbers:\n    average = sum(numbers) / len(numbers)\n    result = {\n        'count': len(numbers),\n        'sum': sum(numbers),\n        'average': average,\n        'max': max(numbers),\n        'min': min(numbers)\n    }\nelse:\n    result = {'error': 'No numbers provided'}\n\n# 输出结果\nprint(json.dumps(result))"
  },
  "inputData": {
    "numbers": [10, 20, 30, 40, 50]
  }
}
```

#### 输出数据格式

```json
{
  "success": true,
  "data": {
    "result": {
      "count": 5,
      "sum": 150,
      "average": 30.0,
      "max": 50,
      "min": 10
    },
    "execution_time": "2023-10-18T10:30:00Z"
  },
  "processedAt": "2023-10-18T10:30:00Z"
}
```

#### Python 代码最佳实践

1. **数据访问**: 使用 `input_data` 变量访问输入数据
2. **结果输出**: 使用 `print(json.dumps(result))` 输出 JSON 格式结果
3. **错误处理**: 使用 try-except 捕获异常
4. **导入模块**: 只使用 Python 标准库和系统已安装的包

#### 代码模板

```python
import json
import sys

def main():
    try:
        # 获取输入数据
        input_data = locals().get('input_data', {})

        # 在这里编写你的处理逻辑
        result = process_data(input_data)

        # 输出结果
        print(json.dumps(result, ensure_ascii=False, indent=2))

    except Exception as e:
        error_result = {
            "error": str(e),
            "error_type": type(e).__name__
        }
        print(json.dumps(error_result, ensure_ascii=False))

def process_data(data):
    """数据处理函数"""
    # 示例：转换数据格式
    if isinstance(data, dict):
        return {
            "processed": True,
            "original_keys": list(data.keys()),
            "timestamp": "2023-10-18T10:30:00Z"
        }
    return data

if __name__ == "__main__":
    main()
```

#### 使用示例

```json
{
  "id": "python-1",
  "name": "数据分析",
  "type": "n8n-nodes-base.code",
  "typeVersion": 1,
  "position": [900, 300],
  "parameters": {
    "pythonCode": "import json\n\n# 分析用户行为数据\ndef analyze_user_data(data):\n    users = data.get('users', [])\n    \n    total_users = len(users)\n    active_users = len([u for u in users if u.get('status') == 'active'])\n    \n    result = {\n        'total_users': total_users,\n        'active_users': active_users,\n        'active_rate': active_users / total_users if total_users > 0 else 0\n    }\n    \n    return result\n\nresult = analyze_user_data(input_data)\nprint(json.dumps(result, ensure_ascii=False))"
  }
}
```

---

## 🌐 网络服务节点

### 域名解析节点

**节点类型**: `DNS.domainResolve`

**功能描述**: 解析域名的 DNS 记录，包括 A、AAAA、MX、NS、TXT、CNAME 等记录类型。

#### 配置参数

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| domain | string | ✅ | - | 要解析的域名 |
| url | string | ❌ | - | URL（自动提取域名） |
| host | string | ❌ | - | 主机名 |
| recordTypes | array | ❌ | ["A", "AAAA", "MX", "NS", "TXT", "CNAME"] | 要解析的记录类型 |

#### 参数优先级

1. **domain**: 直接指定域名
2. **url**: 从 URL 中提取域名
3. **host**: 使用主机名作为域名
4. **inputData**: 从输入数据中查找 domain/url/host 字段

#### 输入数据示例

```json
{
  "nodeId": "dns-resolve-1",
  "nodeName": "域名解析",
  "nodeType": "DNS.domainResolve",
  "parameters": {
    "domain": "example.com",
    "recordTypes": ["A", "AAAA", "MX", "NS", "TXT", "CNAME"]
  },
  "inputData": {
    "url": "https://api.example.com/v1/data"
  }
}
```

#### 输出数据格式

```json
{
  "success": true,
  "data": {
    "domain": "example.com",
    "resolved": true,
    "timestamp": "2023-10-18T10:30:00Z",
    "ip": "93.184.216.34",
    "ipv4": ["93.184.216.34"],
    "ipv6": ["2606:2800:220:1:248:1893:25c8:1946"],
    "mx": ["mail.example.com"],
    "ns": ["ns1.example.com", "ns2.example.com"],
    "txt": ["v=spf1 include:_spf.example.com ~all"],
    "cname": "www.example.com",
    "reverseDNS": "example.com"
  },
  "processedAt": "2023-10-18T10:30:00Z"
}
```

#### DNS 记录类型说明

| 记录类型 | 描述 | 示例 |
|---------|------|------|
| **A** | IPv4 地址记录 | `93.184.216.34` |
| **AAAA** | IPv6 地址记录 | `2606:2800:220:1:248:1893:25c8:1946` |
| **MX** | 邮件交换记录 | `mail.example.com` |
| **NS** | 名称服务器记录 | `ns1.example.com` |
| **TXT** | 文本记录 | `v=spf1 include:_spf.example.com ~all` |
| **CNAME** | 规范名称记录 | `www.example.com` |

#### 使用场景

1. **域名验证**: 检查域名是否有效
2. **网络安全**: 分析域名的 DNS 配置
3. **邮件配置**: 验证 MX 记录配置
4. **CDN 检测**: 检查域名的 CNAME 记录
5. **IPv6 支持**: 检查域名的 AAAA 记录

#### 使用示例

```json
{
  "id": "dns-resolve-1",
  "name": "域名分析",
  "type": "DNS.domainResolve",
  "typeVersion": 1,
  "position": [1120, 300],
  "parameters": {
    "domain": "{{website.url}}",
    "recordTypes": ["A", "AAAA", "MX", "NS", "TXT", "CNAME"]
  }
}
```

#### 错误处理

- **域名不存在**: 返回错误信息
- **网络超时**: 返回超时错误
- **记录不存在**: 相应字段为空数组
- **权限问题**: 返回权限错误

---

## 🛠️ 自定义节点扩展

### 自定义节点基础

**节点类型**: `CUSTOM.*`

**功能描述**: 支持用户自定义业务逻辑的通用节点类型。

#### 内置自定义节点

##### ASM 域名解析节点

**节点类型**: `CUSTOM.asmDomainResolve`

基于标准域名解析节点的增强版本，提供更多解析选项。

```json
{
  "id": "custom-dns-1",
  "name": "ASM域名解析",
  "type": "CUSTOM.asmDomainResolve",
  "typeVersion": 1,
  "position": [1340, 300],
  "parameters": {
    "domain": "{{target_domain}}",
    "recordTypes": ["A", "AAAA", "MX", "NS", "TXT", "CNAME"],
    "timeout": 30,
    "retries": 3
  }
}
```

##### ASM Python 执行节点

**节点类型**: `CUSTOM.asmPythonCode`

增强的 Python 代码执行节点，支持更多配置选项。

```json
{
  "id": "custom-python-1",
  "name": "ASM Python执行",
  "type": "CUSTOM.asmPythonCode",
  "typeVersion": 1,
  "position": [1560, 300],
  "parameters": {
    "pythonCode": "import sys\nprint('Hello from ASM Python')",
    "timeout": 60,
    "workingDirectory": "/tmp",
    "environment": {
      "ENV_VAR": "value"
    }
  }
}
```

##### 通用自定义节点

**节点类型**: `CUSTOM.myCustomNode`

用户可以定义任意业务逻辑的自定义节点。

```json
{
  "id": "custom-1",
  "name": "我的自定义节点",
  "type": "CUSTOM.myCustomNode",
  "typeVersion": 1,
  "position": [1780, 300],
  "parameters": {
    "message": "Hello World",
    "action": "process",
    "options": {
      "strict": true,
      "timeout": 30
    }
  }
}
```

---

## 👨‍💻 节点开发指南

### 创建新的自定义节点

#### 1. 定义节点结构

```go
package activity

import (
    "context"
    "time"
)

// MyCustomNode 自定义节点结构
type MyCustomNode struct {
    BaseActivity
}

// NewMyCustomNode 创建节点实例
func NewMyCustomNode() *MyCustomNode {
    return &MyCustomNode{
        BaseActivity: BaseActivity{
            NodeInfo: &NodeInfo{
                ID:          "my_custom_node",
                Name:        "我的自定义节点",
                Type:        "CUSTOM.myCustomNode",
                Description: "执行自定义业务逻辑",
                Version:     "1.0.0",
                Category:    "custom",
                Icon:        "🛠️",
                Inputs: []NodePort{
                    {
                        ID:          "data",
                        Name:        "输入数据",
                        Type:        "object",
                        Required:    false,
                        Description: "节点要处理的数据",
                    },
                },
                Outputs: []NodePort{
                    {
                        ID:          "result",
                        Name:        "处理结果",
                        Type:        "object",
                        Required:    false,
                        Description: "节点的处理结果",
                    },
                },
                Parameters: []NodeParameter{
                    {
                        ID:          "action",
                        Name:        "执行动作",
                        Type:        "string",
                        Required:    true,
                        Description: "要执行的动作类型",
                        Default:     "process",
                    },
                    {
                        ID:          "options",
                        Name:        "配置选项",
                        Type:        "object",
                        Required:    false,
                        Description: "节点的配置选项",
                        Default:     map[string]interface{}{},
                    },
                },
            },
        },
    }
}
```

#### 2. 实现接口方法

```go
// Execute 执行节点逻辑
func (a *MyCustomNode) Execute(ctx context.Context, input *NodeInput) (*NodeOutput, error) {
    return a.ExecuteWithExecuteTiming(ctx, input, a.executeCustomNode)
}

// executeCustomNode 内部执行逻辑
func (a *MyCustomNode) executeCustomNode(input *NodeInput) (map[string]interface{}, error) {
    // 获取参数
    action := a.GetStringParameter(input.Parameters, "action")
    options := a.GetMapParameter(input.Parameters, "options")

    // 执行业务逻辑
    result, err := a.processAction(action, input.InputData, options)
    if err != nil {
        return nil, err
    }

    // 返回结果
    return map[string]interface{}{
        "success": true,
        "action":  action,
        "result":  result,
        "metadata": map[string]interface{}{
            "nodeId":     input.NodeID,
            "executedAt": time.Now().Unix(),
        },
    }, nil
}

// ValidateInput 验证输入参数
func (a *MyCustomNode) ValidateInput(input *NodeInput) error {
    // 基础验证
    if err := a.BaseActivity.ValidateInput(input); err != nil {
        return err
    }

    // 自定义验证
    action := a.GetStringParameter(input.Parameters, "action")
    if action == "" {
        return fmt.Errorf("action 参数不能为空")
    }

    // 验证动作类型
    validActions := []string{"process", "transform", "validate", "analyze"}
    isValidAction := false
    for _, valid := range validActions {
        if action == valid {
            isValidAction = true
            break
        }
    }

    if !isValidAction {
        return fmt.Errorf("不支持的动作类型: %s", action)
    }

    return nil
}

// processAction 处理具体的业务逻辑
func (a *MyCustomNode) processAction(action string, data map[string]interface{}, options map[string]interface{}) (interface{}, error) {
    switch action {
    case "process":
        return a.processData(data, options)
    case "transform":
        return a.transformData(data, options)
    case "validate":
        return a.validateData(data, options)
    case "analyze":
        return a.analyzeData(data, options)
    default:
        return nil, fmt.Errorf("未知动作: %s", action)
    }
}

// processData 数据处理逻辑
func (a *MyCustomNode) processData(data map[string]interface{}, options map[string]interface{}) (interface{}, error) {
    // 实现数据处理逻辑
    processed := map[string]interface{}{
        "original":  data,
        "processed": true,
        "timestamp": time.Now().Unix(),
    }

    // 添加处理选项
    if strict, ok := options["strict"].(bool); ok && strict {
        processed["mode"] = "strict"
    }

    return processed, nil
}

// transformData 数据转换逻辑
func (a *MyCustomNode) transformData(data map[string]interface{}, options map[string]interface{}) (interface{}, error) {
    // 实现数据转换逻辑
    targetFormat := "json"
    if format, ok := options["format"].(string); ok {
        targetFormat = format
    }

    transformed := map[string]interface{}{
        "data":    data,
        "format":  targetFormat,
        "success": true,
    }

    return transformed, nil
}

// validateData 数据验证逻辑
func (a *MyCustomNode) validateData(data map[string]interface{}, options map[string]interface{}) (interface{}, error) {
    // 实现数据验证逻辑
    validation := map[string]interface{}{
        "valid":    true,
        "errors":   []string{},
        "warnings": []string{},
    }

    // 添加验证规则
    if required, ok := options["required"].([]interface{}); ok {
        for _, field := range required {
            if fieldName, ok := field.(string); ok {
                if _, exists := data[fieldName]; !exists {
                    validation["valid"] = false
                    validation["errors"] = append(validation["errors"].([]string),
                        fmt.Sprintf("缺少必需字段: %s", fieldName))
                }
            }
        }
    }

    return validation, nil
}

// analyzeData 数据分析逻辑
func (a *MyCustomNode) analyzeData(data map[string]interface{}, options map[string]interface{}) (interface{}, error) {
    // 实现数据分析逻辑
    analysis := map[string]interface{}{
        "dataSize":   len(data),
        "fieldCount": len(data),
        "timestamp":  time.Now().Unix(),
    }

    // 分析数据类型
    typeStats := make(map[string]int)
    for key, value := range data {
        typeStats[fmt.Sprintf("%T", value)]++
    }
    analysis["typeStats"] = typeStats

    return analysis, nil
}
```

#### 3. 注册节点

```go
// 在 init 函数中注册节点
func init() {
    RegisterNodeActivity("CUSTOM.myCustomNode", NewMyCustomNode)
}
```

#### 4. 在工作流中处理

```go
// 在 workflow/workflow.go 中添加处理逻辑
case "CUSTOM.myCustomNode":
    var customResult map[string]interface{}
    customActivity := activity.NewMyCustomNode()
    err = workflow.ExecuteActivity(ctx, customActivity.Execute, inputData).Get(ctx, &customResult)
    if err == nil {
        result.Success = true
        result.Data = customResult
    }
```

#### 5. 在 Worker 中注册

```go
// 在 worker/worker.go 中注册活动
w.RegisterActivity(activity.NewMyCustomNode().Execute)
```

### 节点开发最佳实践

#### 1. 错误处理

```go
// 使用结构化错误信息
type NodeError struct {
    Code    string `json:"code"`
    Message string `json:"message"`
    Details interface{} `json:"details,omitempty"`
}

func (e *NodeError) Error() string {
    return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// 在执行逻辑中使用
if err != nil {
    return nil, &NodeError{
        Code:    "VALIDATION_ERROR",
        Message: "输入数据验证失败",
        Details: map[string]interface{}{
            "field": "email",
            "value": input,
        },
    }
}
```

#### 2. 日志记录

```go
func (a *MyCustomNode) executeCustomNode(input *NodeInput) (map[string]interface{}, error) {
    logger := a.GetLogger(ctx)

    logger.Info("开始执行自定义节点",
        "nodeId", input.NodeID,
        "action", a.GetStringParameter(input.Parameters, "action"),
    )

    // 执行逻辑...

    logger.Info("节点执行完成",
        "duration", time.Since(start),
        "resultSize", len(result),
    )

    return result, nil
}
```

#### 3. 性能优化

```go
// 使用缓存优化重复计算
var cache = sync.Map{}

func (a *MyCustomNode) getCachedResult(key string) (interface{}, bool) {
    if value, ok := cache.Load(key); ok {
        return value, true
    }
    return nil, false
}

func (a *MyCustomNode) setCachedResult(key string, value interface{}) {
    cache.Store(key, value)
}
```

#### 4. 单元测试

```go
func TestMyCustomNode(t *testing.T) {
    activity := NewMyCustomNode()

    tests := []struct {
        name     string
        input    *NodeInput
        expected interface{}
        wantErr  bool
    }{
        {
            name: "处理数据成功",
            input: &NodeInput{
                NodeID:   "test-1",
                NodeType: "CUSTOM.myCustomNode",
                Parameters: map[string]interface{}{
                    "action": "process",
                },
                InputData: map[string]interface{}{
                    "test": "data",
                },
            },
            expected: map[string]interface{}{
                "success": true,
                "action":  "process",
            },
            wantErr: false,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            output, err := activity.executeCustomNode(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("executeCustomNode() error = %v, wantErr %v", err, tt.wantErr)
                return
            }
            // 验证结果...
        })
    }
}
```

---

## 📊 性能监控和调试

### 节点执行指标

每个节点都会自动记录以下执行指标：

| 指标名 | 描述 | 类型 |
|--------|------|------|
| `node_execution_duration` | 节点执行耗时 | Histogram |
| `node_execution_total` | 节点执行次数 | Counter |
| `node_execution_errors` | 节点错误次数 | Counter |
| `node_input_size` | 输入数据大小 | Histogram |
| `node_output_size` | 输出数据大小 | Histogram |

### 调试工具

#### 1. 节点执行跟踪

```go
// 启用详细执行跟踪
ctx := context.WithValue(ctx, "debug", true)

// 在节点中记录执行步骤
logger.Debug("执行步骤",
    "step", "validation",
    "input", input,
    "result", validationResult,
)
```

#### 2. 数据快照

```go
// 保存输入输出数据快照用于调试
func (a *MyCustomNode) saveSnapshot(input *NodeInput, output *NodeOutput) {
    snapshot := map[string]interface{}{
        "timestamp":   time.Now(),
        "nodeId":      input.NodeID,
        "nodeType":    input.NodeType,
        "inputData":   input.InputData,
        "parameters":  input.Parameters,
        "outputData":  output.Data,
        "success":     output.Success,
        "duration":    output.ProcessedAt.Sub(time.Now()),
    }

    // 保存到文件或发送到调试服务
    a.saveSnapshotToFile(snapshot)
}
```

---

## 🔗 相关文档

- [项目主文档](../README.md)
- [工作流引擎文档](WORKFLOW_ENGINE.md)
- [Worker 配置文档](WORKER_CONFIG.md)
- [API 参考文档](API_REFERENCE.md)

## 📞 技术支持

如有节点开发相关问题，请：

1. 查阅本文档和相关示例
2. 检查单元测试用例
3. 查看项目 GitHub Issues
4. 提交新的 Issue 或 Pull Request

---

**最后更新**: 2023年10月18日
**文档版本**: v1.0.0
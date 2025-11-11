package wkGraph

import (
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	"n8n2temporal/consts"
	nodepkg "n8n2temporal/node"
	"slices"
	"strings"
)

// WkFlowConn 工作流链接相关信息
type WkFlowConn map[string]map[string][][]struct {
	NodeName string `json:"node"`
	Type     string `json:"type"`
	Index    int    `json:"index"`
}

// Workflow 工作流定义结构
type Workflow struct {
	ID          string               `json:"id"`          // 工作流ID，全局唯一
	Name        string               `json:"name"`        // 工作流名称，由用户自定义
	Nodes       []nodepkg.WkFLowNode `json:"nodes"`       // 工作流所有节点
	Connections WkFlowConn           `json:"connections"` // 工作流节点关系，key表示当前节点，value表示后续节点的名称
	Active      bool                 `json:"active"`      // 是否激活 -- 只有激活的节点才能通过平台发起调用
	VersionId   string               `json:"version_id"`  // 工作流版本ID
}

// WkFlowGraph 封装工作流图结构和解析逻辑
type WkFlowGraph struct {
	Workflow    *Workflow                     // 原始工作流定义
	nodeNameMap map[string]nodepkg.WkFLowNode // 节点名称 -> 节点定义
	nodeIDMap   map[string]string             // 节点ID -> 节点名称
	StartNode   nodepkg.WkFLowNode            // 开始节点
	EndNode     nodepkg.WkFLowNode            // 结束节点
}

// NewWkFlowGraph 初始化工作流图对象
func NewWkFlowGraph(workflowJson string) (*WkFlowGraph, error) {
	// 解析基础工作流定义
	var wkFlow *Workflow
	err := sonic.UnmarshalString(workflowJson, &wkFlow)
	if err != nil {
		return nil, fmt.Errorf("解析工作流 JSON 失败: %v", err)
	}
	// 存储原始工作流定义
	graph := &WkFlowGraph{
		Workflow: wkFlow,
	}
	// 构建节点映射
	graph.nodeNameMap = make(map[string]nodepkg.WkFLowNode)
	graph.nodeIDMap = make(map[string]string)
	for _, node := range wkFlow.Nodes {
		// 节点检查
		if err := node.Check(); err != nil {
			return nil, err
		}
		// 节点名称重复
		_, ok := graph.nodeNameMap[node.Name]
		if ok {
			return nil, fmt.Errorf("节点名称重复")
		}
		// 开始和结束节点的赋值
		if strings.Contains(node.Type, ".start") {
			// 如果Check通过，表示已存在值，这个时候就要报错；如果不存在值，这个时候表示第一次赋值
			if err := graph.StartNode.Check(); err == nil {
				return nil, errors.New("开始节点不唯一")
			}
			graph.StartNode = node
		} else if strings.Contains(node.Type, ".end") {
			if err := graph.EndNode.Check(); err == nil {
				return nil, errors.New("结束节点不唯一")
			}
			graph.EndNode = node
		}
		// 节点名称和ID对应的映射
		graph.nodeNameMap[node.Name] = node
		graph.nodeIDMap[node.ID] = node.Name
	}
	return graph, nil
}

// GetNodeByName 根据节点名称获取节点对象
func (g *WkFlowGraph) GetNodeByName(nodeName string) (nodepkg.WkFLowNode, bool) {
	node, exists := g.nodeNameMap[nodeName]
	return node, exists
}

// GetAllNodes 获取所有节点 -- 返回副本
func (g *WkFlowGraph) GetAllNodes() []nodepkg.WkFLowNode {
	nodes := make([]nodepkg.WkFLowNode, len(g.nodeNameMap))
	i := 0
	for _, node := range g.nodeNameMap {
		nodes[i] = node
		i++
	}
	slices.SortFunc(nodes, func(a, b nodepkg.WkFLowNode) int {
		return strings.Compare(a.ID, b.ID)
	})
	return nodes
}

// GetNextNodes 获取下一个节点
/*
* @param nodeName string 节点名称

* @param branch string 下个节点分支名称（默认为main）

* return nodepkg.WkFLowNode 节点对象

* return error 错误原因
 */
func (g *WkFlowGraph) GetNextNodes(nodeName string, branch string) ([]nodepkg.WkFLowNode, error) {
	var newNode = make([]nodepkg.WkFLowNode, 0)   // 默认响应
	branchMap := g.Workflow.Connections[nodeName] // 获取当前节点的连接信息
	// 遍历所有分支
	for branchName, con := range branchMap {
		if branchName != branch {
			continue
		}
		// 正常情况第一层的长度一定是1,目前该层无特殊意义，兼容格式
		if len(con) < 1 || len(con[0]) < 1 {
			return newNode, consts.WorkFlowConnExcept
		}
		nextCon := con[0]
		// 遍历其分支下所有节点
		for _, nextConn := range nextCon {
			nextNode, ok := g.GetNodeByName(nextConn.NodeName)
			if !ok {
				return newNode, consts.WorkFlowNotFound
			}
			newNode = append(newNode, nextNode)
		}
	}
	slices.SortFunc(newNode, func(a, b nodepkg.WkFLowNode) int {
		return strings.Compare(a.ID, b.ID)
	})
	return newNode, nil
}

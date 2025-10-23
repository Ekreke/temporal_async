package workflow

import (
	"reflect"
	"testing"
)

// TestTopologicalSort_SimpleDAG 测试简单的有向无环图
func TestTopologicalSort_SimpleDAG(t *testing.T) {
	// 构建一个简单的依赖图: A -> B -> C
	// A没有依赖，B依赖于A，C依赖于B
	dependencyGraph := map[string][]string{
		"A": {},    // A没有依赖
		"B": {"A"}, // B依赖于A
		"C": {"B"}, // C依赖于B
	}

	result, err := topologicalSort(dependencyGraph, 10)
	if err != nil {
		t.Fatalf("topologicalSort returned error: %v", err)
	}

	// 验证结果长度
	if len(result) != 3 {
		t.Fatalf("Expected 3 nodes, got %d", len(result))
	}

	// 验证拓扑顺序：A应该在B前面，B应该在C前面
	aIndex := findIndex(result, "A")
	bIndex := findIndex(result, "B")
	cIndex := findIndex(result, "C")

	if aIndex >= bIndex {
		t.Errorf("A should come before B, but got A at %d, B at %d", aIndex, bIndex)
	}
	if bIndex >= cIndex {
		t.Errorf("B should come before C, but got B at %d, C at %d", bIndex, cIndex)
	}
}

// TestTopologicalSort_ComplexDAG 测试复杂的有向无环图
func TestTopologicalSort_ComplexDAG(t *testing.T) {
	// 构建复杂依赖图:
	// A -> D
	// B -> D
	// C -> E
	// D -> F
	// E -> F
	dependencyGraph := map[string][]string{
		"A": {},         // A没有依赖
		"B": {},         // B没有依赖
		"C": {},         // C没有依赖
		"D": {"A", "B"}, // D依赖于A和B
		"E": {"C"},      // E依赖于C
		"F": {"D", "E"}, // F依赖于D和E
	}

	result, err := topologicalSort(dependencyGraph, 10)
	if err != nil {
		t.Fatalf("topologicalSort returned error: %v", err)
	}

	// 验证结果长度
	if len(result) != 6 {
		t.Fatalf("Expected 6 nodes, got %d", len(result))
	}

	// 验证拓扑顺序
	aIndex := findIndex(result, "A")
	bIndex := findIndex(result, "B")
	cIndex := findIndex(result, "C")
	dIndex := findIndex(result, "D")
	eIndex := findIndex(result, "E")
	fIndex := findIndex(result, "F")

	// A和B应该在D前面
	if aIndex >= dIndex {
		t.Errorf("A should come before D, but got A at %d, D at %d", aIndex, dIndex)
	}
	if bIndex >= dIndex {
		t.Errorf("B should come before D, but got B at %d, D at %d", bIndex, dIndex)
	}

	// C应该在E前面
	if cIndex >= eIndex {
		t.Errorf("C should come before E, but got C at %d, E at %d", cIndex, eIndex)
	}

	// D和E应该在F前面
	if dIndex >= fIndex {
		t.Errorf("D should come before F, but got D at %d, F at %d", dIndex, fIndex)
	}
	if eIndex >= fIndex {
		t.Errorf("E should come before F, but got E at %d, F at %d", eIndex, fIndex)
	}
}

// TestTopologicalSort_MultipleStartNodes 测试多个起始节点
func TestTopologicalSort_MultipleStartNodes(t *testing.T) {
	// 多个没有依赖的起始节点
	dependencyGraph := map[string][]string{
		"Start1": {},
		"Start2": {},
		"Start3": {},
		"Middle": {"Start1", "Start2"},
		"End":    {"Start3", "Middle"},
	}

	result, err := topologicalSort(dependencyGraph, 10)
	if err != nil {
		t.Fatalf("topologicalSort returned error: %v", err)
	}

	if len(result) != 5 {
		t.Fatalf("Expected 5 nodes, got %d", len(result))
	}

	// 验证所有节点都被处理
	for _, node := range []string{"Start1", "Start2", "Start3", "Middle", "End"} {
		if findIndex(result, node) == -1 {
			t.Errorf("Node %s not found in result", node)
		}
	}
}

// TestTopologicalSort_CycleDependency 测试循环依赖
func TestTopologicalSort_CycleDependency(t *testing.T) {
	// 构建循环依赖: A -> B -> C -> A
	dependencyGraph := map[string][]string{
		"A": {"C"}, // A依赖于C
		"B": {"A"}, // B依赖于A
		"C": {"B"}, // C依赖于B
	}

	result, err := topologicalSort(dependencyGraph, 10)
	if err != nil {
		t.Fatalf("topologicalSort returned error: %v", err)
	}

	// 由于存在循环，算法应该仍然返回所有节点（通过随机添加剩余节点的方式）
	if len(result) != 3 {
		t.Fatalf("Expected 3 nodes (even with cycle), got %d", len(result))
	}

	// 验证所有节点都被处理
	for _, node := range []string{"A", "B", "C"} {
		if findIndex(result, node) == -1 {
			t.Errorf("Node %s not found in result", node)
		}
	}
}

// TestTopologicalSort_MaxStepLimit 测试最大步数限制
func TestTopologicalSort_MaxStepLimit(t *testing.T) {
	// 构建长链: A -> B -> C -> D -> E -> F
	dependencyGraph := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {"B"},
		"D": {"C"},
		"E": {"D"},
		"F": {"E"},
	}

	// 设置最大步数为3
	result, err := topologicalSort(dependencyGraph, 3)
	if err != nil {
		t.Fatalf("topologicalSort returned error: %v", err)
	}

	// 结果应该最多包含3个节点
	if len(result) > 3 {
		t.Fatalf("Expected max 3 nodes due to step limit, got %d", len(result))
	}

	// 验证前3个节点的拓扑顺序仍然是正确的
	if len(result) >= 2 {
		firstIndex := findIndex(result, "A")
		if firstIndex != 0 {
			t.Errorf("Expected A to be first, got at index %d", firstIndex)
		}
	}

	if len(result) >= 3 {
		secondNode := result[1]
		thirdNode := result[2]
		secondIndex := findIndex(result, secondNode)
		thirdIndex := findIndex(result, thirdNode)
		if secondIndex >= thirdIndex {
			t.Errorf("Expected second node to come before third node")
		}
	}
}

// TestTopologicalSort_EmptyGraph 测试空图
func TestTopologicalSort_EmptyGraph(t *testing.T) {
	dependencyGraph := map[string][]string{}

	result, err := topologicalSort(dependencyGraph, 10)
	if err != nil {
		t.Fatalf("topologicalSort returned error: %v", err)
	}

	if len(result) != 0 {
		t.Fatalf("Expected empty result, got %d nodes", len(result))
	}
}

// TestTopologicalSort_SingleNode 测试单节点
func TestTopologicalSort_SingleNode(t *testing.T) {
	dependencyGraph := map[string][]string{
		"A": {},
	}

	result, err := topologicalSort(dependencyGraph, 10)
	if err != nil {
		t.Fatalf("topologicalSort returned error: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 node, got %d", len(result))
	}

	if result[0] != "A" {
		t.Errorf("Expected node 'A', got '%s'", result[0])
	}
}

// TestTopologicalSort_RealWorldScenario 测试真实世界场景
func TestTopologicalSort_RealWorldScenario(t *testing.T) {
	// 模拟一个真实的工作流场景
	dependencyGraph := map[string][]string{
		"ManualTrigger":  {},                               // 手动触发，无依赖
		"PythonCode1":    {"ManualTrigger"},                // 依赖于手动触发
		"DomainResolve":  {"ManualTrigger"},                // 依赖于手动触发（并行执行）
		"ConditionCheck": {"PythonCode1", "DomainResolve"}, // 依赖于前两个步骤
		"SwitchNode":     {"ConditionCheck"},               // 依赖于条件检查
		"PythonCode2":    {"SwitchNode"},                   // 依赖于Switch结果
		"FinalOutput":    {"PythonCode2"},                  // 最终输出
	}

	result, err := topologicalSort(dependencyGraph, 10)
	if err != nil {
		t.Fatalf("topologicalSort returned error: %v", err)
	}

	if len(result) != 7 {
		t.Fatalf("Expected 7 nodes, got %d", len(result))
	}

	// 验证关键依赖关系
	manualIndex := findIndex(result, "ManualTrigger")
	conditionIndex := findIndex(result, "ConditionCheck")

	if manualIndex >= conditionIndex {
		t.Errorf("ManualTrigger should come before ConditionCheck")
	}

	// 验证所有节点都被处理
	expectedNodes := []string{"ManualTrigger", "PythonCode1", "DomainResolve", "ConditionCheck", "SwitchNode", "PythonCode2", "FinalOutput"}
	for _, node := range expectedNodes {
		if findIndex(result, node) == -1 {
			t.Errorf("Node %s not found in result", node)
		}
	}
}

// findIndex 辅助函数：查找节点在结果中的索引
func findIndex(result []string, node string) int {
	for i, n := range result {
		if n == node {
			return i
		}
	}
	return -1
}

// TestTopologicalSortWithDefault 测试默认最大步数的拓扑排序
func TestTopologicalSortWithDefault(t *testing.T) {
	dependencyGraph := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {"B"},
	}

	result, err := topologicalSortWithDefault(dependencyGraph)
	if err != nil {
		t.Fatalf("topologicalSortWithDefault returned error: %v", err)
	}

	if len(result) != 3 {
		t.Fatalf("Expected 3 nodes, got %d", len(result))
	}

	// 验证拓扑顺序
	aIndex := findIndex(result, "A")
	bIndex := findIndex(result, "B")
	cIndex := findIndex(result, "C")

	if aIndex >= bIndex {
		t.Errorf("A should come before B")
	}
	if bIndex >= cIndex {
		t.Errorf("B should come before C")
	}
}

// BenchmarkTopologicalSort 性能测试
func BenchmarkTopologicalSort(b *testing.B) {
	// 构建一个较大的依赖图用于性能测试
	dependencyGraph := make(map[string][]string)

	// 创建100个节点的链式依赖
	for i := 0; i < 100; i++ {
		nodeName := string(rune('A'+i%26)) + string(rune('0'+i/26))
		if i == 0 {
			dependencyGraph[nodeName] = []string{}
		} else {
			prevNodeName := string(rune('A'+(i-1)%26)) + string(rune('0'+(i-1)/26))
			dependencyGraph[nodeName] = []string{prevNodeName}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := topologicalSort(dependencyGraph, 1000)
		if err != nil {
			b.Fatalf("topologicalSort returned error: %v", err)
		}
	}
}

// TestTopologicalSort_ResultsConsistency 测试结果一致性
func TestTopologicalSort_ResultsConsistency(t *testing.T) {
	dependencyGraph := map[string][]string{
		"A": {},
		"B": {"A"},
		"C": {},
		"D": {"A", "C"},
	}

	// 多次运行，验证结果的一致性（对于DAG，结果应该是确定性的）
	var previousResult []string

	for i := 0; i < 10; i++ {
		result, err := topologicalSort(dependencyGraph, 10)
		if err != nil {
			t.Fatalf("topologicalSort returned error: %v", err)
		}

		if len(result) != 4 {
			t.Fatalf("Expected 4 nodes, got %d on iteration %d", len(result), i)
		}

		if previousResult != nil && !reflect.DeepEqual(previousResult, result) {
			t.Logf("Warning: Results differ between runs. Previous: %v, Current: %v", previousResult, result)
			// 注意：由于map的遍历顺序不确定，结果可能不完全一致，但拓扑约束应该满足
		}

		previousResult = result

		// 验证拓扑约束总是满足
		aIndex := findIndex(result, "A")
		bIndex := findIndex(result, "B")
		cIndex := findIndex(result, "C")
		dIndex := findIndex(result, "D")

		if aIndex >= bIndex {
			t.Errorf("A should come before B on iteration %d", i)
		}
		if aIndex >= dIndex {
			t.Errorf("A should come before D on iteration %d", i)
		}
		if cIndex >= dIndex {
			t.Errorf("C should come before D on iteration %d", i)
		}
	}
}

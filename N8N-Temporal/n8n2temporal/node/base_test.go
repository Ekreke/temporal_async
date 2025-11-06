package node

import (
	"regexp"
	"strings"
	"testing"
)

// TestResolveNodeReference 校验正则表达式是否符合预期
func TestResolveNodeReference(t *testing.T) {
	expression := "$('NodeName').item.json.hello"
	re := regexp.MustCompile(`\$\('([^']+)'\)\.item\.(json)?\.?(\w+|\["[^"]+"\])`)
	matches := re.FindStringSubmatch(expression)
	println(len(matches))
	println(strings.Join(matches, "->"))
}

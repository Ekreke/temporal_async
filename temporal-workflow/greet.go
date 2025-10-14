package temporal_workflow

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"slices"
	"time"

	"go.temporal.io/sdk/activity"
)

// 能力1
func Greet(ctx context.Context, name string) (string, error) {
	log.Println("start greet")
	activity.RecordHeartbeat(ctx, "start greet", "try two")
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second * 1)
		log.Printf("已等待 %d 秒\n", i+1)
		activity.RecordHeartbeat(ctx, fmt.Sprintf("已等待 %d 秒", i+1), "try two")
	}
	log.Println("continue greet")
	return fmt.Sprintf("Hello %s", name), nil
}

// 能力2
func Greet2(ctx context.Context, name string) (string, error) {
	log.Println("start greet2")
	activity.RecordHeartbeat(ctx, "start greet22", "try two2")
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second * 1)
		log.Printf("greet2已等待 %d 秒\n", i+1)
		activity.RecordHeartbeat(ctx, fmt.Sprintf("greet2.1已等待 %d 秒", i+1), "try two2")
	}
	log.Println("continue greet2")
	return fmt.Sprintf("greet2、Hello %s", name), nil
}

// 以下为ASM各个能力方组成activity

// 随机结果数组
var results = []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff", "gggg"}

// 域名解析 -- 输入域名、返回该域名解析记录
func DomainResolveTask(ctx context.Context, domain string, data string) ([]string, error) {
	// todo 改成异步的
	// 结果
	var res = []string{}
	hashValues := fmt.Sprintf("%s_%s", domain, data)
	// 第一次sha56
	hashBytes := sha256.Sum256([]byte(hashValues))
	hashUint32 := binary.BigEndian.Uint32(hashBytes[:4])
	remainder := hashUint32 % uint32(len(results))
	res = append(res, results[remainder])
	// 第二次md5
	md5Bytes := md5.Sum([]byte(hashValues))
	md5Uint32 := binary.BigEndian.Uint32(md5Bytes[:4])
	md5Remainder := md5Uint32 % uint32(len(results))
	res = append(res, results[md5Remainder])
	return res, nil
}

type SpiderResult struct {
	Domain string
	Data   interface{}
}

// 爬虫解析
func SpiderTask(ctx context.Context, domain string) (SpiderResult, error) {
	if slices.Index(results, domain) > (len(results) / 2) {
	}
	return SpiderResult{
		Domain: domain,
		Data:   "spider result",
	}, nil
}

// ==================== 以下是新的5个Activity ====================

// ActivityA1 - 第一个Activity
func ActivityA1(ctx context.Context, input int) ([]int, error) {
	log.Printf("ActivityA1 开始执行，输入: %d", input)
	activity.RecordHeartbeat(ctx, "ActivityA1 开始")

	// 模拟一些处理时间
	time.Sleep(time.Millisecond * 100)

	// 生成3-9个随机值（范围1-100），并去重
	count := rand.Intn(3) + 1 // 1-3之间的随机数
	results := make(map[int]bool)
	for len(results) < count {
		value := rand.Intn(10) + 1 // 1-100之间的随机数
		results[value] = true
	}

	// 转换为切片
	output := make([]int, 0, len(results))
	for value := range results {
		output = append(output, value)
	}

	log.Printf("ActivityA1 完成，输出: %v", output)
	return output, nil
}

// ActivityA2 - 第二个Activity
func ActivityA2(ctx context.Context, input int) ([]int, error) {
	log.Printf("ActivityA2 开始执行，输入: %d", input)
	activity.RecordHeartbeat(ctx, "ActivityA2 开始")

	// 模拟一些处理时间
	time.Sleep(time.Millisecond * 100)

	// 生成3-9个随机值（范围1-100），并去重
	count := rand.Intn(3) + 1 // 1-3之间的随机数
	results := make(map[int]bool)
	for len(results) < count {
		value := rand.Intn(10) + 1 // 1-100之间的随机数
		results[value] = true
	}

	// 转换为切片
	output := make([]int, 0, len(results))
	for value := range results {
		output = append(output, value)
	}

	log.Printf("ActivityA2 完成，输出: %v", output)
	return output, nil
}

// ActivityA3 - 第三个Activity
func ActivityA3(ctx context.Context, input int) ([]int, error) {
	log.Printf("ActivityA3 开始执行，输入: %d", input)
	activity.RecordHeartbeat(ctx, "ActivityA3 开始")

	// 模拟一些处理时间
	time.Sleep(time.Millisecond * 100)

	// 生成3-9个随机值（范围1-100），并去重
	count := rand.Intn(3) + 1 // 1-3之间的随机数
	results := make(map[int]bool)
	for len(results) < count {
		value := rand.Intn(10) + 1 // 1-100之间的随机数
		results[value] = true
	}

	// 转换为切片
	output := make([]int, 0, len(results))
	for value := range results {
		output = append(output, value)
	}

	log.Printf("ActivityA3 完成，输出: %v", output)
	return output, nil
}

// ActivityA4 - 第四个Activity
func ActivityA4(ctx context.Context, input int) ([]int, error) {
	log.Printf("ActivityA4 开始执行，输入: %d", input)
	activity.RecordHeartbeat(ctx, "ActivityA4 开始")

	// 模拟一些处理时间
	time.Sleep(time.Millisecond * 100)

	// 生成3-9个随机值（范围1-100），并去重
	count := rand.Intn(3) + 1 // 1-3之间的随机数
	results := make(map[int]bool)
	for len(results) < count {
		value := rand.Intn(10) + 1 // 1-100之间的随机数
		results[value] = true
	}

	// 转换为切片
	output := make([]int, 0, len(results))
	for value := range results {
		output = append(output, value)
	}

	log.Printf("ActivityA4 完成，输出: %v", output)
	return output, nil
}

// ActivityA5 - 第五个Activity
func ActivityA5(ctx context.Context, input int) ([]int, error) {
	log.Printf("ActivityA5 开始执行，输入: %d", input)
	activity.RecordHeartbeat(ctx, "ActivityA5 开始")

	// 模拟一些处理时间
	time.Sleep(time.Millisecond * 100)

	// 生成3-9个随机值（范围1-100），并去重
	count := rand.Intn(3) + 1 // 1-3之间的随机数
	results := make(map[int]bool)
	for len(results) < count {
		value := rand.Intn(10) + 1 // 1-100之间的随机数
		results[value] = true
	}

	// 转换为切片
	output := make([]int, 0, len(results))
	for value := range results {
		output = append(output, value)
	}

	log.Printf("ActivityA5 完成，输出: %v", output)
	return output, nil
}

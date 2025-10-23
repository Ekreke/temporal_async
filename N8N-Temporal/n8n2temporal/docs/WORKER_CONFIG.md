# Worker 配置详细文档

本文档详细介绍了 N8N to Temporal 工作流转换引擎的 Worker 配置、部署、监控和维护方法。

## 📋 目录

- [Worker 架构概述](#worker-架构概述)
- [基础配置](#基础配置)
- [高级配置选项](#高级配置选项)
- [活动注册](#活动注册)
- [性能调优](#性能调优)
- [监控和日志](#监控和日志)
- [部署策略](#部署策略)
- [故障排除](#故障排除)

## 🏗️ Worker 架构概述

### 核心组件

```
Worker 架构
┌─────────────────────────────────────────────────────────────┐
│                    Worker 进程                               │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  工作流注册   │  │  活动注册    │  │  拦截器配置   │         │
│  │ Workflow    │  │ Activity    │  │ Interceptor │         │
│  │ Registry    │  │ Registry    │  │ Config      │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  任务队列监听 │  │  执行调度器   │  │  结果收集器   │         │
│  │ Task Queue  │  │ Scheduler   │  │ Collector    │         │
│  │ Listener    │  │             │  │              │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                    Temporal SDK 层                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  Worker      │  │  Client      │  │  Logger      │         │
│  │ Instance    │  │ Connection  │  │ Service      │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

### Worker 生命周期

```
Worker 启动流程
1. 解析命令行参数
   ↓
2. 加载配置文件
   ↓
3. 创建 Temporal 客户端
   ↓
4. 创建 Worker 实例
   ↓
5. 注册工作流和活动
   ↓
6. 配置拦截器和选项
   ↓
7. 启动 Worker 监听
   ↓
8. 处理任务直到收到停止信号
```

## ⚙️ 基础配置

### 最小化配置

```go
package main

import (
    "log"
    "go.temporal.io/sdk/client"
    "go.temporal.io/sdk/worker"
    "n8n2temporal/workflow"
    "n8n2temporal/activity"
)

func main() {
    // 创建 Temporal 客户端
    c, err := client.Dial(client.Options{
        HostPort: "localhost:7233",
    })
    if err != nil {
        log.Fatalln("无法创建 Temporal 客户端", err)
    }
    defer c.Close()

    // 创建 Worker
    w := worker.New(c, "n8n-conversion-queue", worker.Options{})

    // 注册工作流
    w.RegisterWorkflow(workflow.GenericWorkflow)

    // 注册活动
    w.RegisterActivity(activity.NewDomainResolveActivity().ExecuteDomainResolve)
    w.RegisterActivity(activity.NewPythonCodeActivity().ExecutePythonCode)
    w.RegisterActivity(activity.NewConditionCheckActivity().ExecuteIf)
    w.RegisterActivity(activity.NewSwitchNodeActivity().ExecuteSwitchNode)
    w.RegisterActivity(activity.NewCustomNodeActivity().ExecuteCustomNode)

    // 启动 Worker
    log.Println("启动 n8n 转换 Worker...")
    if err := w.Run(worker.InterruptCh()); err != nil {
        log.Fatalln("Worker 运行失败:", err)
    }
}
```

### 配置文件支持

创建 `config.yaml` 配置文件：

```yaml
# Temporal 连接配置
temporal:
  host_port: "localhost:7233"
  namespace: "default"

# Worker 配置
worker:
  task_queue: "n8n-conversion-queue"
  identity: "n8n-worker-1"
  max_concurrent_activity_execution_size: 100
  max_concurrent_workflow_task_execution_size: 100
  max_concurrent_local_activity_execution_size: 100

# 日志配置
logging:
  level: "info"
  format: "json"
  output: "stdout"

# 指标配置
metrics:
  enabled: true
  prometheus_port: 9090

# 健康检查配置
health:
  enabled: true
  port: 8080
  path: "/health"

# 调试配置
debug:
  enabled: false
  trace_export_path: "./traces"
```

配置加载器：

```go
// Config 配置结构
type Config struct {
    Temporal TemporalConfig `yaml:"temporal"`
    Worker   WorkerConfig   `yaml:"worker"`
    Logging  LoggingConfig  `yaml:"logging"`
    Metrics  MetricsConfig  `yaml:"metrics"`
    Health   HealthConfig   `yaml:"health"`
    Debug    DebugConfig    `yaml:"debug"`
}

type TemporalConfig struct {
    HostPort string `yaml:"host_port"`
    Namespace string `yaml:"namespace"`
}

type WorkerConfig struct {
    TaskQueue                           string `yaml:"task_queue"`
    Identity                            string `yaml:"identity"`
    MaxConcurrentActivityExecutionSize   int    `yaml:"max_concurrent_activity_execution_size"`
    MaxConcurrentWorkflowTaskExecutionSize int  `yaml:"max_concurrent_workflow_task_execution_size"`
    MaxConcurrentLocalActivityExecutionSize int  `yaml:"max_concurrent_local_activity_execution_size"`
}

// LoadConfig 加载配置文件
func LoadConfig(filename string) (*Config, error) {
    data, err := os.ReadFile(filename)
    if err != nil {
        return nil, fmt.Errorf("读取配置文件失败: %v", err)
    }

    var config Config
    if err := yaml.Unmarshal(data, &config); err != nil {
        return nil, fmt.Errorf("解析配置文件失败: %v", err)
    }

    // 设置默认值
    setDefaults(&config)

    return &config, nil
}

// setDefaults 设置默认配置值
func setDefaults(config *Config) {
    if config.Temporal.HostPort == "" {
        config.Temporal.HostPort = "localhost:7233"
    }
    if config.Temporal.Namespace == "" {
        config.Temporal.Namespace = "default"
    }
    if config.Worker.TaskQueue == "" {
        config.Worker.TaskQueue = "n8n-conversion-queue"
    }
    if config.Worker.Identity == "" {
        config.Worker.Identity = fmt.Sprintf("n8n-worker-%d", os.Getpid())
    }
    if config.Logging.Level == "" {
        config.Logging.Level = "info"
    }
}
```

使用配置文件启动 Worker：

```go
func main() {
    // 加载配置
    config, err := LoadConfig("config.yaml")
    if err != nil {
        log.Fatalf("加载配置失败: %v", err)
    }

    // 创建 Temporal 客户端
    c, err := client.Dial(client.Options{
        HostPort: config.Temporal.HostPort,
        Namespace: config.Temporal.Namespace,
    })
    if err != nil {
        log.Fatalf("无法创建 Temporal 客户端: %v", err)
    }
    defer c.Close()

    // 创建 Worker 选项
    workerOptions := worker.Options{
        Identity: config.Worker.Identity,
        MaxConcurrentActivityExecutionSize:   config.Worker.MaxConcurrentActivityExecutionSize,
        MaxConcurrentWorkflowTaskExecutionSize: config.Worker.MaxConcurrentWorkflowTaskExecutionSize,
        MaxConcurrentLocalActivityExecutionSize: config.Worker.MaxConcurrentLocalActivityExecutionSize,
    }

    // 配置拦截器
    if config.Metrics.Enabled {
        workerOptions.Interceptors = append(workerOptions.Interceptors, NewMetricsInterceptor())
    }

    // 创建 Worker
    w := worker.New(c, config.Worker.TaskQueue, workerOptions)

    // 注册工作流和活动
    registerWorkflowsAndActivities(w)

    // 启动健康检查服务器
    if config.Health.Enabled {
        go startHealthServer(config.Health)
    }

    // 启动指标服务器
    if config.Metrics.Enabled {
        go startMetricsServer(config.Metrics)
    }

    // 启动 Worker
    log.Printf("启动 n8n 转换 Worker (identity: %s)", config.Worker.Identity)
    if err := w.Run(worker.InterruptCh()); err != nil {
        log.Fatalf("Worker 运行失败: %v", err)
    }
}
```

## 🔧 高级配置选项

### 拦截器配置

```go
// MetricsInterceptor 指标收集拦截器
type MetricsInterceptor struct {
    activityCounter   prometheus.Counter
    activityHistogram prometheus.Histogram
    workflowCounter   prometheus.Counter
    workflowHistogram prometheus.Histogram
}

func NewMetricsInterceptor() *MetricsInterceptor {
    return &MetricsInterceptor{
        activityCounter: prometheus.NewCounter(prometheus.CounterOpts{
            Name: "n8n_activity_executions_total",
            Help: "Total number of activity executions",
        }),
        activityHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{
            Name:    "n8n_activity_execution_duration_seconds",
            Help:    "Duration of activity executions",
            Buckets: prometheus.DefBuckets,
        }),
        workflowCounter: prometheus.NewCounter(prometheus.CounterOpts{
            Name: "n8n_workflow_executions_total",
            Help: "Total number of workflow executions",
        }),
        workflowHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{
            Name:    "n8n_workflow_execution_duration_seconds",
            Help:    "Duration of workflow executions",
            Buckets: prometheus.DefBuckets,
        }),
    }
}

func (i *MetricsInterceptor) InterceptActivity(ctx context.Context, in interceptors.ActivityInboundInterceptor) (interceptors.ActivityOutboundInterceptor, error) {
    info := in.GetActivityInfo()

    start := time.Now()

    return interceptors.NewActivityOutboundInterceptor(
        func(ctx context.Context, next interceptors.ActivityOutboundInterceptor) error {
            defer func() {
                duration := time.Since(start).Seconds()
                i.activityHistogram.WithLabelValues(info.ActivityType.Name()).Observe(duration)
                i.activityCounter.WithLabelValues(info.ActivityType.Name()).Inc()
            }()
            return next.ExecuteActivity(ctx)
        },
        func(ctx context.Context, next interceptors.ActivityOutboundInterceptor) (interface{}, error) {
            return next.ExecuteActivity(ctx)
        },
    ), nil
}

// LoggingInterceptor 日志拦截器
type LoggingInterceptor struct {
    logger log.Logger
}

func NewLoggingInterceptor(logger log.Logger) *LoggingInterceptor {
    return &LoggingInterceptor{logger: logger}
}

func (i *LoggingInterceptor) InterceptActivity(ctx context.Context, in interceptors.ActivityInboundInterceptor) (interceptors.ActivityOutboundInterceptor, error) {
    info := in.GetActivityInfo()

    i.logger.Info("开始执行活动",
        "activityType", info.ActivityType.Name(),
        "activityId", info.ActivityID,
        "workflowId", info.WorkflowExecution.ID,
        "attempt", info.Attempt,
    )

    return interceptors.NewActivityOutboundInterceptor(
        func(ctx context.Context, next interceptors.ActivityOutboundInterceptor) error {
            start := time.Now()
            err := next.ExecuteActivity(ctx)
            duration := time.Since(start)

            if err != nil {
                i.logger.Error("活动执行失败",
                    "activityType", info.ActivityType.Name(),
                    "activityId", info.ActivityID,
                    "duration", duration,
                    "error", err,
                )
            } else {
                i.logger.Info("活动执行成功",
                    "activityType", info.ActivityType.Name(),
                    "activityId", info.ActivityID,
                    "duration", duration,
                )
            }

            return err
        },
        func(ctx context.Context, next interceptors.ActivityOutboundInterceptor) (interface{}, error) {
            return next.ExecuteActivity(ctx)
        },
    ), nil
}
```

### 重试策略配置

```go
// RetryPolicy 重试策略配置
type RetryPolicy struct {
    InitialInterval    time.Duration `yaml:"initial_interval"`
    BackoffCoefficient float64       `yaml:"backoff_coefficient"`
    MaximumInterval    time.Duration `yaml:"maximum_interval"`
    MaximumAttempts    int           `yaml:"maximum_attempts"`
    NonRetryableErrors []string      `yaml:"non_retryable_errors"`
}

// GetActivityRetryOptions 获取活动重试选项
func GetActivityRetryOptions(policy RetryPolicy) activity.Options {
    return activity.Options{
        RetryPolicy: &temporal.RetryPolicy{
            InitialInterval:    policy.InitialInterval,
            BackoffCoefficient: policy.BackoffCoefficient,
            MaximumInterval:    policy.MaximumInterval,
            MaximumAttempts:    policy.MaximumAttempts,
            NonRetryableErrorTypes: policy.NonRetryableErrors,
        },
    }
}

// 预定义重试策略
var (
    DefaultRetryPolicy = RetryPolicy{
        InitialInterval:    time.Second * 1,
        BackoffCoefficient: 2.0,
        MaximumInterval:    time.Second * 30,
        MaximumAttempts:    3,
        NonRetryableErrors: []string{"ValidationError", "PermissionError"},
    }

    AggressiveRetryPolicy = RetryPolicy{
        InitialInterval:    time.Millisecond * 500,
        BackoffCoefficient: 1.5,
        MaximumInterval:    time.Second * 10,
        MaximumAttempts:    5,
        NonRetryableErrors: []string{},
    }

    ConservativeRetryPolicy = RetryPolicy{
        InitialInterval:    time.Second * 2,
        BackoffCoefficient: 2.5,
        MaximumInterval:    time.Minute * 1,
        MaximumAttempts:    2,
        NonRetryableErrors: []string{"ValidationError", "PermissionError", "NetworkError"},
    }
)
```

### 资源限制配置

```go
// ResourceLimits 资源限制配置
type ResourceLimits struct {
    MaxMemoryMB     int64         `yaml:"max_memory_mb"`
    MaxCPUPercent   float64       `yaml:"max_cpu_percent"`
    MaxGoroutines   int           `yaml:"max_goroutines"`
    MaxConnections  int           `yaml:"max_connections"`
    GCPercent       int           `yaml:"gc_percent"`
    MaxIdleTime     time.Duration `yaml:"max_idle_time"`
}

// ResourceMonitor 资源监控器
type ResourceMonitor struct {
    limits ResourceLimits
    ticker *time.Ticker
    stopCh chan struct{}
}

func NewResourceMonitor(limits ResourceLimits) *ResourceMonitor {
    return &ResourceMonitor{
        limits: limits,
        ticker: time.NewTicker(time.Second * 10),
        stopCh: make(chan struct{}),
    }
}

func (m *ResourceMonitor) Start() {
    go func() {
        for {
            select {
            case <-m.ticker.C:
                m.checkResources()
            case <-m.stopCh:
                return
            }
        }
    }()
}

func (m *ResourceMonitor) Stop() {
    close(m.stopCh)
    m.ticker.Stop()
}

func (m *ResourceMonitor) checkResources() {
    // 检查内存使用
    var memStats runtime.MemStats
    runtime.ReadMemStats(&memStats)

    memoryMB := int64(memStats.Alloc / 1024 / 1024)
    if memoryMB > m.limits.MaxMemoryMB {
        log.Warn("内存使用超过限制",
            "current", memoryMB,
            "limit", m.limits.MaxMemoryMB,
        )

        // 触发 GC
        runtime.GC()
    }

    // 检查 Goroutine 数量
    goroutineCount := runtime.NumGoroutine()
    if goroutineCount > m.limits.MaxGoroutines {
        log.Warn("Goroutine 数量超过限制",
            "current", goroutineCount,
            "limit", m.limits.MaxGoroutines,
        )
    }
}
```

## 📝 活动注册

### 动态活动注册

```go
// ActivityRegistry 活动注册表
type ActivityRegistry struct {
    activities map[string]activity.Activity
    mutex      sync.RWMutex
}

func NewActivityRegistry() *ActivityRegistry {
    return &ActivityRegistry{
        activities: make(map[string]activity.Activity),
    }
}

func (r *ActivityRegistry) Register(name string, activity activity.Activity) error {
    r.mutex.Lock()
    defer r.mutex.Unlock()

    if _, exists := r.activities[name]; exists {
        return fmt.Errorf("活动 %s 已经注册", name)
    }

    r.activities[name] = activity
    log.Info("注册活动", "name", name)
    return nil
}

func (r *ActivityRegistry) RegisterAll(worker worker.Worker) error {
    r.mutex.RLock()
    defer r.mutex.RUnlock()

    for name, activity := range r.activities {
        worker.RegisterActivity(activity)
        log.Info("在 Worker 中注册活动", "name", name)
    }

    return nil
}

func (r *ActivityRegistry) List() []string {
    r.mutex.RLock()
    defer r.mutex.RUnlock()

    names := make([]string, 0, len(r.activities))
    for name := range r.activities {
        names = append(names, name)
    }

    return names
}

// 注册所有活动
func registerAllActivities(registry *ActivityRegistry) error {
    // 域名解析活动
    if err := registry.Register("DomainResolve", activity.NewDomainResolveActivity().ExecuteDomainResolve); err != nil {
        return err
    }

    // Python 代码执行活动
    if err := registry.Register("PythonCode", activity.NewPythonCodeActivity().ExecutePythonCode); err != nil {
        return err
    }

    // 条件判断活动
    if err := registry.Register("IfCondition", activity.NewConditionCheckActivity().ExecuteIf); err != nil {
        return err
    }

    // Switch 分支活动
    if err := registry.Register("SwitchBranch", activity.NewSwitchNodeActivity().ExecuteSwitchNode); err != nil {
        return err
    }

    // 自定义节点活动
    if err := registry.Register("CustomNode", activity.NewCustomNodeActivity().ExecuteCustomNode); err != nil {
        return err
    }

    return nil
}
```

### 条件性活动注册

```go
// ConditionalActivityRegister 条件性活动注册器
type ConditionalActivityRegister struct {
    config ActivityConfig
}

type ActivityConfig struct {
    EnableDomainResolve bool `yaml:"enable_domain_resolve"`
    EnablePythonCode    bool `yaml:"enable_python_code"`
    EnableIfCondition   bool `yaml:"enable_if_condition"`
    EnableSwitchBranch  bool `yaml:"enable_switch_branch"`
    EnableCustomNode    bool `yaml:"enable_custom_node"`
}

func (r *ConditionalActivityRegister) RegisterActivities(worker worker.Worker) {
    if r.config.EnableDomainResolve {
        worker.RegisterActivity(activity.NewDomainResolveActivity().ExecuteDomainResolve)
        log.Info("启用域名解析活动")
    }

    if r.config.EnablePythonCode {
        worker.RegisterActivity(activity.NewPythonCodeActivity().ExecutePythonCode)
        log.Info("启用 Python 代码执行活动")
    }

    if r.config.EnableIfCondition {
        worker.RegisterActivity(activity.NewConditionCheckActivity().ExecuteIf)
        log.Info("启用条件判断活动")
    }

    if r.config.EnableSwitchBranch {
        worker.RegisterActivity(activity.NewSwitchNodeActivity().ExecuteSwitchNode)
        log.Info("启用分支选择活动")
    }

    if r.config.EnableCustomNode {
        worker.RegisterActivity(activity.NewCustomNodeActivity().ExecuteCustomNode)
        log.Info("启用自定义节点活动")
    }
}
```

## 🚀 性能调优

### 并发控制

```go
// ConcurrencyConfig 并发配置
type ConcurrencyConfig struct {
    MaxConcurrentActivityExecutions     int           `yaml:"max_concurrent_activity_executions"`
    MaxConcurrentWorkflowExecutions     int           `yaml:"max_concurrent_workflow_executions"`
    MaxConcurrentLocalActivityExecutions int           `yaml:"max_concurrent_local_activity_executions"`
    TaskQueueHandshakeTimeout           time.Duration `yaml:"task_queue_handshake_timeout"`
    MaxTaskQueueActivitiesPerSecond     float64       `yaml:"max_task_queue_activities_per_second"`
}

// ApplyConcurrencyOptions 应用并发选项
func (c *ConcurrencyConfig) ApplyConcurrencyOptions(opts *worker.Options) {
    if c.MaxConcurrentActivityExecutions > 0 {
        opts.MaxConcurrentActivityExecutionSize = c.MaxConcurrentActivityExecutions
    }

    if c.MaxConcurrentWorkflowExecutions > 0 {
        opts.MaxConcurrentWorkflowTaskExecutionSize = c.MaxConcurrentWorkflowExecutions
    }

    if c.MaxConcurrentLocalActivityExecutions > 0 {
        opts.MaxConcurrentLocalActivityExecutionSize = c.MaxConcurrentLocalActivityExecutions
    }

    if c.TaskQueueHandshakeTimeout > 0 {
        opts.TaskQueueHandshakeTimeout = c.TaskQueueHandshakeTimeout
    }

    if c.MaxTaskQueueActivitiesPerSecond > 0 {
        opts.MaxTaskQueueActivitiesPerSecond = c.MaxTaskQueueActivitiesPerSecond
    }
}
```

### 连接池配置

```go
// ConnectionPoolConfig 连接池配置
type ConnectionPoolConfig struct {
    MaxConnections     int           `yaml:"max_connections"`
    MinConnections     int           `yaml:"min_connections"`
    ConnectionTimeout  time.Duration `yaml:"connection_timeout"`
    IdleTimeout        time.Duration `yaml:"idle_timeout"`
    MaxLifetime        time.Duration `yaml:"max_lifetime"`
    HealthCheckPeriod  time.Duration `yaml:"health_check_period"`
}

// TemporalClientOptions Temporal 客户端选项
type TemporalClientOptions struct {
    HostPort      string
    Namespace     string
    ConnectionPool ConnectionPoolConfig
    TLS           *TLSConfig
    Headers       map[string]string
}

// CreateTemporalClient 创建 Temporal 客户端
func CreateTemporalClient(opts TemporalClientOptions) (client.Client, error) {
    clientOpts := client.Options{
        HostPort:  opts.HostPort,
        Namespace: opts.Namespace,
    }

    // 配置连接池
    if opts.ConnectionPool.MaxConnections > 0 {
        clientOpts.ConnectionPool = &client.ConnectionPoolOptions{
            MaxConnections: opts.ConnectionPool.MaxConnections,
            MinConnections: opts.ConnectionPool.MinConnections,
        }
    }

    // 配置 TLS
    if opts.TLS != nil {
        clientOpts.ConnectionOptions = client.ConnectionOptions{
            TLS: &tls.Config{
                InsecureSkipVerify: opts.TLS.InsecureSkipVerify,
                ServerName:         opts.TLS.ServerName,
            },
        }
    }

    // 配置头部
    if len(opts.Headers) > 0 {
        clientOpts.Headers = opts.Headers
    }

    return client.Dial(clientOpts)
}
```

### 缓存配置

```go
// CacheConfig 缓存配置
type CacheConfig struct {
    Enabled         bool          `yaml:"enabled"`
    Type            string        `yaml:"type"`            // "memory", "redis"
    TTL             time.Duration `yaml:"ttl"`
    MaxSize         int64         `yaml:"max_size"`
    EvictionPolicy  string        `yaml:"eviction_policy"` // "lru", "lfu", "fifo"
    RedisEndpoint   string        `yaml:"redis_endpoint"`
    RedisPassword   string        `yaml:"redis_password"`
    RedisDB         int           `yaml:"redis_db"`
}

// CacheManager 缓存管理器
type CacheManager struct {
    config CacheConfig
    cache  cache.Interface
}

func NewCacheManager(config CacheConfig) (*CacheManager, error) {
    if !config.Enabled {
        return &CacheManager{config: config}, nil
    }

    var cacheImpl cache.Interface
    var err error

    switch config.Type {
    case "memory":
        cacheImpl, err = newMemoryCache(config)
    case "redis":
        cacheImpl, err = newRedisCache(config)
    default:
        return nil, fmt.Errorf("不支持的缓存类型: %s", config.Type)
    }

    if err != nil {
        return nil, fmt.Errorf("创建缓存失败: %v", err)
    }

    return &CacheManager{
        config: config,
        cache:  cacheImpl,
    }, nil
}

func (m *CacheManager) Get(key string) (interface{}, bool) {
    if !m.config.Enabled || m.cache == nil {
        return nil, false
    }
    return m.cache.Get(key)
}

func (m *CacheManager) Set(key string, value interface{}) error {
    if !m.config.Enabled || m.cache == nil {
        return nil
    }
    return m.cache.Set(key, value, m.config.TTL)
}
```

## 📊 监控和日志

### 结构化日志配置

```go
// LoggingConfig 日志配置
type LoggingConfig struct {
    Level      string `yaml:"level"`       // "debug", "info", "warn", "error"
    Format     string `yaml:"format"`      // "json", "text"
    Output     string `yaml:"output"`      // "stdout", "stderr", "file"
    Filename   string `yaml:"filename"`    // 日志文件名
    MaxSize    int    `yaml:"max_size"`    // 最大文件大小 (MB)
    MaxBackups int    `yaml:"max_backups"` // 最大备份文件数
    MaxAge     int    `yaml:"max_age"`     // 最大保留天数
    Compress   bool   `yaml:"compress"`    // 是否压缩
}

// SetupLogger 设置日志记录器
func SetupLogger(config LoggingConfig) (log.Logger, error) {
    var writer io.Writer

    switch config.Output {
    case "stdout":
        writer = os.Stdout
    case "stderr":
        writer = os.Stderr
    case "file":
        if config.Filename == "" {
            return nil, errors.New("文件输出需要指定文件名")
        }

        // 配置日志轮转
        writer = &lumberjack.Logger{
            Filename:   config.Filename,
            MaxSize:    config.MaxSize,
            MaxBackups: config.MaxBackups,
            MaxAge:     config.MaxAge,
            Compress:   config.Compress,
        }
    default:
        return nil, fmt.Errorf("不支持的输出类型: %s", config.Output)
    }

    // 设置日志级别
    level, err := log.ParseLevel(config.Level)
    if err != nil {
        return nil, fmt.Errorf("无效的日志级别: %s", config.Level)
    }

    // 创建日志记录器
    switch config.Format {
    case "json":
        return log.NewLogger(log.NewJSONLogger(log.NewSyncWriter(writer)), level), nil
    case "text":
        return log.NewLogger(log.NewStructuredLogger(log.NewSyncWriter(writer)), level), nil
    default:
        return nil, fmt.Errorf("不支持的日志格式: %s", config.Format)
    }
}
```

### 指标收集

```go
// MetricsConfig 指标配置
type MetricsConfig struct {
    Enabled          bool   `yaml:"enabled"`
    PrometheusPort   int    `yaml:"prometheus_port"`
    MetricsPath      string `yaml:"metrics_path"`
    EnabledMetrics   []string `yaml:"enabled_metrics"`
    Labels           map[string]string `yaml:"labels"`
}

// MetricsCollector 指标收集器
type MetricsCollector struct {
    config MetricsConfig

    // Prometheus 指标
    activityDuration    *prometheus.HistogramVec
    activityTotal       *prometheus.CounterVec
    activityErrors      *prometheus.CounterVec
    workflowDuration    *prometheus.HistogramVec
    workflowTotal       *prometheus.CounterVec
    workflowErrors      *prometheus.CounterVec
    workerMemoryUsage   prometheus.Gauge
    workerGoroutines    prometheus.Gauge
}

func NewMetricsCollector(config MetricsConfig) *MetricsCollector {
    if !config.Enabled {
        return &MetricsCollector{config: config}
    }

    collector := &MetricsCollector{
        config: config,

        activityDuration: prometheus.NewHistogramVec(
            prometheus.HistogramOpts{
                Name: "n8n_activity_execution_duration_seconds",
                Help: "Duration of activity executions",
                Buckets: prometheus.DefBuckets,
            },
            []string{"activity_type", "node_type"},
        ),

        activityTotal: prometheus.NewCounterVec(
            prometheus.CounterOpts{
                Name: "n8n_activity_executions_total",
                Help: "Total number of activity executions",
            },
            []string{"activity_type", "node_type", "status"},
        ),

        activityErrors: prometheus.NewCounterVec(
            prometheus.CounterOpts{
                Name: "n8n_activity_execution_errors_total",
                Help: "Total number of activity execution errors",
            },
            []string{"activity_type", "node_type", "error_type"},
        ),

        workflowDuration: prometheus.NewHistogramVec(
            prometheus.HistogramOpts{
                Name: "n8n_workflow_execution_duration_seconds",
                Help: "Duration of workflow executions",
                Buckets: prometheus.DefBuckets,
            },
            []string{"workflow_type"},
        ),

        workflowTotal: prometheus.NewCounterVec(
            prometheus.CounterOpts{
                Name: "n8n_workflow_executions_total",
                Help: "Total number of workflow executions",
            },
            []string{"workflow_type", "status"},
        ),

        workflowErrors: prometheus.NewCounterVec(
            prometheus.CounterOpts{
                Name: "n8n_workflow_execution_errors_total",
                Help: "Total number of workflow execution errors",
            },
            []string{"workflow_type", "error_type"},
        ),

        workerMemoryUsage: prometheus.NewGauge(
            prometheus.GaugeOpts{
                Name: "n8n_worker_memory_usage_bytes",
                Help: "Worker memory usage in bytes",
            },
        ),

        workerGoroutines: prometheus.NewGauge(
            prometheus.GaugeOpts{
                Name: "n8n_worker_goroutines",
                Help: "Number of goroutines in worker",
            },
        ),
    }

    // 注册指标
    prometheus.MustRegister(collector.activityDuration)
    prometheus.MustRegister(collector.activityTotal)
    prometheus.MustRegister(collector.activityErrors)
    prometheus.MustRegister(collector.workflowDuration)
    prometheus.MustRegister(collector.workflowTotal)
    prometheus.MustRegister(collector.workflowErrors)
    prometheus.MustRegister(collector.workerMemoryUsage)
    prometheus.MustRegister(collector.workerGoroutines)

    return collector
}

func (m *MetricsCollector) StartMetricsServer() {
    if !m.config.Enabled {
        return
    }

    http.Handle(m.config.MetricsPath, promhttp.Handler())

    server := &http.Server{
        Addr:    fmt.Sprintf(":%d", m.config.PrometheusPort),
        Handler: promhttp.Handler(),
    }

    go func() {
        log.Printf("启动指标服务器，端口: %d", m.config.PrometheusPort)
        if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Printf("指标服务器错误: %v", err)
        }
    }()
}

func (m *MetricsCollector) RecordActivityExecution(activityType, nodeType string, duration time.Duration, success bool, err error) {
    if !m.config.Enabled {
        return
    }

    // 记录执行时间
    m.activityDuration.WithLabelValues(activityType, nodeType).Observe(duration.Seconds())

    // 记录执行总数
    status := "success"
    if !success {
        status = "error"
    }
    m.activityTotal.WithLabelValues(activityType, nodeType, status).Inc()

    // 记录错误
    if err != nil {
        errorType := "unknown"
        if strings.Contains(err.Error(), "timeout") {
            errorType = "timeout"
        } else if strings.Contains(err.Error(), "validation") {
            errorType = "validation"
        } else if strings.Contains(err.Error(), "network") {
            errorType = "network"
        }
        m.activityErrors.WithLabelValues(activityType, nodeType, errorType).Inc()
    }
}
```

### 健康检查

```go
// HealthConfig 健康检查配置
type HealthConfig struct {
    Enabled bool   `yaml:"enabled"`
    Port    int    `yaml:"port"`
    Path    string `yaml:"path"`
    Timeout int    `yaml:"timeout"` // 超时时间（秒）
}

// HealthChecker 健康检查器
type HealthChecker struct {
    config       HealthConfig
    temporalClient client.Client
    startTime    time.Time
    healthy      bool
    mutex        sync.RWMutex
}

func NewHealthChecker(config HealthConfig, client client.Client) *HealthChecker {
    return &HealthChecker{
        config:         config,
        temporalClient: client,
        startTime:      time.Now(),
        healthy:        true,
    }
}

func (h *HealthChecker) Start() error {
    if !h.config.Enabled {
        return nil
    }

    mux := http.NewServeMux()
    mux.HandleFunc(h.config.Path, h.healthHandler)

    // 添加就绪检查端点
    mux.HandleFunc("/ready", h.readyHandler)

    // 添加存活检查端点
    mux.HandleFunc("/live", h.liveHandler)

    server := &http.Server{
        Addr:         fmt.Sprintf(":%d", h.config.Port),
        Handler:      mux,
        ReadTimeout:  time.Duration(h.config.Timeout) * time.Second,
        WriteTimeout: time.Duration(h.config.Timeout) * time.Second,
    }

    go func() {
        log.Printf("启动健康检查服务器，端口: %d", h.config.Port)
        if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Printf("健康检查服务器错误: %v", err)
        }
    }()

    return nil
}

func (h *HealthChecker) healthHandler(w http.ResponseWriter, r *http.Request) {
    h.mutex.RLock()
    defer h.mutex.RUnlock()

    if !h.healthy {
        w.WriteHeader(http.StatusServiceUnavailable)
        w.Write([]byte("Service Unhealthy"))
        return
    }

    // 检查 Temporal 连接
    ctx, cancel := context.WithTimeout(context.Background(), time.Duration(h.config.Timeout)*time.Second)
    defer cancel()

    if _, err := h.temporalClient.WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}); err != nil {
        h.SetHealthy(false)
        w.WriteHeader(http.StatusServiceUnavailable)
        w.Write([]byte(fmt.Sprintf("Temporal Connection Error: %v", err)))
        return
    }

    w.WriteHeader(http.StatusOK)
    w.Write([]byte("OK"))
}

func (h *HealthChecker) readyHandler(w http.ResponseWriter, r *http.Request) {
    // 检查是否已经启动完成
    if time.Since(h.startTime) < time.Second*30 {
        w.WriteHeader(http.StatusServiceUnavailable)
        w.Write([]byte("Service Starting"))
        return
    }

    w.WriteHeader(http.StatusOK)
    w.Write([]byte("Ready"))
}

func (h *HealthChecker) liveHandler(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("Alive"))
}

func (h *HealthChecker) SetHealthy(healthy bool) {
    h.mutex.Lock()
    defer h.mutex.Unlock()
    h.healthy = healthy
}
```

## 🚀 部署策略

### Docker 部署

创建 `Dockerfile`:

```dockerfile
# 构建阶段
FROM golang:1.24.6-alpine AS builder

WORKDIR /app

# 安装依赖
RUN apk add --no-cache git

# 复制 go mod 文件
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 复制源代码
COPY . .

# 构建应用
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o n8n2temporal-worker ./worker

# 运行阶段
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# 从构建阶段复制二进制文件
COPY --from=builder /app/n8n2temporal-worker .
COPY --from=builder /app/config.yaml .

# 创建非 root 用户
RUN addgroup -g 1001 -S n8n && \
    adduser -u 1001 -S n8n -G n8n

USER n8n

# 暴露端口
EXPOSE 8080 9090

# 健康检查
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# 启动命令
CMD ["./n8n2temporal-worker"]
```

创建 `docker-compose.yml`:

```yaml
version: '3.8'

services:
  temporal:
    image: temporalio/auto-setup:latest
    ports:
      - "7233:7233"
      - "8233:8233"
    environment:
      - DB=postgresql
      - DB_PORT=5432
      - DB_HOST=postgres
      - DB_USER=temporal
      - DB_PASSWORD=temporal
      - DB_NAME=temporal
    depends_on:
      - postgres
    networks:
      - n8n-network

  postgres:
    image: postgres:13-alpine
    environment:
      - POSTGRES_USER=temporal
      - POSTGRES_PASSWORD=temporal
      - POSTGRES_DB=temporal
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - n8n-network

  n8n-worker:
    build: .
    ports:
      - "8080:8080"
      - "9090:9090"
    environment:
      - TEMPORAL_HOST_PORT=temporal:7233
      - LOG_LEVEL=info
      - METRICS_ENABLED=true
    depends_on:
      - temporal
    volumes:
      - ./config.yaml:/root/config.yaml
      - ./logs:/root/logs
    restart: unless-stopped
    networks:
      - n8n-network

volumes:
  postgres_data:

networks:
  n8n-network:
    driver: bridge
```

### Kubernetes 部署

创建 `k8s-deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: n8n2temporal-worker
  labels:
    app: n8n2temporal-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: n8n2temporal-worker
  template:
    metadata:
      labels:
        app: n8n2temporal-worker
    spec:
      containers:
      - name: worker
        image: n8n2temporal/worker:latest
        ports:
        - containerPort: 8080
          name: health
        - containerPort: 9090
          name: metrics
        env:
        - name: TEMPORAL_HOST_PORT
          value: "temporal:7233"
        - name: LOG_LEVEL
          value: "info"
        - name: METRICS_ENABLED
          value: "true"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /live
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: n8n2temporal-worker-service
spec:
  selector:
    app: n8n2temporal-worker
  ports:
  - name: health
    port: 8080
    targetPort: 8080
  - name: metrics
    port: 9090
    targetPort: 9090
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: worker-config
data:
  config.yaml: |
    temporal:
      host_port: "temporal:7233"
      namespace: "default"
    worker:
      task_queue: "n8n-conversion-queue"
      max_concurrent_activity_execution_size: 50
    logging:
      level: "info"
      format: "json"
    metrics:
      enabled: true
      prometheus_port: 9090
    health:
      enabled: true
      port: 8080
```

## 🔧 故障排除

### 常见问题诊断

1. **Worker 无法连接到 Temporal Server**

```bash
# 检查网络连接
telnet localhost 7233

# 检查 Temporal Server 状态
curl http://localhost:8233/api/v1

# 检查 Docker 容器状态
docker ps | grep temporal
docker logs temporal
```

2. **活动注册失败**

```bash
# 检查活动类型冲突
grep -r "RegisterActivity" .

# 检查活动方法签名
go build ./worker
```

3. **内存泄漏诊断**

```bash
# 生成内存分析报告
go tool pprof http://localhost:8080/debug/pprof/heap

# 检查 Goroutine 泄漏
go tool pprof http://localhost:8080/debug/pprof/goroutine
```

### 调试工具

```go
// DebugConfig 调试配置
type DebugConfig struct {
    Enabled       bool   `yaml:"enabled"`
    LogLevel      string `yaml:"log_level"`
    ProfileEnabled bool   `yaml:"profile_enabled"`
    ProfilePort    int    `yaml:"profile_port"`
    TraceEnabled   bool   `yaml:"trace_enabled"`
    TracePath      string `yaml:"trace_path"`
}

// EnableDebugging 启用调试功能
func EnableDebugging(config DebugConfig) error {
    if !config.Enabled {
        return nil
    }

    // 设置日志级别
    if config.LogLevel != "" {
        log.SetLevel(log.ParseLevel(config.LogLevel))
    }

    // 启用性能分析
    if config.ProfileEnabled {
        go func() {
            log.Printf("启动性能分析服务器，端口: %d", config.ProfilePort)
            log.Println(http.ListenAndServe(fmt.Sprintf(":%d", config.ProfilePort), nil))
        }()
    }

    // 启用跟踪
    if config.TraceEnabled {
        return setupTracing(config.TracePath)
    }

    return nil
}

func setupTracing(tracePath string) error {
    if tracePath == "" {
        tracePath = "./traces"
    }

    // 创建跟踪目录
    if err := os.MkdirAll(tracePath, 0755); err != nil {
        return fmt.Errorf("创建跟踪目录失败: %v", err)
    }

    // 启用 Go 跟踪
    runtime.SetBlockProfileRate(1)
    runtime.SetMutexProfileFraction(1)

    // 定期保存跟踪文件
    go func() {
        ticker := time.NewTicker(time.Minute * 5)
        defer ticker.Stop()

        for range ticker.C {
            filename := fmt.Sprintf("%s/trace-%s.out", tracePath, time.Now().Format("20060102-150405"))
            if err := writeTraceFile(filename); err != nil {
                log.Printf("保存跟踪文件失败: %v", err)
            }
        }
    }()

    return nil
}
```

---

## 🔗 相关文档

- [项目主文档](../README.md)
- [Activity 节点文档](ACTIVITY_NODES.md)
- [工作流引擎文档](WORKFLOW_ENGINE.md)
- [API 参考文档](API_REFERENCE.md)

## 📞 技术支持

如有 Worker 配置相关问题，请：

1. 查阅本文档中的配置选项
2. 检查日志文件和错误信息
3. 使用健康检查端点验证状态
4. 提交 GitHub Issue 获取支持

---

**最后更新**: 2023年10月18日
**文档版本**: v1.0.0
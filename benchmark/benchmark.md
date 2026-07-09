# 短链接服务压测说明

本目录提供短链接跳转接口的压测脚本，包括 JMeter 测试计划（`.jmx`）与 wrk Lua 脚本（`.lua`），用于验证「三级缓存 + 布隆过滤器 + 分布式锁」链路在高并发场景下的性能表现。

## 目录结构

```
benchmark/
├── jmeter_short_link.jmx   # JMeter 测试计划（500 并发 × 100 循环）
├── wrk_jump.lua            # wrk 压测脚本（随机短码 + 响应码统计）
└── benchmark.md            # 本说明文档
```

## 环境准备

### 1. 启动依赖中间件

```bash
# 启动 MySQL（默认 3306，需先执行 sql/schema.sql + sql/data.sql 初始化表结构）
# 启动 Redis（默认 6379）
# 启动 Kafka（默认 9092，可选：仅统计上报与缓存广播需要）
```

### 2. 启动短链接服务

```bash
cd d:\CCDevelop\Garlic
mvn clean package -DskipTests
java -jar short-link-api/target/short-link-api-1.0.0.jar
# 或在 IDE 中直接运行 ShortLinkApplication
```

服务默认监听 `http://localhost:8001`。

### 3. 预置 100 个短链（关键）

压测脚本默认使用 `bm000` ~ `bm099` 共 100 个短码。压测前需通过**批量创建接口**预置这 100 条短链，否则跳转接口会因布隆过滤器拦截返回 404，无法测出真实缓存性能。

使用 cURL 调用批量创建接口（一次最多 100 条）：

```bash
# 1. 生成包含 100 个 URL 的请求体（URL 后缀对应短码编号，便于对应）
#    注意：实际生成的短码由 ShortCodeGenerator 决定，不一定为 bm000~bm099
#    压测脚本中的 codes 数组需替换为真实生成的短码

curl -X POST http://localhost:8001/api/short-link/batch/create \
  -H "Content-Type: application/json" \
  -d '{"urls":["https://www.example.com/0","https://www.example.com/1",...,"https://www.example.com/99"],"groupId":1,"describe":"压测预置"}'

# 2. 将响应中的 results[].shortCode 提取出来，替换 jmeter_short_link.jmx 与 wrk_jump.lua 中的 codes 数组
```

> **简化方案**：若仅需验证压测链路可用性，可直接修改脚本中的 `codes` 为已存在的任意短码（如 `["abc123"]`），但此时 QPS 指标无意义（缓存命中率会因单 key 过热失真）。

## JMeter 使用方式

### GUI 模式（调试用，不建议用于正式压测）

```bash
# 启动 JMeter GUI
jmeter -t d:\CCDevelop\Garlic\benchmark\jmeter_short_link.jmx

# 在 GUI 中点击 ▶ 启动测试，通过「聚合报告」与「查看结果树」观察结果
```

### CLI 模式（正式压测，推荐）

```bash
# 无 GUI 运行，结果输出到 jtl 文件
jmeter -n -t d:\CCDevelop\Garlic\benchmark\jmeter_short_link.jmx \
       -l d:\CCDevelop\Garlic\benchmark\result.jtl \
       -e -o d:\CCDevelop\Garlic\benchmark\report

# 参数说明：
#   -n  非 GUI 模式
#   -t  指定测试计划文件
#   -l  指定结果文件（jtl）
#   -e  测试结束后生成 HTML 报告
#   -o  指定 HTML 报告输出目录（必须为空目录）
```

执行完毕后，打开 `benchmark/report/index.html` 查看可视化报告，重点关注：
- **Statistics** → Throughput（QPS）
- **Response Times Over Time** → P99 折线
- **Response Time Percentiles** → P50/P90/P99/P99.9 分布

### 测试计划参数

| 参数 | 值 | 说明 |
|------|-----|------|
| 线程数 | 500 | 并发用户数 |
| Ramp-Up | 10 秒 | 10 秒内启动全部 500 线程 |
| 循环次数 | 100 | 每线程循环 100 次，总请求数 = 500 × 100 = 50000 |
| HTTP 方法 | GET | 跳转接口 |
| 请求路径 | `/${shortCode}` | shortCode 由 JSR223 PreProcessor 随机生成 |
| 跟随重定向 | 否 | 不跟随 302，便于统计原始响应码 |

## wrk 使用方式

### 基础压测命令

```bash
# 8 线程 × 500 连接 × 30 秒，打印延迟分布
wrk -t8 -c500 -d30s -s d:\CCDevelop\Garlic\benchmark\wrk_jump.lua \
    --latency http://localhost:8001
```

### 进阶参数

```bash
# 提升连接数到 1000，持续 60 秒
wrk -t16 -c1000 -d60s -s d:\CCDevelop\Garlic\benchmark\wrk_jump.lua \
    --latency http://localhost:8001

# 设置超时（默认 2 秒，压测建议 5 秒避免误判）
wrk -t8 -c500 -d30s -T5s -s d:\CCDevelop\Garlic\benchmark\wrk_jump.lua \
    --latency http://localhost:8001
```

### 输出示例

```
Running 30s test @ http://localhost:8001
  8 threads and 500 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     3.25ms    2.10ms  45.20ms   89.30%
    Req/Sec     6.25k     0.85k    8.50k     75.20%
  Latency Distribution
     50%    2.80ms
     75%    4.10ms
     90%    5.80ms
     99%   18.50ms
  1500124 requests in 30.00s, 0.25GB read
Requests/sec:  50004.13
Transfer/sec:      8.33MB

========== 响应码分布 ==========
200 (OK)            : 0
302 (Redirect)      : 1500124
404 (Not Found)     : 0
410 (Expired)       : 0
429 (Too Many Reqs) : 0
500 (Server Error)  : 0
other               : 0
```

## 预期指标

跳转接口是高频读场景，三级缓存（Caffeine + Redis + DB）的设计目标是：

| 指标 | 目标值 | 说明 |
|------|--------|------|
| **QPS** | ≥ 5000 | 单机吞吐量（8C16G 参考） |
| **P99 延迟** | < 20ms | 99% 请求在 20ms 内返回 |
| **缓存命中率** | ≥ 95% | L1 Caffeine + L2 Redis 命中率合计 |
| **错误率** | 0% | 无 5xx / 429 错误 |

### 指标不达标排查思路

- **QPS 不达标**：
  - 检查 Tomcat 线程池（`server.tomcat.max-threads`，默认 200，压测建议调到 500+）
  - 检查 Redis 连接池（`lettuce.pool.max-active`，默认 16）
  - 检查是否触发了 Sentinel 限流（`@RateLimit(capacity=5000)`）
- **P99 过高**：
  - 检查缓存命中率，若命中率低则排查布隆过滤器误判或缓存过期
  - 检查是否大量请求打到 DB（缓存击穿），看分布式锁是否生效
  - 查看 GC 日志（`-Xlog:gc*`），长 GC 会导致 P99 飙升
- **缓存命中率低**：
  - 确认预置的 100 个短码已被访问过（首次访问会回填缓存）
  - 检查 Caffeine 容量配置（`shortLinkLocalCache` Bean 的 maximumSize）
  - 检查 Redis 是否有大量 key 过期（TTL 抖动 0~300s）

## 监控指标查看

服务通过 Spring Boot Actuator + Micrometer 暴露 Prometheus 格式指标。

### 1. 健康检查

```bash
curl http://localhost:8001/actuator/health
# 期望返回：{"status":"UP",...}
```

### 2. Prometheus 指标

```bash
# 查看全部指标
curl http://localhost:8001/actuator/prometheus

# 过滤跳转接口相关指标
curl http://localhost:8001/actuator/prometheus | grep -E "jump|short_link"

# 关注指标：
#   http_server_requests_seconds_count{uri="/{code}"}   跳转接口请求总数
#   http_server_requests_seconds_max{uri="/{code}"}    跳转接口最大响应时间
#   jvm_memory_used_bytes{area="heap"}                 JVM 堆内存使用
#   jvm_threads_states_threads                        JVM 线程状态分布
#   lettuce_command_completion_seconds_count           Redis 命令耗时
```

### 3. 接入 Prometheus + Grafana（可选）

在 `prometheus.yml` 中添加抓取配置：

```yaml
scrape_configs:
  - job_name: 'short-link-service'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['localhost:8001']
```

启动 Grafana 后导入 JVM (Micrometer) Dashboard（ID: 4701），可可视化观察：
- 堆内存 / 非堆内存使用趋势
- GC 次数与耗时
- 线程状态分布
- HTTP 请求 QPS / P99 / 错误率

## 注意事项

1. **压测环境隔离**：禁止在生产环境运行压测脚本，会产生大量真实跳转请求。
2. **预热**：正式压测前先以低并发（如 50 并发）运行 30 秒预热 Caffeine 缓存，避免冷启动影响 P99。
3. **短码替换**：脚本中的 `bm000~bm099` 为占位短码，务必替换为真实预置的短码，否则全部返回 404。
4. **客户端瓶颈**：wrk 单机通常可压到 5~10 万 QPS；若目标 QPS 更高，需多台压测机分布式压测。
5. **网络环境**：压测客户端与服务端网络延迟应 < 1ms（同机房或本机），否则 P99 指标无参考价值。

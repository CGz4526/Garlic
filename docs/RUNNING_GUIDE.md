# Garlic 短链接服务 — 运行指南

> 本文档面向需要本地启动、调试、系统测试本项目的人。包含：环境准备 → 启动步骤 → 7 个 Maven 模块职责与所用技术详解 → 10 个系统测试样例（含 cURL 命令、预期结果、验证点）。

---

## 一、环境准备

### 1.1 依赖中间件

| 中间件 | 版本 | 默认端口 | 本项目用途 |
|--------|------|----------|-----------|
| JDK | 17+ | - | 运行时 |
| Maven | 3.6+ | - | 构建 |
| MySQL | 8.0 | 3306 | 持久化（4 库分片） |
| Redis | 6+ | 6379 | L2 缓存 / 分布式锁 / 限流 / HyperLogLog |
| Kafka | 3.x | 9092 | 访问记录削峰、缓存失效广播 |

> 本项目通过环境变量 `MYSQL_PASSWORD`、`REDIS_PASSWORD` 注入数据库与缓存凭证（见 `application.yml` / `application-sharding.yml` 中的 `${MYSQL_PASSWORD:}` / `${REDIS_PASSWORD:}` 占位符）。启动前请通过系统环境变量或 IDE 运行配置设置这两个值。

### 1.2 初始化数据库

```bash
# 创建 4 个库 link_db_0~3，每库含短链表/统计表分表 + 访问明细表 + 广播表
mysql -uroot -p < sql/schema.sql
# 写入测试数据（用户、分组、示例短链）
mysql -uroot -p < sql/data.sql
```

### 1.3 创建 Kafka Topic

```bash
# 访问记录上报 topic（8 分区，消费者并发聚合）
kafka-topics.sh --create --topic short-link-access-record \
  --bootstrap-server 127.0.0.1:9092 --partitions 8 --replication-factor 1

# 缓存失效广播 topic（4 分区，多实例 L1 一致性）
kafka-topics.sh --create --topic short-link-cache-invalid \
  --bootstrap-server 127.0.0.1:9092 --partitions 4 --replication-factor 1
```

> 若不创建 Topic，应用启动时消费者会报 `UNKNOWN_TOPIC_OR_PARTITION` 警告，但不影响应用启动；首次发送消息时 Kafka（默认 `auto-create-topics-enable=true`）会自动创建。

---

## 二、启动步骤

### 2.1 构建

```bash
cd d:\CCDevelop\Garlic
mvn clean package -DskipTests
```

构建产物：`short-link-api\target\short-link-api-1.0.0.jar`（可执行 fat jar，含主清单属性）。

### 2.2 启动后端

**方式一：直接 java -jar（推荐）**

```bash
java -jar short-link-api\target\short-link-api-1.0.0.jar
```

**方式二：带 JVM 调优参数（生产/压测）**

```bash
java -server -Xms2g -Xmx2g -Xmn1g ^
  -XX:+UseG1GC -XX:MaxGCPauseMillis=100 ^
  -Xlog:gc*:file=logs/gc.log:time,uptime,level,tags:filecount=5,filesize=10m ^
  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=logs/heapdump.hprof ^
  -jar short-link-api\target\short-link-api-1.0.0.jar
```

**方式三：Maven 直接运行（开发期）**

```bash
cd short-link-api
mvn spring-boot:run
```

**方式四：脚本启动**

```bash
# Windows
bin\startup.bat
# Linux / macOS
bin/startup.sh
```

### 2.3 启动成功标志

日志出现以下两行即代表启动成功（约 12 秒）：

```
Started ShortLinkApplication in 11.632 seconds (JVM running for 12.464)
```

启动过程中会依次完成：
1. 4 个 HikariPool 连接池初始化（ds_0~3）
2. Redisson 连接 Redis（24 connections initialized）
3. 布隆过滤器加载/预热（从 DB 加载已有短码）
4. 线程池创建（batch-gen / stats-async / cacheRefresh / bloom）
5. Kafka 消费者加入消费组（short-link-consumer / short-link-cache-invalid-group）

### 2.4 启动前端（可选）

前端为纯静态页面，位于 `frontend/` 目录，任意静态服务器均可：

```bash
# 使用 Python 内置 HTTP 服务器
cd frontend
python -m http.server 8081
```

浏览器访问 http://localhost:8081/index.html

> 前端 `app.js` 中 `USE_MOCK = true` 使用内置 Mock 数据；改为 `false` 则对接真实后端（`API_BASE_URL = 'http://localhost:8001'`）。

### 2.5 运行地址汇总

| 服务 | 地址 |
|------|------|
| 后端 API | http://localhost:8001 |
| 前端页面 | http://localhost:8081/index.html |
| 健康检查 | http://localhost:8001/actuator/health |
| Prometheus 指标 | http://localhost:8001/actuator/prometheus |
| 线程转储 | http://localhost:8001/actuator/threaddump |

### 2.6 已知启动问题与解决

| 问题 | 原因 | 解决 |
|------|------|------|
| `没有主清单属性` | short-link-api 未配置 spring-boot-maven-plugin repackage | 已在 `short-link-api/pom.xml` 配置，重新 `mvn package` |
| `INTERVAL_RANGE SPI not found` | ShardingSphere 5.2.1 无此 SPI | `application-sharding.yml` 改用 `type: INTERVAL` |
| `setCodePointLimit NoSuchMethodError` | SpringBoot 2.7.18 自带 snakeyaml 1.30，ShardingSphere 需 1.32+ | 父 `pom.xml` 加 `<snakeyaml.version>1.33</snakeyaml.version>` |
| `NOAUTH Authentication required` | Redis 需密码 | `application.yml` 配 `spring.redis.password` |
| Sentinel `ExceptionInInitializerError` | 沙箱禁止写 `*.lck` 日志文件 | `SentinelConfig.initDegradeRules()` 已 try-catch 容错，不阻塞启动 |

---

## 三、模块说明与技术详解

本项目为 Maven 多模块工程，共 7 个模块，依赖关系：

```
short-link-api (主应用)
  ├── short-link-common (公共组件)
  ├── short-link-service (业务通用服务)
  ├── short-link-stat (统计扩展，占位)
  └── short-link-dao (数据访问)
       └── short-link-common
short-link-mq (MQ 扩展，占位)
short-link-admin (管理后台，占位)
```

### 3.1 short-link-common（公共组件）

**职责**：存放所有模块共享的基础设施代码，是整个项目的"工具箱"。不依赖业务模块，被 dao / api 等模块引用。

**技术清单**：

| 子包 | 技术实现 | 关键类 |
|------|---------|--------|
| `cache` | Caffeine 3.1.8（L1 本地缓存）+ 热点探测 | `TwoLevelCache`、`HotKeyDetector` |
| `bloom` | Redisson 布隆过滤器（防缓存穿透） | `BloomFilterService`、`ScalableBloomFilter` |
| `ratelimit` | Redis + Lua 令牌桶、ZSet 滑动窗口、AOP 切面、SpEL key 解析 | `TokenBucketRateLimiter`、`SlidingWindowRateLimiter`、`RateLimitAspect` |
| `config` | Caffeine/Redis/Kafka/Sentinel/Snowflake/线程池配置 | `CaffeineConfig`、`RedisConfig`、`SentinelConfig`、`ThreadPoolConfig` |
| `sentinel` | Sentinel 1.8.6 规则持久化（SPI） | `SentinelRulePersistence` |
| `util` | Base62 编码、雪花 ID、IP/UserAgent 解析、URL 校验 | `Base62Utils`、`SnowflakeIdWorker`、`IpUtils` |
| `exception` | 全局异常处理 + 业务错误码 | `GlobalExceptionHandler`、`ErrorCode` |
| `result` | 统一响应封装 | `Result<T>`、`PageResponse<T>` |
| `validation` | 自定义 URL 校验注解 | `@Url`、`UrlValidator` |

**核心技术点**：

- **二级缓存 `TwoLevelCache`**：L1 Caffeine → L2 Redis → DB 三级查询链路。缓存击穿用 Redisson 分布式锁 + 双重检查；缓存雪崩用 TTL 随机抖动（基准 3600s + 0~300s 随机）；缓存穿透由调用方处理（布隆过滤器）。序列化用 Hutool JSONUtil。
- **限流 `RateLimitAspect`**：`@RateLimit` 注解 + AOP 环绕通知。`INTERFACE` 类型用令牌桶（Redis Lua），`USER`/`IP` 类型用滑动窗口（Redis ZSet）。支持 SpEL 表达式解析限流 key（如 `#req.userId`）。
- **线程池 `ThreadPoolConfig`**：4 个隔离线程池（batch-gen 8/16、stats-async 4/8、cacheRefresh 2/4、bloom 1/2），每个线程池注册 Micrometer 指标，拒绝任务计数。

### 3.2 short-link-dao（数据访问层）

**职责**：定义 5 个数据库实体（DO）与 5 个 MyBatis-Plus Mapper，屏蔽底层分库分表细节。

**技术清单**：

| 组件 | 技术 | 说明 |
|------|------|------|
| ORM | MyBatis-Plus 3.5.5 | 通用 CRUD、条件构造器、分页插件 |
| 数据源 | ShardingSphere-JDBC 5.2.1 | 分库分表中间件，对上层透明 |
| 连接池 | HikariCP | SpringBoot 默认，4 个 ds 各一个池 |
| 实体 | 5 个 DO | `ShortLinkDO`、`LinkAccessRecordDO`、`LinkAccessStatsDO`、`UserDO`、`GroupDO` |

**分片策略**（在 `application-sharding.yml` 配置）：

| 表 | 分片键 | 分库算法 | 分表算法 |
|----|--------|---------|---------|
| `t_short_link` | short_code | 哈希取模 4（ds_0~3） | 哈希取模 4（_0~3），共 16 分片 |
| `t_link_access_stats` | short_code | 同上（绑定表） | 哈希取模 4 |
| `t_link_access_record` | access_time | 不分库（落 ds_0） | INTERVAL 按月份分表（_202601~_202612） |
| `t_user` / `t_group` | - | 广播表（每库一份相同数据） | - |

主键生成：雪花算法（SNOWFLAKE），`worker-id=1, datacenter-id=1`。

### 3.3 short-link-service（业务通用服务）

**职责**：存放与具体 Controller 无关的通用业务逻辑。当前核心是短码生成器。

**技术清单**：

| 类 | 技术 | 说明 |
|----|------|------|
| `ShortCodeGenerator` | 雪花 ID + Base62 | 雪花算法生成 64bit ID → Base62 编码为 6~7 位短码 |

**短码生成流程**：`SnowflakeIdWorker.nextId()` → 64bit long → `Base62Utils.encode()` → 6~7 位字符串。Base62 字符集 `[0-9A-Za-z]`，理论容量 62^6 ≈ 568 亿。

### 3.4 short-link-mq（MQ 扩展，占位）

**职责**：预留模块，用于未来拆分独立的 MQ 生产/消费组件。当前为空，仅含 `package-info.java`。

### 3.5 short-link-stat（统计扩展，占位）

**职责**：预留模块，用于未来拆分独立的统计服务（如离线聚合、报表导出）。当前为空。

### 3.6 short-link-admin（管理后台，占位）

**职责**：预留模块，用于未来拆分独立的管理后台应用。当前为空。

### 3.7 short-link-api（主应用入口）

**职责**：SpringBoot 主启动模块，聚合所有业务能力：5 个 Controller、5 个 Service、缓存一致性、Kafka 生产消费、统计聚合。

**技术清单**：

| 子包 | 技术实现 | 关键类 |
|------|---------|--------|
| `controller` | Spring MVC REST | `ShortLinkCreateController`、`ShortLinkJumpController`、`ShortLinkManageController`、`ShortLinkBatchController`、`StatsController` |
| `service.shortlink` | 三级缓存、幂等、CompletableFuture | `ShortLinkCreateService`、`ShortLinkJumpService`、`ShortLinkManageService`、`ShortLinkBatchService`、`ShortLinkTwoLevelCache` |
| `service.cache` | 延迟双删 + Kafka 广播 | `CacheConsistencyService`、`CacheInvalidListener` |
| `service.mq` | 本地队列 + Kafka 生产者 | `AccessRecordQueue`（10000 容量）、`KafkaAccessRecordProducer` |
| `service.stats` | Kafka 消费 + 内存聚合 + 定时落库 | `AccessRecordConsumer`、`StatsAggregator`、`StatsFlushTask` |
| `service.ratelimit` | IP 黑名单动态封禁 | `IpBlacklistService` |
| `runner` | 启动后预热布隆过滤器 | `BloomFilterWarmUpRunner` |
| `dto` | 请求/响应 DTO + JSR-303 校验 | `ShortLinkCreateReqDTO` 等 |

**核心技术点**：

- **缓存一致性 `CacheConsistencyService`**：更新短链时执行"延迟双删"（先删缓存 → 更新 DB → 延迟 500ms 再删一次），并通过 Kafka 广播失效消息（`short-link-cache-invalid` topic），其他实例的 `CacheInvalidListener` 收到后清理本地 Caffeine，用 `instanceId` 去重避免重复处理。
- **异步统计链路**：跳转请求 → `AccessRecordQueue`（本地 BlockingQueue，容量 10000，削峰）→ `KafkaAccessRecordProducer`（statsExecutor 线程异步发送 Kafka）→ `AccessRecordConsumer`（`@KafkaListener` 消费）→ `StatsAggregator`（两级 ConcurrentHashMap 内存聚合，UV 用 Redis HyperLogLog 去重，IP 用 Redis Set 去重）→ `StatsFlushTask`（`@Scheduled` 每 60s 批量落库 `t_link_access_stats`）。
- **幂等创建**：`ShortLinkCreateService` 用 Redis SETNX + Redisson 分布式锁保证同一 URL 不会重复创建短链。
- **监控**：Actuator 暴露 health/metrics/threaddump/prometheus 端点，线程池指标通过 Micrometer 上报 Prometheus。

---

## 四、系统测试样例

以下 10 个测试样例覆盖完整业务链路：创建 → 跳转 → 统计 → 管理 → 限流 → 容错。

> 测试前请确保后端已启动（http://localhost:8001），且已执行 `sql/data.sql` 初始化数据。cURL 命令在 PowerShell 中需用 `Invoke-RestMethod` 替代或使用 Git Bash 执行。

### 样例 1：创建短链（正向）

**目的**：验证短链生成核心链路（雪花 ID → Base62 → 分库分表写入 → 布隆过滤器加入 → 缓存预填）。

```bash
curl -X POST http://localhost:8001/api/short-link/create \
  -H "Content-Type: application/json" \
  -d '{
    "originalUrl": "https://github.com",
    "describe": "测试短链-GitHub"
  }'
```

**预期响应**：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "shortCode": "aB3xK9",
    "originalUrl": "https://github.com"
  },
  "requestId": "..."
}
```

**验证点**：
- `shortCode` 为 6~7 位字母数字
- 后端日志可见 ShardingSphere 实际路由 SQL：`Actual SQL: ds_X ::: INSERT INTO t_short_link_Y ...`，X 为 0~3 的库，Y 为 0~3 的表
- Redis 中 `short:link:{shortCode}` 已预填缓存
- 布隆过滤器 `short_link:bloom` 已加入该 shortCode

### 样例 2：创建短链（参数校验失败）

**目的**：验证 JSR-303 校验 + 全局异常处理。

```bash
curl -X POST http://localhost:8001/api/short-link/create \
  -H "Content-Type: application/json" \
  -d '{"originalUrl": "not-a-valid-url"}'
```

**预期响应**：

```json
{
  "code": 400,
  "message": "URL 不合法",
  "data": null
}
```

**验证点**：自定义 `@Url` 校验注解生效，`GlobalExceptionHandler` 捕获 `MethodArgumentNotValidException`。

### 样例 3：短链跳转（302 重定向）

**目的**：验证三级缓存查询链路 + 302 跳转 + 异步统计上报。

```bash
# 假设样例 1 生成的短码为 aB3xK9
curl -i http://localhost:8001/aB3xK9
```

**预期响应**：

```
HTTP/1.1 302
Location: https://github.com
```

**验证点**：
- 返回 302 状态码，`Location` 头指向原始 URL
- 后端日志：首次 `L2 Redis 命中` 或 `缓存未命中 → 查 DB`
- 再次请求同一短码，日志出现 `L1 Caffeine 命中`（热点 key 回填后）
- 异步链路：`AccessRecordQueue` 入队 → Kafka 生产者发送 → 消费者聚合 → 日志可见 `聚合访问记录：shortCode=...`

### 样例 4：跳转不存在的短码（布隆过滤器拦截）

**目的**：验证布隆过滤器防缓存穿透。

```bash
curl -i http://localhost:8001/notExist
```

**预期响应**：

```json
{
  "code": 404,
  "message": "短链不存在",
  "data": null
}
```

**验证点**：
- 布隆过滤器判定不存在，直接返回 404，不查 Redis 与 DB
- 后端日志：`布隆过滤器判定不存在：shortCode=notExist`

### 样例 5：分页查询短链列表

**目的**：验证 ShardingSphere 广播查询 + MyBatis-Plus 分页。

```bash
curl "http://localhost:8001/api/short-link/page?userId=1&current=1&size=10"
```

**预期响应**：

```json
{
  "code": 0,
  "data": {
    "records": [
      {"shortCode": "aB3xK9", "originalUrl": "https://github.com", ...}
    ],
    "total": 1,
    "current": 1,
    "size": 10
  }
}
```

**验证点**：
- 无分片键查询，ShardingSphere 走广播：`Actual SQL: ds_0 ::: ... UNION ALL ds_1 ::: ... UNION ALL ds_2 ::: ... UNION ALL ds_3 ::: ...`
- 分页参数正确返回

### 样例 6：更新短链（触发缓存一致性）

**目的**：验证延迟双删 + Kafka 广播失效。

```bash
curl -X PUT http://localhost:8001/api/short-link/update \
  -H "Content-Type: application/json" \
  -d '{
    "shortCode": "aB3xK9",
    "describe": "更新后的描述"
  }'
```

**预期响应**：

```json
{"code": 0, "message": "success", "data": null}
```

**验证点**：
- 后端日志：`缓存延迟双删：第一次删除 → 更新DB → 延迟500ms → 第二次删除`
- Kafka topic `short-link-cache-invalid` 收到失效消息
- 若启动多实例，其他实例日志可见 `CacheInvalidListener 收到失效消息，清理本地缓存`

### 样例 7：统计总览查询

**目的**：验证统计聚合落库后的查询。

```bash
# 先多次访问短链产生统计数据
for i in $(seq 1 20); do curl -s -o /dev/null http://localhost:8001/aB3xK9; done

# 等待 60s 让 StatsFlushTask 落库（或调小定时任务间隔）

curl http://localhost:8001/api/stats/aB3xK9/overview
```

**预期响应**：

```json
{
  "code": 0,
  "data": {
    "pv": 20,
    "uv": 1,
    "ipCount": 1
  }
}
```

**验证点**：
- PV = 访问次数，UV = Redis HyperLogLog 去重后的用户数，IP 数 = Redis Set 去重后的 IP 数
- 查询走单分片（带 short_code 分片键）：`Actual SQL: ds_X ::: SELECT ... WHERE short_code = ?`

### 样例 8：批量创建短链

**目的**：验证 CompletableFuture 多线程编排 + 线程池隔离。

```bash
curl -X POST http://localhost:8001/api/short-link/batch-create \
  -H "Content-Type: application/json" \
  -d '{
    "urls": [
      "https://www.baidu.com",
      "https://www.taobao.com",
      "https://www.jd.com",
      "https://www.bilibili.com",
      "https://www.zhihu.com"
    ],
    "describe": "批量测试"
  }'
```

**预期响应**：

```json
{
  "code": 0,
  "data": {
    "successCount": 5,
    "failCount": 0,
    "results": [
      {"originalUrl": "https://www.baidu.com", "shortCode": "xK9aB3", "success": true},
      ...
    ]
  }
}
```

**验证点**：
- 5 条短链并发生成，耗时接近单条而非 5 倍
- 后端日志可见 `batch-gen-N` 线程并发执行
- 超过 100 条 URL 返回校验错误：`单批最多 100 条 URL`

### 样例 9：限流触发（用户级限流）

**目的**：验证 `@RateLimit` 用户级滑动窗口限流（创建接口 50 次/秒）。

```bash
# 用循环快速发起 60 次创建请求（同一 userId）
for i in $(seq 1 60); do
  curl -s -X POST http://localhost:8001/api/short-link/create \
    -H "Content-Type: application/json" \
    -d "{\"originalUrl\":\"https://example.com/$i\"}" &
done
wait
```

**预期结果**：
- 前 50 次返回成功
- 后 10 次返回：

```json
{"code": 429, "message": "生成过于频繁，请稍后再试", "data": null}
```

**验证点**：
- `SlidingWindowRateLimiter` 基于 Redis ZSet，1 秒窗口内超过 50 次拒绝
- 后端日志：`限流触发：type=USER, key=1, message=生成过于频繁...`

### 样例 10：健康检查与监控指标

**目的**：验证 Actuator 监控端点可用性。

```bash
# 1. 健康检查（含 MySQL / Redis 详情）
curl http://localhost:8001/actuator/health

# 2. Prometheus 指标
curl http://localhost:8001/actuator/prometheus | grep -E "http_server_requests|jvm_memory|jvm_threads"
```

**预期 health 响应**：

```json
{
  "status": "UP",
  "components": {
    "db": {"status": "UP", "details": {"database": "MySQL", "validationQuery": "isValid()"}},
    "redis": {"status": "UP", "details": {"version": "8.6.3"}},
    "diskSpace": {"status": "UP"}
  }
}
```

**验证点**：
- `db`、`redis`、`diskSpace`、`ping` 四个组件全部 UP
- Prometheus 指标含 `http_server_requests_seconds_count`（HTTP 请求计数）、`jvm_memory_used_bytes`（JVM 内存）、`jvm_threads_states_threads`（线程状态）

---

## 五、测试链路验证顺序建议

为保证测试顺利，建议按以下顺序执行：

1. **样例 10**（健康检查）→ 确认服务与中间件连通
2. **样例 1**（创建短链）→ 记录返回的 `shortCode`
3. **样例 3**（跳转）→ 用样例 1 的 shortCode，多次访问产生统计数据
4. **样例 5**（分页查询）→ 验证列表
5. **样例 6**（更新）→ 验证缓存一致性
6. **样例 7**（统计）→ 等待 60s 落库后查询
7. **样例 8**（批量创建）→ 验证并发
8. **样例 2、4**（异常场景）→ 验证容错
9. **样例 9**（限流）→ 验证高并发防护

---

## 六、附：API 接口速查表

| 接口 | 方法 | 路径 | 限流 |
|------|------|------|------|
| 创建短链 | POST | `/api/short-link/create` | 用户级 50 次/秒 |
| 批量创建 | POST | `/api/short-link/batch-create` | - |
| 分页查询 | GET | `/api/short-link/page` | - |
| 短链详情 | GET | `/api/short-link/{code}` | - |
| 更新短链 | PUT | `/api/short-link/update` | - |
| 删除短链 | DELETE | `/api/short-link/{code}` | - |
| 短链跳转 | GET | `/{code}` | 接口级令牌桶 5000 QPS + IP 黑名单 |
| 统计总览 | GET | `/api/stats/{code}/overview` | - |
| 访问趋势 | GET | `/api/stats/{code}/trend` | - |
| 维度分布 | GET | `/api/stats/{code}/distribution` | - |
| 小时分布 | GET | `/api/stats/{code}/hour` | - |

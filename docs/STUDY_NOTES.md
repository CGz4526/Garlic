# Garlic 短链接服务 — 项目学习笔记

> 本笔记记录在开发 Garlic 短链接服务过程中的核心知识点、设计权衡、踩坑经验与面试要点提炼，便于复习与回顾。

---

## 一、项目整体认知

### 1.1 一句话定位

一个面向 Java 后端面试的高并发短链接服务，用 4 个技术亮点串起缓存、分库分表、高并发防护、异步削峰四大后端核心能力。

### 1.2 核心业务链路（必须背下来）

```
创建短链：POST /api/short-link/create
  → 雪花 ID → Base62 短码 → 幂等校验（SETNX+锁）→ 分库分表写入 → 布隆过滤器加入 → 缓存预填

跳转短链：GET /{code}
  → 布隆过滤器（防穿透）→ L1 Caffeine → L2 Redis → DB（分布式锁防击穿）
  → 302 重定向 → 异步上报访问记录到 Kafka

统计聚合：跳转 → 本地队列(10000) → Kafka → 消费者内存聚合 → 60s 定时落库
```

### 1.3 7 个 Maven 模块的依赖关系

```
short-link-api（主应用，聚合所有业务）
  ├── short-link-common（公共组件：缓存/限流/布隆/线程池/工具）
  ├── short-link-service（短码生成器）
  ├── short-link-stat（占位）
  └── short-link-dao（5 DO + 5 Mapper）
       └── short-link-common
short-link-mq（占位）
short-link-admin（占位）
```

**记忆口诀**：common 是地基，dao 是数据，service 是工具，api 是聚合。

---

## 二、亮点 1：三级缓存架构 + 缓存一致性

### 2.1 为什么需要三级缓存

| 层级 | 技术 | 命中 RT | 容量 | 一致性 |
|------|------|---------|------|--------|
| L1 | Caffeine（JVM 内） | ~0.05ms | 1 万条（受限） | 弱（多实例不一致） |
| L2 | Redis | ~1ms | 大 | 强（单点） |
| L3 | MySQL | ~10ms | 海量 | 强 |

**核心权衡**：纯 Redis 在热点短链下网络往返 + 反序列化是瓶颈；纯 Caffeine 多实例不一致且内存受限。三级缓存让热点 key 走 L1（极速），冷数据走 L2（大容量），未命中走 L3（兜底）。

### 2.2 三大缓存问题的解决方案

| 问题 | 场景 | 方案 | 代码位置 |
|------|------|------|---------|
| **穿透**（查不存在的 key） | 恶意请求不存在的短码 | 布隆过滤器前置校验，false 直接拒绝 | [TwoLevelCache](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/shortlink/ShortLinkJumpService.java) jump 方法 |
| **击穿**（热点 key 过期瞬间） | 大促短链缓存失效 | Redisson 分布式锁 + 双重检查 | [TwoLevelCache.getWithLock](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/cache/TwoLevelCache.java#L167-L224) |
| **雪崩**（大量 key 同时过期） | 缓存批量失效 | TTL 随机抖动 3600+random(0,300)s | [TwoLevelCache.computeTtlWithJitter](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/cache/TwoLevelCache.java#L281) |

### 2.3 热点 key 探测（HotKeyDetector）

**为什么不全部缓存到 Caffeine？** 内存会爆。只缓存真正高频的 key。

**判定算法**：滑动窗口 + LongAdder 加权
- 两个窗口：当前窗口 + 上一窗口，每秒滚动
- 加权公式：`当前窗口计数 + 上一窗口计数 × 剩余时间比例`
- 阈值：100 次/秒

**为什么用 LongAdder 不用 AtomicLong？** 高并发下 AtomicLong 的 CAS 自旋开销大（多核竞争同一内存地址），LongAdder 分段累加 + 合并，写性能远高。

### 2.4 多实例 L1 缓存一致性

**方案**：延迟双删 + Kafka 广播 + instanceId 去重

```
更新短链流程：
1. 删除 Redis 缓存（第一次删）
2. 更新 MySQL
3. 延迟 500ms 后再删 Redis（第二次删，覆盖主从复制延迟窗口）
4. 发送 Kafka 消息到 short-link-cache-invalid topic
5. 其他实例的 CacheInvalidListener 收到消息，清理本地 Caffeine
6. 消息携带 sourceInstanceId，消费者跳过自己发的消息（去重）
```

**为什么是 500ms？** MySQL 主从复制延迟通常 100~500ms。太短覆盖不到，太长 DB 压力大。

**兜底**：即使 Kafka 广播失败，Caffeine 60s TTL 也会自然过期，达到最终一致。

### 2.5 学习要点

- 缓存一致性的本质是「读旧值」问题，没有完美方案，只有适合场景的权衡
- 延迟双删是「最终一致」方案，不是强一致；强一致需读主库或分布式事务
- 布隆过滤器特性：false 一定不存在，true 可能存在（误判）；不支持删除
- 分布式锁的双重检查：拿到锁后再次查 Redis，可能已被其他请求重建

---

## 三、亮点 2：ShardingSphere 分库分表

### 3.1 分片键选择的权衡

**核心问题**：按 short_code 还是 user_id 分片？

| 分片键 | 跳转查询（90%+ 流量） | 用户列表查询 | 选择 |
|--------|---------------------|-------------|------|
| short_code | 单分片精确路由（快） | 广播 16 分片（慢） | ✅ 选这个 |
| user_id | 广播 16 分片（慢） | 单分片精确路由（快） | ❌ |

**结论**：按高频查询的维度分片。短链服务跳转占 90%+，必须单分片路由。

### 3.2 分片策略全景

| 表 | 分片键 | 分库 | 分表 | 总分片 |
|----|--------|------|------|--------|
| t_short_link | short_code | 哈希取模 4 | 哈希取模 4 | 16 |
| t_link_access_stats | short_code | 同上（绑定表） | 哈希取模 4 | 16 |
| t_link_access_record | access_time | 不分库 | INTERVAL 按月份 | 12（月） |
| t_user / t_group | - | 广播表 | - | 4（每库一份） |

### 3.3 关键概念

**绑定表**：分片键相同的表，ShardingSphere 保证同一 short_code 的数据在同一物理库。JOIN 时不会笛卡尔积（16 分片不会变成 16×16=256 次查询）。

**广播表**：结构和数据在所有库一致，用于被 JOIN 的字典表。写入时广播到所有库，读取时随机选一个库。

**INTERVAL 分片**：按时间分表，适合日志/明细类数据。本项目按月份分 12 张表（_202601~_202612），便于归档老数据。

### 3.4 雪花算法 + Base62 短码

```
雪花 ID（64 bit）= 41 bit 时间戳 + 5 bit datacenterId + 5 bit workerId + 12 bit 序列号
                                                          ↓
                                              Base62 编码 → 6~7 位短码
```

**容量**：62^6 ≈ 568 亿，远超业务需求。

**时钟回拨处理**：检测到 `currentTimestamp < lastTimestamp` 直接抛异常（严格拒绝策略）。替代方案有等待追平、借用未来时间，但本项目选严格拒绝更安全。

### 3.5 踩坑记录

| 坑 | 现象 | 解决 |
|----|------|------|
| `INTERVAL_RANGE SPI not found` | ShardingSphere 5.2.1 启动报错 | 改用 `type: INTERVAL`（5.2.1 无 INTERVAL_RANGE） |
| `setCodePointLimit NoSuchMethodError` | snakeyaml 版本冲突 | 父 pom 加 `<snakeyaml.version>1.33</snakeyaml.version>` 覆盖 SpringBoot 2.7.18 自带的 1.30 |
| 分页深度分页性能差 | offset 大时各分片返回冗余数据多 | 限制最大页数 / 改游标分页 / 冗余映射表 |

### 3.6 学习要点

- 分库分表的核心是「分片键选择」，决定查询路由效率
- 绑定表避免笛卡尔积，广播表避免跨库 join，两者解决不同问题
- 分库分表后跨分片聚合是难点，本项目用「内存聚合 + HyperLogLog」绕开
- ShardingSphere 版本差异大，5.2.1 与 5.3.x 的 SPI 名称不同

---

## 四、亮点 3：多级限流 + 熔断降级 + 幂等防刷

### 4.1 三层限流体系

| 层级 | 算法 | 维度 | 阈值 | 适用场景 |
|------|------|------|------|---------|
| 接口级 | 令牌桶 | 接口 | 5000 QPS | 保护整体，允许突发 |
| 用户级 | 滑动窗口 | userId | 50 次/秒 | 防单用户刷接口 |
| IP 级 | 滑动窗口 | IP | - | 防恶意 IP |

**令牌桶 vs 滑动窗口**：
- 令牌桶允许突发（桶里积攒的令牌可瞬间放过），适合接口级
- 滑动窗口精确控制窗口内总量，适合用户/IP 级的绝对上限

### 4.2 Redis + Lua 保证原子性

令牌桶的「读余量→计算填充→扣减→写回」是多步操作，多线程下有竞态。

**方案**：用 Lua 脚本封装全部逻辑，Redis 单线程执行 Lua 是原子的。

**集群模式注意**：限流 key 必须在同一 slot。本项目限流 key 是单 key（`bucket:接口名`），天然在单 slot，无跨 slot 问题。

### 4.3 @RateLimit 注解 + AOP + SpEL

```java
@RateLimit(type = LimitType.USER, maxRequests = 50, windowSize = 1000, 
           key = "#req.userId", message = "生成过于频繁")
@PostMapping("/create")
public Result<...> create(@Valid @RequestBody ShortLinkCreateReqDTO req) { ... }
```

**实现要点**：
- AOP 环绕通知拦截标注 `@RateLimit` 的方法
- SpEL 表达式解析 key（`#req.userId`），用 `DefaultParameterNameDiscoverer` 获取参数名
- 不指定 key 时按 LimitType 自动生成（INTERFACE=方法全名，USER=当前用户，IP=请求 IP）

### 4.4 Sentinel 熔断降级

**双熔断策略**：
- 慢调用比例：RT > 100ms 占比 > 50% → 熔断 10s
- 异常比例：异常比例 > 50% → 熔断 10s

**降级策略**：熔断后只读 Caffeine 本地缓存，命中返回旧数据，未命中抛业务异常。这是「可用性 > 一致性」的取舍——返回稍旧数据比报错好。

**规则持久化**：用 SPI（`InitFunc`）从 classpath 读取 JSON 规则文件加载，不依赖 Spring。

### 4.5 幂等创建

```
1. 查 Redis 幂等映射（idempotent:userId:longUrlHash → shortCode），命中直接返回
2. 未命中 → Redisson tryLock(0, 10s)（不等待，拿不到说明有并发）
3. 拿到锁后双重检查幂等映射（可能已被其他请求写入）
4. 仍未命中 → 生成短码 → 写 DB → 写缓存 → 写幂等映射 → 加布隆过滤器
```

**为什么要双重检查？** 「查 Redis」和「获取锁」之间有窗口，另一个请求可能刚写完幂等映射。

### 4.6 IP 黑名单动态封禁

- 1 分钟内访问 ≥ 1000 次 → 自动加入黑名单
- 封禁 1 小时，过期自动解封
- 误杀应对：白名单兜底（公司内网、CDN IP）、渐进式封禁

### 4.7 踩坑记录

| 坑 | 现象 | 解决 |
|----|------|------|
| Sentinel `ExceptionInInitializerError` | 沙箱禁止写 `*.lck` 日志文件，`DegradeRuleManager` 静态初始化失败 | `SentinelConfig.initDegradeRules()` 用 try-catch(Throwable) 包裹，不阻塞应用启动 |

### 4.8 学习要点

- 限流算法选型：突发流量用令牌桶，绝对上限用滑动窗口
- Lua 脚本是 Redis 保证原子性的标准方案，集群模式注意 hash slot
- 熔断降级的本质是「快速失败 + 兜底降级」，避免级联故障
- 幂等的核心是「唯一标识 + 锁 + 双重检查」
- Sentinel 规则持久化用 SPI 而非 Spring，因为 Sentinel 核心不依赖 Spring

---

## 五、亮点 4：Kafka 削峰 + 多线程异步聚合

### 5.1 异步统计链路全景

```
跳转请求
  ↓ (同步，O(1) 非阻塞)
AccessRecordQueue（ArrayBlockingQueue, 容量 10000）
  ↓ (后台线程 statsExecutor 匀速消费)
KafkaAccessRecordProducer → Kafka topic short-link-access-record
  ↓ (@KafkaListener 消费)
AccessRecordConsumer
  ↓ (投递到聚合器)
StatsAggregator（两级 ConcurrentHashMap 内存聚合）
  ↓ (@Scheduled 每 60s)
StatsFlushTask → 批量落库 t_link_access_stats
```

### 5.2 为什么先入本地队列再发 Kafka

**直接发 Kafka 的问题**：
1. `kafkaTemplate.send` 内部有序列化、选分区、缓冲，Broker 不可达时 max.block.ms=60s 会阻塞跳转线程
2. 突发流量直接打到 Kafka，Broker 压力骤增

**本地队列的好处**：
1. `offer` 是 O(1) 非阻塞，跳转线程立即返回
2. 削峰填谷：队列缓冲突发，后台线程匀速发 Kafka
3. 降级容错：队列满时丢弃记录（有损策略），优先保跳转可用

**代价**：最多丢 10000 条访问记录（队列满时），对统计场景可接受。

### 5.3 UV 去重：HyperLogLog vs Set

| 维度 | HyperLogLog | Set |
|------|-------------|-----|
| 内存 | 固定 12KB | 每元素 ~50 字节 |
| 误差 | 0.81% | 精确 |
| 操作 | PFADD/PFCOUNT O(1) | SADD/SCARD O(1) |

**结论**：UV 用 HyperLogLog（海量去重，0.81% 误差可接受，省内存）；IP 数用 Set（数量少，需精确值用于风控）。

### 5.4 两级 ConcurrentHashMap 聚合

```java
ConcurrentHashMap<shortCode, ConcurrentHashMap<dateHour, StatsBucket>>
```

- 第一级 shortCode：不同短链天然隔离
- 第二级 dateHour：同一短链不同小时隔离，flush 时按 bucket 粒度 remove
- StatsBucket 内部用 synchronized（pv++、Set.add、Map.merge 是多步复合操作）

**为什么用 synchronized 不用 CAS？** 聚合是写密集场景，pv++ 用 CAS 需自旋，高并发下自旋开销可能大于锁。synchronized 在 JDK 17 偏向锁/轻量锁优化下，无竞争时几乎零开销。

### 5.5 线程池隔离 + 拒绝策略

| 线程池 | 核心/最大 | 拒绝策略 | 理由 |
|--------|----------|----------|------|
| statsExecutor | 4/8 | CallerRunsPolicy | 统计不能丢，调用线程执行形成反压 |
| batchGenExecutor | 8/16 | AbortPolicy | 过载快速失败，让调用方感知 |
| cacheRefreshExecutor | 2/4 | DiscardOldestPolicy | 最新数据优先，丢弃最老任务 |
| bloomFilterExecutor | 1/2 | CallerRunsPolicy | 布隆加载不能丢 |

**为什么隔离？** 避免「线程池饥饿」——统计上报和批量生成共用一个池时，批量生成耗时长会占满线程，导致统计积压。

**RejectedTaskCounter**：包装原始拒绝策略，拒绝时计数，通过 Micrometer 暴露 `executor.rejected.count` 指标。

### 5.6 CompletableFuture 批量编排

```java
List<CompletableFuture<Resp>> futures = urls.stream()
    .map(url -> CompletableFuture.supplyAsync(() -> create(url), batchGenExecutor)
        .handle((resp, ex) -> ex != null ? null : resp))  // 异常隔离
    .collect(toList());
CompletableFuture.allOf(futures).join();  // 等待全部完成
```

**异常隔离关键**：`.handle` 捕获异常返回 null，避免单条失败导致 allOf 异常终止其他任务。

### 5.7 G1 调优要点

```
-Xms2g -Xmx2g          # 初始=最大，避免动态扩展停顿
-XX:+UseG1GC
-XX:MaxGCPauseMillis=100   # 目标停顿 100ms
-XX:G1HeapRegionSize=16m   # 2G 堆建议 16MB Region（堆/64）
-XX:+ParallelRefProcEnabled  # 并行处理引用，减少 GC 耗时
-XX:+HeapDumpOnOutOfMemoryError  # OOM 自动 dump
```

**为什么选 G1 不选 ZGC？**
1. JDK 17 中 ZGC 仍实验性，生产稳定性不如 G1
2. 2G 堆下 G1 停顿已足够（<100ms），ZGC 的亚毫秒级停顿对短链场景过剩
3. ZGC 适合超大堆（>32G），2G 堆 G1 更合适

### 5.8 学习要点

- 异步削峰的核心是「同步快速入队 + 后台匀速消费」
- 本地队列 + Kafka 是两层削峰：本地队列抗突发，Kafka 抗持续高压
- HyperLogLog 是海量去重的利器，用极小内存换可接受误差
- 线程池隔离避免业务互相影响，拒绝策略按业务特性选
- G1 调优的核心是「固定堆 + 目标停顿 + 合理 Region 大小」

---

## 六、通用知识点

### 6.1 Spring Boot 多模块工程

- 父 pom 用 `<packaging>pom</packaging>` + `<modules>` 聚合子模块
- `dependencyManagement` 统一版本，子模块引用时不写 version
- 可执行 fat jar 需在主模块配 `spring-boot-maven-plugin` 的 `repackage` goal

### 6.2 MyBatis-Plus + ShardingSphere 集成

- ShardingSphere 提供 DataSource，MyBatis-Plus 使用该 DataSource
- 对上层透明，Mapper 无感知分库分表
- `application-sharding.yml` 单独配置分片规则，通过 profile 激活

### 6.3 Actuator + Micrometer 监控

- `management.endpoints.web.exposure.include` 暴露端点
- `/actuator/health` 健康检查（含 db/redis/diskSpace）
- `/actuator/prometheus` Prometheus 格式指标
- 自定义指标用 `MeterRegistry` 注册 Gauge/Counter

### 6.4 全局异常处理

- `@RestControllerAdvice` + `@ExceptionHandler` 统一捕获
- 业务异常 `BizException` + 错误码枚举 `ErrorCode`
- 校验异常 `MethodArgumentNotValidException` 转友好提示
- 统一响应封装 `Result<T>`（code/message/data）

---

## 七、面试速记卡片

### 7.1 30 秒电梯演讲

> 我做了一个高并发短链接服务，用三级缓存（Caffeine+Redis+MySQL）把跳转接口缓存命中率做到 99%+；用 ShardingSphere 分 4 库 16 表支撑亿级短链；用 Redis+Lua 令牌桶 + Sentinel 熔断 + IP 黑名单做多层防护；用 Kafka + 本地队列削峰 + 内存聚合 + 定时落库处理访问统计，单节点支撑 5000 QPS。

### 7.2 核心数字

| 指标 | 数值 |
|------|------|
| 分片数 | 4 库 × 4 表 = 16 |
| 短码长度 | 6~7 位（Base62） |
| 布隆过滤器容量 | 1000 万，0.1% 误判 |
| 热点阈值 | 100 次/秒 |
| 缓存 TTL | 3600 + random(0,300) 秒 |
| 延迟双删间隔 | 500ms |
| 本地队列容量 | 10000 |
| 落库周期 | 60 秒 |
| UV 误差 | 0.81%（HyperLogLog） |
| 接口限流 | 5000 QPS（令牌桶） |
| 用户限流 | 50 次/秒（滑动窗口） |
| IP 封禁阈值 | 1000 次/分 |
| 堆内存 | 2G（G1） |
| GC 目标停顿 | 100ms |
| 设计 QPS | 5000 |

### 7.3 高频面试题索引

| 亮点 | 高频问题 | 答案位置 |
|------|---------|---------|
| 缓存 | 为什么加 Caffeine 不只用 Redis？ | [RESUME_HIGHLIGHTS.md Q1](file:///d:/CCDevelop/Garlic/docs/RESUME_HIGHLIGHTS.md) |
| 缓存 | 本地缓存一致性怎么保证？ | Q2 |
| 缓存 | 击穿/穿透/雪崩三件套？ | Q5 |
| 分片 | 为什么按 short_code 不按 user_id？ | Q1 |
| 分片 | 分库分表后如何分页？ | Q2 |
| 分片 | 雪花算法时钟回拨？ | Q4 |
| 限流 | 令牌桶 vs 滑动窗口？ | Q1 |
| 限流 | 幂等并发如何保证？ | Q4 |
| 异步 | 为什么先入本地队列再发 Kafka？ | Q1 |
| 异步 | UV 为什么用 HyperLogLog？ | Q7 |
| 异步 | 线程池为什么隔离？ | Q4 |
| JVM | G1 调优思路？为什么不用 ZGC？ | Q5 |

---

## 八、相关文档索引

| 文档 | 内容 |
|------|------|
| [README.md](file:///d:/CCDevelop/Garlic/README.md) | 项目总览、快速启动、面试支撑点 |
| [docs/ARCHITECTURE.md](file:///d:/CCDevelop/Garlic/docs/ARCHITECTURE.md) | 详细架构设计（缓存/分片/高并发/异步/JVM） |
| [docs/RESUME_HIGHLIGHTS.md](file:///d:/CCDevelop/Garlic/docs/RESUME_HIGHLIGHTS.md) | 4 大亮点的简历描述 + 面试问答 + 代码位置 |
| [docs/JVM_TUNING.md](file:///d:/CCDevelop/Garlic/docs/JVM_TUNING.md) | JVM G1 调优详解 |
| [docs/RUNNING_GUIDE.md](file:///d:/CCDevelop/Garlic/docs/RUNNING_GUIDE.md) | 运行指南 + 模块技术详解 + 10 个测试样例 |
| [benchmark/benchmark.md](file:///d:/CCDevelop/Garlic/benchmark/benchmark.md) | JMeter/wrk 压测说明 |

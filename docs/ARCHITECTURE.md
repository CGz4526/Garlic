# Garlic 短链接服务 - 架构设计文档

> 本文档详细阐述 Garlic 短链接服务的整体架构、缓存设计、分库分表、高并发策略、异步统计、线程池设计与 JVM 调优。

---

## 1. 整体架构

### 1.1 架构总览

系统采用经典分层架构，以「跳转接口高并发读」为核心优化目标：

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           客户端层 (浏览器 / App)                         │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │ HTTP (RESTful)
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      接入层 (Spring Boot, port 8001)                    │
│  ┌─────────────┐ ┌──────────────┐ ┌─────────────┐ ┌────────────────┐  │
│  │ IP 黑名单校验 │ │ @RateLimit   │ │ Sentinel    │ │ 全局异常处理    │  │
│  │ (Redis Set)  │ │ AOP 限流切面  │ │ 熔断降级     │ │ GlobalException│  │
│  └──────┬──────┘ └──────┬───────┘ └──────┬──────┘ └───────┬────────┘  │
│         └───────────────┴────────────────┴───────────────┘            │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          业务服务层 (Service)                           │
│  ┌──────────┐ ┌───────────┐ ┌──────────┐ ┌──────────┐ ┌────────────┐  │
│  │ Create   │ │  Jump     │ │ Manage   │ │  Batch   │ │ Stats Query│  │
│  │ 幂等创建  │ │ 三级缓存跳转│ │ CRUD     │ │ 并行编排  │ │ 统计查询    │  │
│  └────┬─────┘ └─────┬─────┘ └────┬─────┘ └────┬─────┘ └─────┬──────┘  │
│       │             │            │            │             │          │
│       ▼             ▼            ▼            ▼             ▼          │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │        TwoLevelCache (L1 Caffeine + L2 Redis + DB)               │  │
│  │   HotKeyDetector 探测 │ Redisson 锁防击穿 │ TTL 抖动防雪崩         │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────缓存一致性──────────────────────────────┐ │
│  │ CacheConsistencyService: 延迟双删(500ms) + Kafka 广播 + 去重       │ │
│  └────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      异步统计链路 (Kafka + 线程池)                      │
│  跳转 → AccessRecordQueue(10000) → Producer(statsExecutor) → Kafka    │
│       → Consumer → StatsAggregator(两级 Map + HyperLogLog) → FlushTask │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      ▼
┌──────────────────┬──────────────────────┬─────────────────────────────┐
│   Redis Cluster  │   Kafka Cluster      │   ShardingSphere → MySQL    │
│  缓存/锁/布隆/HLL │  access-record/cache │   4库×4表 16 分片 + 月份分表 │
└──────────────────┴──────────────────────┴─────────────────────────────┘
```

### 1.2 模块职责

| 模块 | 职责 | 关键组件 |
|------|------|---------|
| `short-link-common` | 公共组件层 | TwoLevelCache、HotKeyDetector、BloomFilterService、限流器、Sentinel、ThreadPoolConfig、工具类 |
| `short-link-dao` | 数据访问层 | ShortLinkDO/LinkAccessRecordDO/LinkAccessStatsDO/UserDO/GroupDO + 5 Mapper |
| `short-link-service` | 通用业务服务 | ShortCodeGenerator（雪花 ID + Base62 短码生成 + 冲突重试） |
| `short-link-api` | 主应用（Web 入口） | 5 Controller、5 Service、CacheConsistencyService、Kafka 生产消费、StatsAggregator |
| `short-link-mq/stat/admin` | 预留扩展模块 | 占位，供后续拆分 MQ、统计聚合、管理后台 |

---

## 2. 缓存架构设计

### 2.1 三级缓存查询链路

设计目标：跳转接口 RT < 20ms，缓存命中率 > 99%，DB 兜底。

```
请求 jump(shortCode)
        │
        ▼
┌───────────────────┐
│ 布隆过滤器前置校验  │ ── contains=false ──→ 抛 SHORT_LINK_NOT_FOUND（防穿透）
│ BloomFilterService │
└─────────┬─────────┘
          │ contains=true（可能存在）
          ▼
┌───────────────────┐
│ L1 Caffeine 本地   │ ── 命中 ──→ 反序列化返回（RT ~0.05ms）
│ (仅热点 key)       │
└─────────┬─────────┘
          │ 未命中
          ▼
┌───────────────────┐
│ L2 Redis 分布式    │ ── 命中 ──→ 反序列化 + 热点回填 L1 → 返回（RT ~1ms）
│ (TTL 3600+随机300) │
└─────────┬─────────┘
          │ 未命中
          ▼
┌───────────────────────────────────────────────────┐
│ 分布式锁防击穿 (Redisson tryLock(0, 5, SECONDS))  │
│  ┌─────────────┐                                 │
│  │ 拿到锁       │ → 双重检查 Redis → 仍未命中查 DB  │
│  │             │   → 回填 Redis(随机TTL) + L1(热点)│
│  └─────────────┘                                 │
│  ┌─────────────┐                                 │
│  │ 拿不到锁     │ → sleep(50ms) 重试 Redis         │
│  │             │ → 仍未命中查 DB 兜底（保可用性）   │
│  └─────────────┘                                 │
└───────────────────────────────────────────────────┘
```

**查询链路说明**（[TwoLevelCache.get](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/cache/TwoLevelCache.java) L111-L149）：

1. `hotKeyDetector.recordAccess(cacheKey)` 记录访问用于热点探测；
2. 查 L1 Caffeine `getIfPresent`，命中反序列化返回；反序列化失败清理脏数据；
3. 查 L2 Redis `opsForValue().get`，命中反序列化 + 若热点则回填 L1；
4. 缓存未命中进入 `getWithLock`（L167）分布式锁防击穿流程。

**关键设计：L1 只存热点 key**

冷数据全量进 Caffeine 会导致：① 内存爆炸；② 多实例不一致放大。通过 HotKeyDetector 判定，仅热点 key 才 `backfillCaffeineIfHot`（L294），L1 容量可控（1 万条上限），且只缓存真正高频 key。

### 2.2 热点 key 探测机制

[HotKeyDetector](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/cache/HotKeyDetector.java) 采用滑动窗口 + LongAdder 计数：

**数据结构**：
```
keyAccessCounter: ConcurrentHashMap<String, LongAdder>  // 当前窗口计数
windowCounter:    ConcurrentHashMap<String, LongAdder>  // 上一窗口计数
currentWindowStart: volatile long                        // 当前窗口起始时间
```

**算法流程**：

1. **记录访问**（`recordAccess` L103）：`keyAccessCounter.computeIfAbsent(key, k -> new LongAdder()).increment()`；LongAdder 分段累加，高并发写无竞争。

2. **窗口滚动**（`rotateWindow` L164，每秒执行）：
   - 清空 `windowCounter`；
   - 将 `keyAccessCounter` 中有计数的 key 迁移到 `windowCounter`（保留 LongAdder 引用，避免计数丢失）；
   - 清空 `keyAccessCounter`，更新 `currentWindowStart`。

3. **热点判定**（`isHotKey` L119）：加权计算
   ```
   elapsed = now - currentWindowStart
   remainRatio = max(0, 1 - elapsed / windowSizeMillis)  // 上一窗口剩余权重
   weighted = currentCount + previousCount × remainRatio
   isHot = weighted >= 100  // 阈值 100 次/秒
   ```
   加权近似滑动窗口效果：越靠近当前窗口起始，上一窗口权重越高，平滑过渡。

**为什么用 LongAdder**：高并发下 AtomicLong 的 CAS 在多核竞争同一内存地址时自旋开销大；LongAdder 采用 Cell 分段，每个 Cell 独立计数，sum 时合并，写性能远优于 AtomicLong，适合写多读少的热点计数。

### 2.3 缓存一致性方案

[CacheConsistencyService](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/cache/CacheConsistencyService.java) 解决多实例 L1 缓存一致性问题。

**延迟双删时序图**：

```
时间轴 ──────────────────────────────────────────────────────────→

实例A (更新方)                    实例B (读方)                   Redis
   │                                │                             │
   │ 1. DELETE Redis                │                             │ key 已删
   │ ──────────────────────────────►│                             │
   │                                │                             │
   │ 2. UPDATE DB                   │                             │
   │ ──────────────────────────────►│                             │ (主从复制延迟窗口)
   │                                │ 3. 读 Redis 未命中 → 查 DB   │
   │                                │    (可能读到从库旧值)         │
   │                                │ ───────────────────────────►│ 旧值回填
   │                                │                             │
   │ 4. 延迟 500ms 后二次删除         │                             │
   │    DELETE Redis                │                             │ 清除回填的旧值
   │    清理本地 Caffeine            │                             │
   │    发送 Kafka 广播              │                             │
   │ ──────────────────────────────►│                             │
   │                                │ 5. 收到广播清理本地 Caffeine   │
   │                                │                             │

兜底：Caffeine 60s TTL + Redis 1h TTL，最终一致
```

**实现细节**（`evictWithDoubleDelete` L135-L167）：

1. **第一次删 Redis**：更新 DB 前先删缓存，避免并发读到旧值；
2. **执行 DB 更新**：调用方传入 `dbUpdate` Runnable；
3. **延迟 500ms 二次删除**（`scheduledExecutor.schedule` L151）：
   - 再次 DELETE Redis（清除主从延迟窗口内被旧值回填的缓存）；
   - `caffeineCache.invalidate(shortCode)` 清理本地 L1；
   - `sendCacheInvalidMessage(shortCode)` 发送 Kafka 广播。

**Kafka 广播消费**（[CacheInvalidListener](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/cache/CacheInvalidListener.java) L74）：
- `@KafkaListener(topics = "short-link-cache-invalid")` 监听；
- 解析消息获取 `sourceInstanceId`，若等于当前实例 ID 则跳过（去重，L82）；
- `caffeineCache.invalidate(shortCode)` 清理本地缓存。

**消息顺序保证**：`sendCacheInvalidMessage`（L190）用 shortCode 作为 Kafka 消息 key，同一短链的失效消息落到同一分区顺序消费，避免乱序。

**兜底机制**：即使 Kafka 广播完全失败，Caffeine 60s TTL + Redis 1h TTL 保证最终一致——本地缓存最长 60s 自然过期。

### 2.4 缓存三大问题防护

| 问题 | 场景 | 防护方案 | 代码位置 |
|------|------|---------|---------|
| **穿透** | 查不存在的 key，每次打到 DB | 布隆过滤器前置校验（Redisson RBloomFilter，1000 万容量，0.1% 误判率）；contains=false 直接拒绝，不查 DB 不缓存空对象 | [BloomFilterService](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/bloom/BloomFilterService.java) L110 |
| **击穿** | 热点 key 过期瞬间大量请求打 DB | Redisson 分布式锁 `tryLock(0, 5, SECONDS)` + 双重检查 Redis；拿不到锁 sleep(50ms) 重试，兜底查 DB | [TwoLevelCache.getWithLock](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/cache/TwoLevelCache.java) L167-L224 |
| **雪崩** | 大量 key 同时过期 | Redis TTL = 3600 + random(0, 300) 秒离散过期；Caffeine 60s TTL 因写入时间不同自然离散 | [TwoLevelCache.computeTtlWithJitter](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/cache/TwoLevelCache.java) L281-L286 |

**布隆过滤器扩容**（[ScalableBloomFilter](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/bloom/ScalableBloomFilter.java)）：
- 主过滤器使用率 > 80% 触发 `checkAndExpand`（L54），创建扩容过滤器（容量翻倍）；
- 扩容期间双写（add L116），查询先主后扩容（contains L89）；
- 迁移完成后切换主从（需借助 DB 全量读取 shortCode，因 RBloomFilter 不支持遍历）。

---

## 3. 分库分表设计

### 3.1 分片策略

基于 ShardingSphere-JDBC 5.2.1，配置见 [application-sharding.yml](file:///d:/CCDevelop/Garlic/short-link-api/src/main/resources/application-sharding.yml)。

**分片总览**：

| 表 | 分片键 | 分片算法 | 分片数 | 说明 |
|----|--------|---------|--------|------|
| `t_short_link` | `short_code` | INLINE（哈希取模） | 4 库 × 4 表 = 16 | 短链主表，跳转查询精确路由 |
| `t_link_access_stats` | `short_code` | INLINE（哈希取模） | 4 库 × 4 表 = 16 | 与短链表绑定，避免笛卡尔积 |
| `t_link_access_record` | `access_time` | INTERVAL_RANGE（月份） | 12 表（按月） | 访问明细，便于归档 |
| `t_user` / `t_group` | - | 广播表 | 4 库全量 | 字典表，避免跨库 join |

**INLINE 分片算法**（application-sharding.yml L97-L110）：
```yaml
short_link_db_inline:
  type: INLINE
  props:
    algorithm-expression: ds_${short_code.hashCode().abs() % 4}
short_link_table_inline:
  type: INLINE
  props:
    algorithm-expression: t_short_link_${short_code.hashCode().abs() % 4}
```

跳转查询 `WHERE short_code = ?` 时，ShardingSphere 解析 short_code 计算 `hashCode().abs() % 4` 得到库号和表号，精确路由到单分片，避免广播查询。

**INTERVAL_RANGE 月份分表**（application-sharding.yml L112-L120）：
```yaml
access_record_interval:
  type: INTERVAL_RANGE
  props:
    datetime-pattern: yyyy-MM-dd HH:mm:ss
    datetime-lower: 2026-01-01 00:00:00
    datetime-upper: 2026-12-31 23:59:59
    sharding-suffix-pattern: yyyyMM
    datetime-interval-amount: 1
    datetime-interval-unit: MONTHS
```

按 access_time 路由到 `t_link_access_record_202601` ~ `t_link_access_record_202612`，每月一张表，便于按月归档老数据。

### 3.2 广播表与绑定表设计

**广播表**（application-sharding.yml L90-L92）：
```yaml
broadcast-tables:
  - t_user
  - t_group
```

- 表结构与数据在所有 4 个库完全一致；
- 写入时 ShardingSphere 向所有库广播写入，保证一致；
- 读取时随机选一个库，避免跨库 join；
- 适用场景：数据量小、更新少、需与分片表 join 的字典/配置表（如 user_id 校验）。

**绑定表**（application-sharding.yml L86-L87）：
```yaml
binding-tables:
  - t_short_link,t_link_access_stats
```

- 分片键相同（short_code）、分片算法相同的表；
- ShardingSphere 保证同一 short_code 的两条记录在同一物理库；
- `JOIN t_short_link s ON t_link_access_stats st ON s.short_code = st.short_code` 不会产生笛卡尔积（从 16×16=256 次查询降为 16 次同库 join）。

### 3.3 月份分表策略

`t_link_access_record` 按月份分表的设计考量：

1. **按时间维度查询**：访问明细常按时间范围查（如「某天的访问记录」），按 access_time 分表后同月数据在同一表，范围查询高效；
2. **归档便利**：超过 3 个月的表可通过 `RENAME TABLE` 或 `pt-archiver` 整表迁移到归档库，主库只保留近期数据；
3. **不按 short_code 分**：访问明细数据量大（每次跳转一条），按 short_code 分会导致单分片数据膨胀；明细查询频率低，跨分片聚合可接受；
4. **自动建表**：需定时任务每月初 `CREATE TABLE t_link_access_record_YYYYMM LIKE t_link_access_record`，避免写入时表不存在。

### 3.4 分布式 ID 方案

[SnowflakeIdWorker](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/util/SnowflakeIdWorker.java) + [Base62Utils](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/util/Base62Utils.java)：

**雪花 ID 结构**（64 bit）：
```
| 1 bit 符号 | 41 bit 时间戳 | 5 bit DC | 5 bit worker | 12 bit 序列 |
```

- 时间戳：相对 2024-01-01 的毫秒差，可用约 69 年；
- DC + worker：共支持 32 × 32 = 1024 个节点；
- 序列号：单 ms 内最多 4096 个 ID。

**时钟回拨处理**（nextId L83-L86）：
```java
if (currentTimestamp < lastTimestamp) {
    throw new IllegalStateException("时钟回拨，拒绝生成 ID");
}
```
严格拒绝策略，避免回拨期间生成重复 ID。worker-id/datacenter-id 在 application.yml 配置（`snowflake.worker-id=1, datacenter-id=1`），多实例需唯一。

**Base62 编码**（Base62Utils.encode L29）：
- 字符集 `0-9A-Za-z` 共 62 个字符；
- 雪花 ID（long）转 62 进制 → 6~7 位短码；
- 62^6 ≈ 568 亿，62^7 ≈ 3.5 万亿，足够覆盖短链需求。

**冲突重试**（[ShortCodeGenerator.generateShortCodeWithConflictCheck](file:///d:/CCDevelop/Garlic/short-link-service/src/main/java/com/garlic/shortlink/service/shortcode/ShortCodeGenerator.java) L69）：
- 生成短码后用 `existsChecker` 校验是否存在（查 Redis/DB）；
- 冲突则重试，最多 3 次；
- 雪花 ID 全局唯一，冲突概率极低，重试主要防御 Base62 编码碰撞。

---

## 4. 高并发设计

### 4.1 多级限流架构

```
请求 ──► ┌─────────────┐ ──► ┌──────────────┐ ──► ┌─────────────┐ ──► 业务
         │ IP 黑名单    │     │ 接口级限流    │     │ 用户级限流   │
         │ (Redis Set)  │     │ 令牌桶 5000QPS│     │ 滑动窗口 50/s│
         │ 1000/min封禁  │     │ (Redis+Lua)  │     │ (Redis ZSet) │
         └─────────────┘     └──────────────┘     └─────────────┘
              │                    │                    │
              ▼                    ▼                    ▼
         命中黑名单→429        超限→429            超限→429
```

**三种限流算法对比**：

| 算法 | 实现 | 特点 | 适用维度 | 代码位置 |
|------|------|------|---------|---------|
| 令牌桶 | Redis + Lua（TokenBucketRateLimiter） | 允许突发（桶里积攒令牌），平滑速率 | 接口级（整体 QPS） | [TokenBucketRateLimiter](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/ratelimit/TokenBucketRateLimiter.java) L28 |
| 滑动窗口 | Redis ZSet + Lua（SlidingWindowRateLimiter） | 精确控制窗口内总量 | 用户级/IP 级 | [SlidingWindowRateLimiter](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/ratelimit/SlidingWindowRateLimiter.java) L28 |
| 漏桶 | - | 强制匀速，不允许突发 | 整形下游（本项目未用） | - |

**令牌桶 Lua 脚本**（TokenBucketRateLimiter.TOKEN_BUCKET_LUA L31）保证原子性：
```lua
-- 读取上次时间和当前令牌数
-- 计算时间差填充令牌（不超过容量）
-- 令牌足够则扣减返回 1，否则返回 0
```

**滑动窗口 Lua 脚本**（SlidingWindowRateLimiter.SLIDING_WINDOW_LUA L31）：
```lua
-- ZREMRANGEBYSCORE 移除窗口外过期请求
-- ZCARD 统计当前窗口请求数
-- 未超限则 ZADD 添加当前请求，EXPIRE 刷新过期时间
```

**AOP 切面**（[RateLimitAspect](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/ratelimit/RateLimitAspect.java) L65）：
- `@Around("@annotation(rateLimit)")` 拦截标注 `@RateLimit` 的方法；
- 按 `LimitType`（INTERFACE/USER/IP）调用对应限流器；
- 支持 SpEL 表达式解析 key（如 `#req.userId`）；
- 超限抛 `BizException(RATE_LIMITED)`，由 GlobalExceptionHandler 统一返回 429。

### 4.2 熔断降级策略

[Sentinel](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/sentinel/SentinelRulePersistence.java) 配置跳转接口资源 `jumpResource`：

**熔断规则**（rules/sentinel-degrade-rules.json）：
- **慢调用比例熔断**：RT > 100ms 的请求占比 > 50% 时熔断 10s；
- **异常比例熔断**：异常比例 > 50% 时熔断 10s；
- 最小请求数 `minRequestAmount`：触发熔断的最小请求量，避免低流量误判。

**降级策略**（[ShortLinkJumpService.jumpBlockHandler](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/shortlink/ShortLinkJumpService.java) L353）：
```java
public String jumpBlockHandler(String shortCode, BlockException ex) {
    return fallbackFromLocalCache(shortCode);  // 只读 Caffeine 本地缓存
}
```

- 熔断时只读 Caffeine 本地缓存（Redis/DB 可能不可用）；
- 命中返回 originalUrl，未命中抛友好业务异常（不返回 500）；
- 取舍：可用性 > 数据一致性，返回稍旧缓存数据比直接报错好。

**规则持久化**（SentinelRulePersistence L39）：
- 实现 `InitFunc` 接口，通过 SPI 机制加载（`META-INF/services/com.alibaba.csp.sentinel.init.InitFunc`）；
- `init()` 从 classpath 读取 JSON 规则文件，`DegradeRuleManager.loadRules(rules)` 加载；
- 规则与代码解耦，运维可直接改 JSON 调整熔断策略。

### 4.3 接口幂等与防刷

**创建接口幂等**（[ShortLinkCreateService.createShortLink](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/shortlink/ShortLinkCreateService.java) L82）：

```
1. 计算幂等 key = "idempotent:" + userId + ":" + abs(longUrl.hashCode())
2. 查 Redis 幂等映射 → 命中直接返回 shortCode（快速路径）
3. 未命中 → Redisson tryLock(0, 10, SECONDS) 获取分布式锁
4. 拿到锁 → 双重检查幂等映射（可能已被并发请求写入）
5. 仍未命中 → 生成短码 → 写 DB → 写缓存 → 写幂等映射 → 加布隆过滤器
6. 释放锁
```

幂等 key 用 `userId + longUrl.hashCode()`，相同用户相同 URL 返回相同 shortCode，TTL 1 天。

**IP 黑名单防刷**（[IpBlacklistService](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/ratelimit/IpBlacklistService.java) L35）：

| 机制 | 实现 | 参数 |
|------|------|------|
| 黑名单存储 | Redis Set `short:link:ip:blacklist` | SISMEMBER O(1) |
| 访问计数 | 分钟级 key `short:link:ip:counter:{ip}:{yyyyMMddHHmm}` | TTL 70s |
| 封禁阈值 | 1 分钟内 ≥ 1000 次 | 自动加入黑名单 |
| 封禁时长 | 1 小时 | TTL 3600s，过期自动解封 |

跳转接口流程（[ShortLinkJumpController](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/controller/ShortLinkJumpController.java) L62）：
1. `ipBlacklistService.checkAndBlock(ip)` 命中黑名单或超阈值→拒绝；
2. `ipBlacklistService.incrementAccess(ip)` 递增计数；
3. 调用 Service 执行跳转。

---

## 5. 异步统计架构

### 5.1 本地队列削峰链路

```
跳转请求 (jump)
    │
    │ reportAccessRecord(shortCode)
    ▼
┌─────────────────────────────────────────┐
│ AccessRecordQueue (ArrayBlockingQueue)  │
│ 容量 10000，offer O(1) 非阻塞            │
│ 队列满 → 丢弃 + 日志告警（有损策略）     │
└──────────────────┬──────────────────────┘
                   │ take() 阻塞出队
                   ▼
┌─────────────────────────────────────────┐
│ KafkaAccessRecordProducer (后台守护线程) │
│ runLoop: while(running) { take → send } │
│ doSend: kafkaTemplate.send(topic, json) │
│ 异步回调 onFailure → 仅日志（降级）      │
└──────────────────┬──────────────────────┘
                   │ Kafka topic (short-link-access-record)
                   ▼
┌─────────────────────────────────────────┐
│ AccessRecordConsumer (@KafkaListener)   │
│ 按 shortCode 分区顺序消费               │
│ onAccessRecord → statsAggregator.aggregate │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│ StatsAggregator (内存聚合)              │
│ 两级 ConcurrentHashMap + StatsBucket    │
│ Redis HyperLogLog (UV) + Set (IP)       │
│ 访问明细同步落库（按月份分表）           │
└──────────────────┬──────────────────────┘
                   │ @Scheduled fixedRate=60000
                   ▼
┌─────────────────────────────────────────┐
│ StatsFlushTask (定时落库)               │
│ 遍历聚合 Map → remove bucket → insert  │
│ t_link_access_stats (按 short_code 分表)│
└─────────────────────────────────────────┘
```

**设计要点**：

1. **本地队列削峰**（[AccessRecordQueue](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/mq/AccessRecordQueue.java)）：
   - `ArrayBlockingQueue(10000)`，基于数组的有界阻塞队列，内存连续 cache 友好；
   - `offer()` O(1) 非阻塞入队，不影响跳转 RT；
   - 队列满返回 false，丢弃记录（有损策略，统计允许近似，跳转必须可用）。

2. **后台线程消费**（[KafkaAccessRecordProducer](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/mq/KafkaAccessRecordProducer.java) runLoop L108）：
   - 单线程守护线程 `kafka-access-record-producer`，`queue.take()` 阻塞出队；
   - `kafkaTemplate.send()` 异步发送，回调 `onFailure` 仅日志；
   - 优雅关闭：`shutdown()`（L203）设置 running=false，中断线程，`drainRemaining()` 排空剩余最多等 5s。

### 5.2 Kafka 消费者分区与聚合

**分区策略**：
- 生产端发送时用 shortCode 作为消息 key（Kafka 按 key 哈希分区）；
- 同一 shortCode 的访问记录落到同一分区，消费者按分区顺序消费；
- `spring.kafka.listener.concurrency` 控制消费线程数，多线程时不同分区并行、同分区串行。

**消费者入口**（[AccessRecordConsumer.onAccessRecord](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/stats/AccessRecordConsumer.java) L54）：
```java
@KafkaListener(topics = "short-link-access-record", groupId = "short-link-consumer")
public void onAccessRecord(String message) {
    JSONObject record = JSONUtil.parseObj(message);
    statsAggregator.aggregate(record);  // 委托聚合器
}
```

### 5.3 内存聚合 + 定时落库

[StatsAggregator](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/stats/StatsAggregator.java) 采用两级 ConcurrentHashMap 聚合：

**数据结构**：
```
aggregateMap: ConcurrentHashMap<String, ConcurrentHashMap<String, StatsBucket>>
                  ↑ shortCode              ↑ dateHourKey      ↑ 聚合桶
```

**聚合流程**（`aggregate` L80）：
1. 提取 shortCode、accessTime，解析 statsDate + hour → dateHourKey（如 `20260708:14`）；
2. `computeIfAbsent` 获取或创建 StatsBucket（线程安全）；
3. `bucket.increment()` 累加 PV、UV Set/IP Set 去重、维度 Map.merge 计数；
4. Redis HyperLogLog 记录 UV（`recordUv` L146），Redis Set 记录 IP（`recordIp` L178）；
5. 访问明细同步落库 `t_link_access_record`（按 access_time 月份分表）。

**StatsBucket 线程安全**（L247）：
- 内部所有变更方法 `synchronized` 修饰，保证 `pv++`/`Set.add`/`Map.merge` 原子性；
- bucket 粒度小（单 shortCode+hour），锁竞争低；
- JDK 17 synchronized 偏向锁/轻量锁优化，无竞争时几乎零开销。

**定时落库**（[StatsFlushTask.flushStats](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/stats/StatsFlushTask.java) L62）：
- `@Scheduled(fixedRate = 60000)` 每分钟执行；
- 遍历聚合 Map，先 `bucketIt.remove()` 移除 bucket 再落库（避免 flush 期间新数据写入旧桶）；
- UV 从 Redis HyperLogLog `PFCOUNT` 获取（跨实例全局去重）；
- IP 数从 Redis Set `SCARD` 获取；
- 维度字段取分布中最大的一个（topLocale/topBrowser 等）；
- `linkAccessStatsMapper.insert()` 落库，ShardingSphere 按 short_code 自动路由分表。

### 5.4 UV 去重（HyperLogLog）

| 方案 | 内存 | 误差 | 操作复杂度 | 适用 |
|------|------|------|-----------|------|
| HyperLogLog | 固定 12KB | 0.81% | PFADD/PFCOUNT O(1) | 海量 UV 去重 |
| Set | ~50B/元素 | 精确 | SADD/SCARD O(1) | IP 数（需精确） |

**UV 实现**（`recordUv` L146）：
- key = `short:link:uv:{shortCode}:{yyyyMMdd}`，按天分桶；
- `opsForHyperLogLog().add(key, userIdentifier)`；
- TTL 8 天（覆盖一周 + 1 天缓冲）；
- 查询 `opsForHyperLogLog().size(key)` 返回近似 UV 数。

**IP 数实现**（`recordIp` L178）：
- key = `short:link:ip:{shortCode}:{yyyyMMdd}`；
- `opsForSet().add(key, ip)`，`opsForSet().size(key)` 精确计数；
- IP 数量远少于 UV，内存可控；需精确值用于风控分析。

---

## 6. 线程池设计

### 6.1 隔离线程池

[ThreadPoolConfig](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/config/ThreadPoolConfig.java) 配置 4 个隔离线程池：

| 线程池 | 核心/最大 | 队列 | 拒绝策略 | 用途 | 选型理由 |
|--------|-----------|------|----------|------|---------|
| `statsExecutor` | 4/8 | LinkedBlockingQueue(1000) | CallerRunsPolicy | 统计异步上报 | 反压保护，不能丢数据 |
| `batchGenExecutor` | 8/16 | ArrayBlockingQueue(500) | AbortPolicy | 批量短链生成 | 快速失败，让调用方感知过载 |
| `cacheRefreshExecutor` | 2/4 | ArrayBlockingQueue(200) | DiscardOldestPolicy | 缓存异步刷新 | 保新弃旧，最新数据优先 |
| `bloomFilterExecutor` | 1/2 | ArrayBlockingQueue(100) | CallerRunsPolicy | 布隆过滤器加载 | 不能丢，同步执行保证一致 |

**隔离原则**：不同业务用不同线程池，避免「线程池饥饿」——若统计上报和批量生成共用一个池，批量生成耗时长会占满线程导致统计上报积压。隔离后各业务独立线程池、独立队列、独立拒绝策略，互不影响、独立监控。

**显式构造**（createExecutor L142）：使用 `ThreadPoolExecutor` 显式构造而非 `Executors` 工厂方法，符合阿里规约（避免无界队列导致 OOM）。`allowCoreThreadTimeOut(true)` 允许核心线程空闲回收，节省资源。

### 6.2 拒绝策略选型

**RejectedTaskCounter 包装**（L223-L244）：
```java
public static class RejectedTaskCounter implements RejectedExecutionHandler {
    private final RejectedExecutionHandler delegate;
    private final AtomicLong rejectedCount = new AtomicLong(0);

    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        rejectedCount.incrementAndGet();  // 先计数
        delegate.rejectedExecution(r, executor);  // 再委托原策略
    }
}
```

委托模式：不改变原拒绝策略行为，附加计数功能，便于监控。

**四种拒绝策略对比**：

| 策略 | 行为 | 适用场景 |
|------|------|---------|
| AbortPolicy | 抛 RejectedExecutionException | 调用方需感知过载（批量生成） |
| CallerRunsPolicy | 调用线程自己执行 | 不能丢数据 + 反压（统计上报） |
| DiscardOldestPolicy | 丢弃队列最老任务 | 最新数据优先（缓存刷新） |
| DiscardPolicy | 静默丢弃 | 允许丢失（日志记录等） |

### 6.3 监控指标

`@PostConstruct registerMetrics`（L185）注册 Micrometer 指标：

| 指标 | 类型 | 说明 |
|------|------|------|
| `executor.active.count` | Gauge | 活跃线程数 |
| `executor.queue.size` | Gauge | 队列任务数 |
| `executor.completed.count` | Gauge | 累计完成任务数 |
| `executor.rejected.count` | Gauge | 累计拒绝任务数 |

所有指标带 `name` 标签区分线程池，通过 `/actuator/prometheus` 暴露给 Prometheus，Grafana 可可视化监控线程池负载与拒绝率，拒绝率上升时告警扩容或限流。

---

## 7. JVM 调优

### 7.1 参数选择理由

[startup.sh](file:///d:/CCDevelop/Garlic/bin/startup.sh) L7-L18 JVM 参数：

```bash
JAVA_OPTS="-server \
  -Xms2g -Xmx2g -Xmn1g \              # 堆内存
  -XX:+UseG1GC \                       # 收集器
  -XX:MaxGCPauseMillis=100 \           # 目标停顿
  -XX:G1HeapRegionSize=16m \           # Region 大小
  -XX:+ParallelRefProcEnabled \        # 并行引用处理
  -XX:+PrintGCDetails \                # GC 详情
  -Xlog:gc*:file=logs/gc.log:... \     # GC 日志
  -XX:+HeapDumpOnOutOfMemoryError \    # OOM dump
  -XX:HeapDumpPath=logs/heapdump.hprof"
```

| 参数 | 值 | 选择理由 |
|------|----|---------|
| `-Xms2g -Xmx2g` | 初始堆=最大堆=2G | 避免堆动态扩展导致停顿；2G 支撑单节点 5000 QPS |
| `-Xmn1g` | 新生代 1G | 约占堆 50%，G1 中由 Region 动态分配，此参数参考 |
| `-XX:+UseG1GC` | G1 收集器 | JDK 9+ 默认，适合大堆+低延迟，Region 化设计可预测停顿 |
| `-XX:MaxGCPauseMillis=100` | 目标停顿 100ms | 跳转接口 RT<20ms，GC 不能卡顿 |
| `-XX:G1HeapRegionSize=16m` | Region 16MB | 2G 堆建议 16MB（堆/64），平衡大对象分配与 GC 效率 |
| `-XX:+ParallelRefProcEnabled` | 并行引用处理 | 减少引用对象处理耗时 |
| `-XX:+HeapDumpOnOutOfMemoryError` | OOM 自动 dump | 便于事后分析 OOM 原因 |

### 7.2 G1 调优思路

**为什么选 G1 不选 ZGC**：
1. JDK 17 中 ZGC 仍需实验性参数解锁，生产稳定性不如 G1；
2. G1 在 2G 堆下停顿已足够（<100ms），ZGC 的亚毫秒级停顿对短链场景过剩；
3. ZGC 适合超大堆（>32G），2G 堆 G1 更合适；
4. G1 是 JDK 9+ 默认，生态成熟，监控工具支持好。

**堆大小选择依据**：
- 跳转接口热点数据在 Caffeine + Redis 缓存，DB 查询少；
- 堆压力主要来自：Kafka 消费者聚合数据（ConcurrentHashMap）、并发请求的短期对象（DTO、JSON 序列化）；
- 2G 堆可支撑单节点 5000 QPS，Full GC 频率 < 1 次/小时。

**G1 Region 设计**：
- 堆划分为等大小 Region（2G/16MB = 128 个 Region）；
- 每个 Region 可动态充当 Eden/Survivor/Old/Humongous；
- GC 时优先回收「垃圾最多」的 Region（Garbage First），在目标停顿内最大化回收效率。

### 7.3 监控与排查

**监控端点**（application.yml 暴露 Actuator）：
- `/actuator/health`：健康检查
- `/actuator/metrics/jvm.gc.pause`：GC 停顿时间
- `/actuator/metrics/jvm.gc.live.data.size`：存活数据大小
- `/actuator/metrics/jvm.threads.live`：活跃线程数
- `/actuator/threaddump`：线程转储（排查死锁）
- `/actuator/prometheus`：Prometheus 指标抓取

**OOM 排查**：
1. OOM 时自动 dump 到 `logs/heapdump.hprof`；
2. 用 MAT/VisualVM 分析 dump，定位大对象；
3. 结合 GC 日志 `logs/gc.log` 分析 GC 频率与停顿；
4. 常见 OOM 原因：Caffeine 缓存膨胀（调小 maximumSize）、Kafka 消费积压（aggregationMap 过大）、线程池队列堆积。

**GC 日志分析**：
```
-Xlog:gc*:file=logs/gc.log:time,uptime,level,tags:filecount=5,filesize=10m
```
- `gc*` 输出所有 GC 相关日志；
- `filecount=5,filesize=10m` 滚动 5 个文件每个 10MB；
- 关注 Full GC 频率与 Young GC 停顿，Full GC 频繁说明老年代不足或内存泄漏。

---

## 附：架构决策记录

| 决策点 | 选择 | 替代方案 | 选择理由 |
|--------|------|---------|---------|
| 缓存层级 | 三级（Caffeine+Redis+DB） | 纯 Redis | 热点 key 本地命中降 RT，L1 容量可控 |
| L1 缓存策略 | 仅存热点 key | 全量缓存 | 避免内存爆炸 + 多实例不一致 |
| 一致性方案 | 延迟双删 + Kafka 广播 | 强一致（2PC） | 最终一致足够，强一致牺牲性能 |
| 分片键 | short_code | user_id | 跳转查询占 90%+，精确路由 |
| 限流算法 | 令牌桶+滑动窗口 | 单一算法 | 不同维度需求不同 |
| 异步队列 | 本地队列+Kafka | 直接 Kafka | 削峰 + 降级容错 |
| UV 去重 | HyperLogLog | Set | 12KB 固定内存 vs 50B/元素 |
| GC | G1 | ZGC | 2G 堆 G1 足够，ZGC 实验性 |

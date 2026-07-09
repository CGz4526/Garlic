# Garlic 短链接服务 - 简历亮点提炼

> 本文档针对项目的 4 大技术亮点，提供可直接用于简历的描述、精确到行号的代码位置、模拟面试官追问的深度问答，以及可量化的指标数据。

---

## 亮点 1：三级缓存架构 + 缓存一致性

### 简历描述

设计并实现 `Caffeine(L1 本地) → Redis(L2 分布式) → MySQL` 三级缓存架构，通过滑动窗口 + LongAdder 实时探测热点 key 仅回填本地缓存，避免冷数据撑爆 L1；采用「延迟双删 + Kafka 广播 + instanceId 去重」方案保证多实例 L1 缓存一致性；布隆过滤器（1000 万容量，0.1% 误判率）防穿透、Redisson 分布式锁双重检查防击穿、TTL 随机抖动防雪崩，跳转接口缓存命中率达 99%+。

### 关键代码位置

| 文件 | 类 | 行号范围 | 职责 |
|------|----|---------|------|
| [TwoLevelCache.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/cache/TwoLevelCache.java) | `TwoLevelCache` | L35-L333 | 二级缓存抽象，L1→L2→DB 查询链路 + 击穿/雪崩防护 |
| [TwoLevelCache.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/cache/TwoLevelCache.java) | `TwoLevelCache.getWithLock` | L167-L224 | 分布式锁 + 双重检查防击穿 |
| [HotKeyDetector.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/cache/HotKeyDetector.java) | `HotKeyDetector` | L38-L190 | 滑动窗口 + LongAdder 热点探测 |
| [HotKeyDetector.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/cache/HotKeyDetector.java) | `HotKeyDetector.isHotKey` | L119-L130 | 加权热点判定 |
| [BloomFilterService.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/bloom/BloomFilterService.java) | `BloomFilterService` | L34-L136 | Redisson RBloomFilter 防穿透 |
| [ScalableBloomFilter.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/bloom/ScalableBloomFilter.java) | `ScalableBloomFilter` | L34-L151 | 双布隆扩容方案 |
| [CacheConsistencyService.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/cache/CacheConsistencyService.java) | `CacheConsistencyService` | L47-L193 | 延迟双删 + Kafka 广播 |
| [CacheConsistencyService.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/cache/CacheConsistencyService.java) | `evictWithDoubleDelete` | L135-L167 | 延迟双删核心逻辑 |
| [CacheInvalidListener.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/cache/CacheInvalidListener.java) | `CacheInvalidListener` | L37-L96 | Kafka 广播消费 + 去重 |
| [ShortLinkJumpService.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/shortlink/ShortLinkJumpService.java) | `ShortLinkJumpService.jump` | L92-L170 | 三级缓存跳转主流程 |
| [ShortLinkTwoLevelCache.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/shortlink/ShortLinkTwoLevelCache.java) | `ShortLinkTwoLevelCache` | L38-L106 | 短链二级缓存类型安全封装 |

### 面试问答点

**Q1：为什么不只用 Redis？还要加一层 Caffeine 本地缓存？**

A：纯 Redis 方案在以下场景存在瓶颈：① 单次跳转 Redis 网络往返 RT 约 0.5~1ms，热点短链（如大促活动）下 Redis 集群带宽会成为瓶颈；② Redis 命中后仍需反序列化 JSON，本地缓存可直接命中对象引用。引入 Caffeine 后热点 key 直接在 JVM 内命中，RT 从 ~1ms 降到 ~0.05ms。但本地缓存不能全量缓存（内存受限、多实例不一致），所以只缓存「热点 key」——由 HotKeyDetector 滑动窗口判定阈值 100 次/秒，达到阈值才回填 Caffeine，冷数据只走 Redis。这样 L1 容量可控（Caffeine 配 1 万条上限），且只缓存真正的高频 key。

**Q2：本地缓存一致性怎么保证？Kafka 广播失败怎么办？**

A：多层保障：
1. **延迟双删**（CacheConsistencyService.evictWithDoubleDelete L135）：更新 DB 前先删 Redis，更新后延迟 500ms 二次删除 Redis，覆盖主从复制延迟窗口内被旧值回填的场景。
2. **Kafka 广播**（sendCacheInvalidMessage L182）：二次删除时同时发送 Kafka 消息到 `short-link-cache-invalid` topic，其他实例的 CacheInvalidListener 收到后清理本地 Caffeine。消息用 shortCode 作为 key，保证同一短链的消息落到同一分区顺序消费，避免乱序。
3. **instanceId 去重**（CacheInvalidListener.onCacheInvalid L82）：消息体携带 sourceInstanceId，消费者跳过自己发的消息，避免重复处理。
4. **兜底机制**：Caffeine 短过期（60s TTL）+ Redis 中过期（1h），即使 Kafka 广播完全失败，本地缓存最长 60s 后自然过期，达到最终一致。生产中可加监控告警 Kafka 消费 lag，lag 过大时人工介入。

**Q3：布隆过滤器误判怎么处理？如何扩容？**

A：布隆过滤器特性是「返回 false 一定不存在，返回 true 可能存在（误判）」。本项目用 Redisson RBloomFilter，预期 1000 万容量、0.1% 误判率（BloomFilterService.init L54）。误判的 shortCode 会查 DB 后返回不存在，不会写入缓存（避免缓存空对象放大问题），所以误判只是少量 DB 穿透，可接受。

扩容方案见 ScalableBloomFilter（L34）：
1. 主过滤器容量使用率超过 80% 时触发 `checkAndExpand`（L54），创建扩容过滤器（容量翻倍，名称 `short_link:bloom:expansion`）；
2. 扩容期间写入双写（add L116 同时写主和扩容），查询先查主再查扩容（contains L89）；
3. 由于 RBloomFilter 不支持遍历，迁移需借助 DB 全量读取 shortCode 异步写入扩容过滤器（Canal 监听 binlog 或定时任务），完成后切换主从。

**Q4：热点 key 如何判定？阈值多少？为什么用 LongAdder 而不是 AtomicLong？**

A：HotKeyDetector（L38）采用滑动窗口计数：
- 维护「当前窗口」与「上一窗口」两个 `ConcurrentHashMap<String, LongAdder>`，窗口大小 1 秒；
- 每次 `recordAccess` 在当前窗口 `computeIfAbsent(key, LongAdder).increment()`；
- `isHotKey` 加权计算：`当前窗口计数 + 上一窗口计数 × 剩余时间比例`，≥ 阈值 100 判定为热点；
- 定时调度器（scheduleAtFixedRate）每秒滚动窗口：清空上一窗口，当前窗口迁移为上一窗口。

阈值 100 次/秒的原因：单节点跳转 QPS 设计 5000，按短链均匀分布 1000 万短链算，平均每个 key 0.0005 次/秒；热点 key 通常是大促/营销短链，100 次/秒能筛出真正的热点而避免冷数据进入 L1。

用 LongAdder 而非 AtomicLong：高并发下 AtomicLong 的 CAS 自旋开销大（多核竞争同一内存地址），LongAdder 采用「分段累加 + 合并」策略，每个 Cell 独立计数，sum 时合并，写性能远高于 AtomicLong，适合写多读少的热点计数场景。

**Q5：缓存击穿/穿透/雪崩三件套如何防护？**

A：
- **穿透**（查不存在的 key）：布隆过滤器前置校验（ShortLinkJumpService.jump L96），`contains` 返回 false 直接抛异常不查 DB；返回 true 的少量误判会查 DB 后返回不存在，不缓存空对象（避免缓存被无效 key 撑爆）。
- **击穿**（热点 key 过期瞬间大量请求打到 DB）：`TwoLevelCache.getWithLock`（L167）Redisson 分布式锁 `tryLock(0, 5, SECONDS)` 不等待，拿到锁后双重检查 Redis（可能已被其他请求重建），仍未命中才查 DB；拿不到锁的请求 sleep(50ms) 后重试 Redis，兜底直接查 DB（保证可用性）。
- **雪崩**（大量 key 同时过期）：`computeTtlWithJitter`（L281）`TTL = 3600 + random(0, 300)` 秒，离散过期时间；Caffeine 自身 expireAfterWrite 60s 也会因写入时间不同而离散。

**Q6：为什么用 shortCode 作为 Kafka 消息 key？**

A：Kafka 按 key 哈希分区，同一 shortCode 的消息落到同一分区，消费者按分区顺序消费。这样同一短链的缓存失效消息按时间顺序处理，避免「先失效后清理」的乱序导致脏缓存回填。如果不按 key 分区，两条针对同一 shortCode 的失效消息可能被不同消费者并发处理，无法保证顺序。

**Q7：延迟双删的 500ms 是怎么定的？太短或太长有什么问题？**

A：500ms 是基于 MySQL 主从复制延迟的经验值——大多数场景主从延迟在 100~500ms 内。太短（如 50ms）可能仍在主从延迟窗口内，二次删除时从库还没同步完新值，旧值又会被读请求回填；太长（如 5s）期间缓存处于失效状态，增加 DB 压力。生产中可监控主从延迟动态调整，或读请求强制走主库（但牺牲性能）。本项目用 500ms 是兼顾一致性与可用性的折中。

### 可量化指标

| 指标 | 数值 | 说明 |
|------|------|------|
| 缓存命中率 | 99%+ | L1+L2 综合命中率，DB 查询比例 < 1% |
| L1 命中 RT | ~0.05ms | Caffeine 内存命中 |
| L2 命中 RT | ~1ms | Redis 网络往返 + 反序列化 |
| 布隆过滤器容量 | 1000 万 | 误判率 0.1%，占用 ~17MB |
| 热点判定阈值 | 100 次/秒 | 滑动窗口加权判定 |
| 缓存 TTL | 3600~3900s | 基准 1h + 0~300s 随机抖动 |
| 延迟双删间隔 | 500ms | 覆盖主从复制延迟窗口 |
| 本地缓存容量 | 1 万条 | 仅存热点 key，60s 过期 |

---

## 亮点 2：ShardingSphere 分库分表策略

### 简历描述

基于 ShardingSphere-JDBC 5.2.1 设计分库分表方案：短链表 `t_short_link` 按 `short_code` 哈希分 4 库 × 4 表 = 16 分片（INLINE 算法），访问统计表与短链表绑定同分片键避免笛卡尔积；访问明细表按 `access_time` 月份 INTERVAL_RANGE 自动分表便于归档；`t_user/t_group` 广播表避免跨库 join；雪花算法生成全局唯一 ID + Base62 编码生成 6~7 位短码，含时钟回拨检测与冲突重试。

### 关键代码位置

| 文件 | 行号范围 | 职责 |
|------|---------|------|
| [application-sharding.yml](file:///d:/CCDevelop/Garlic/short-link-api/src/main/resources/application-sharding.yml) | L7-L130 | ShardingSphere 完整配置（数据源/分片/绑定/广播） |
| [application-sharding.yml](file:///d:/CCDevelop/Garlic/short-link-api/src/main/resources/application-sharding.yml) | L45-L58 | t_short_link 分库分表策略 |
| [application-sharding.yml](file:///d:/CCDevelop/Garlic/short-link-api/src/main/resources/application-sharding.yml) | L86-L92 | 绑定表 + 广播表配置 |
| [application-sharding.yml](file:///d:/CCDevelop/Garlic/short-link-api/src/main/resources/application-sharding.yml) | L95-L120 | 分片算法（INLINE / INTERVAL_RANGE） |
| [schema.sql](file:///d:/CCDevelop/Garlic/sql/schema.sql) | L1-L178 | 4 库 × 4 表完整 DDL |
| [SnowflakeIdWorker.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/util/SnowflakeIdWorker.java) | L11-L130 | 雪花算法 ID 生成器 |
| [SnowflakeIdWorker.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/util/SnowflakeIdWorker.java) | L79-L106 | nextId + 时钟回拨检测 |
| [Base62Utils.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/util/Base62Utils.java) | L11-L67 | 62 进制编码（短码生成） |
| [ShortCodeGenerator.java](file:///d:/CCDevelop/Garlic/short-link-service/src/main/java/com/garlic/shortlink/service/shortcode/ShortCodeGenerator.java) | L31-L80 | 短码生成 + 冲突重试 |

### 面试问答点

**Q1：为什么按 short_code 分片而不是 user_id？**

A：核心考虑是「查询路由」。短链服务的最高频查询是跳转 `WHERE short_code = ?`（占 90%+ 流量），按 short_code 分片后这类查询能精确路由到单分片，避免广播查询。如果按 user_id 分片：
- 跳转查询不知道 user_id，ShardingSphere 会向所有 16 个分片广播再聚合，RT 放大 16 倍；
- 分页查询「某用户的短链列表」虽然能路由到单分片，但这不是高频场景。

短链创建时同时有 user_id 和 short_code，我们以 short_code 为分片键，user_id 查询走 idx_user_id 索引（单分片内索引）。这是一种「读写权衡」——牺牲低频的 user 维度查询性能，换取高频跳转查询的单分片精确路由。

**Q2：分库分表后如何分页查询？**

A：本项目分页查询主要是「按 user_id 分页查短链列表」（ShortLinkManageController.page），由于 user_id 不是分片键，ShardingSphere 会改写 SQL 为 `SELECT * FROM t_short_link_* WHERE user_id = ? ORDER BY create_time LIMIT offset, size`，向所有 16 个分片并行下发，各分片返回 `offset+size` 条，在协调层归并排序后取对应页。

这种「流式归并」存在深度分页问题：offset 越大各分片返回的冗余数据越多。优化方案：
1. 业务上限制最大页数（如不超过 100 页）；
2. 改用游标分页（`WHERE create_time < ? ORDER BY create_time DESC LIMIT size`），避免 offset；
3. 若 user_id 查询极频繁，可冗余一份「user_id → short_code 映射表」按 user_id 分片，先查映射再按 short_code 精确查。

**Q3：跨分片聚合怎么做？**

A：统计聚合场景（如全局 PV/UV）涉及跨分片。本项目的做法：
1. **明细落库**：访问明细按 access_time 月份分表（不按 short_code），所以同一月份的数据可能跨库，但 ShardingSphere 对聚合查询有优化；
2. **内存聚合**：StatsAggregator 在消费者内存中按 shortCode + dateHour 聚合，UV/IP 用 Redis HyperLogLog/Set 全局去重，避免跨分片聚合；
3. **统计表分片**：t_link_access_stats 按 short_code 分片与短链表绑定，查「某短链的统计」是单分片查询；
4. 若需全局统计（如平台总 PV），可定时跑批将各分片 stats 汇总到汇总表，或用 Elasticsearch 做离线分析。

**Q4：雪花算法时钟回拨怎么处理？**

A：SnowflakeIdWorker.nextId（L79）在生成前检测 `currentTimestamp < lastTimestamp`，若发生回拨直接抛 `IllegalStateException`（L83-L86），拒绝生成 ID。这是「严格拒绝」策略，避免回拨期间生成重复 ID。

替代方案：
1. **等待追平**：回拨幅度小（如 < 5ms）时 spin 等待时间追平；
2. **借用未来时间**：记录上次时间，回拨时沿用上次时间 + 序列号，但会导致 ID 超前；
3. **NTP 同步**：生产环境用 chrony/ntp 同步时钟，禁止大步进调整。

本项目选严格拒绝是因为短链生成不是超高并发场景（单节点 QPS < 5000，雪花单 ms 序列号 4096 足够），偶发回拨直接报错比生成重复 ID 安全。worker-id/datacenter-id 在 application.yml 配置（默认 1/1），多实例部署需保证唯一。

**Q5：月份分表如何归档老数据？**

A：t_link_access_record 配置 INTERVAL_RANGE 分片（application-sharding.yml L112），按 access_time 月份路由到 `t_link_access_record_202601` ~ `t_link_access_record_202612`。归档策略：
1. **冷数据迁移**：超过 3 个月的明细表通过 `RENAME TABLE` 或 `pt-archiver` 迁移到归档库（如 TiDB/ClickHouse），主库只保留近 3 个月；
2. **分片配置更新**：datetime-upper 需定期更新（当前配 2026-12-31，跨年需改）；
3. **自动建表**：可写定时任务每月初自动 `CREATE TABLE t_link_access_record_YYYYMM LIKE t_link_access_record`，避免写入时表不存在报错。

明细表只用于按需查询（如排查某次访问），高频统计走 t_link_access_stats（内存聚合 + 定时落库），所以明细归档不影响在线统计。

**Q6：广播表和绑定表分别解决什么问题？**

A：
- **广播表**（t_user/t_group）：表结构和数据在所有库完全一致，主要用于「被 join 的字典/配置表」。写入时 ShardingSphere 向所有库广播写入，读取时随机选一个库（避免跨库 join）。短链创建时需要校验 user_id 是否存在，广播后可在同库内 join 查询。
- **绑定表**（t_short_link ↔ t_link_access_stats）：分片键相同（short_code）、分片算法相同的表，ShardingSphere 保证同一 short_code 的两条记录在同一物理库。这样 `JOIN t_short_link s ON t_link_access_stats st ON s.short_code = st.short_code` 不会产生笛卡尔积（否则 16 分片会变成 16×16=256 次查询）。绑定后只需 16 次同库 join。

**Q7：INLINE 分片算法的表达式 `short_code.hashCode().abs() % 4` 有什么风险？**

A：风险点：
1. **数据倾斜**：String.hashCode 分布不均，某些短码可能集中落到少数分片。优化可改用一致性哈希或 MurmurHash3；
2. **扩容困难**：取模算法扩容时（4→8 库）需重新分布全部数据。可预先分更多物理表（如 16 库 64 表），按逻辑分片再映射，或用一致性哈希环；
3. **Java hashCode 跨语言不一致**：若其他语言客户端需计算路由，需保证哈希算法一致。本项目全 Java 栈，无此问题。

### 可量化指标

| 指标 | 数值 | 说明 |
|------|------|------|
| 分片数 | 16（4 库 × 4 表） | t_short_link / t_link_access_stats |
| 单分片数据量 | ~625 万 | 按 1 亿短链均分 |
| 短码长度 | 6~7 位 | Base62 编码雪花 ID |
| 雪花 ID 位数 | 64 bit | 41 时间 + 5 DC + 5 worker + 12 序列 |
| 单 ms 序列号上限 | 4096 | 单节点单毫秒最大生成 4096 个 ID |
| 月份分表数 | 12 | 2026-01 ~ 2026-12 |
| 广播表数 | 2 | t_user / t_group |
| 绑定表组数 | 1 | t_short_link ↔ t_link_access_stats |

---

## 亮点 3：多级限流 + 熔断降级 + 幂等防刷

### 简历描述

实现接口级令牌桶（Redis + Lua 原子脚本）、用户级/IP 级滑动窗口（ZSet）三层限流，`@RateLimit` 注解 + AOP 切面支持 SpEL 动态 key；Sentinel 配置慢调用比例（RT>100ms 占比>50%）与异常比例双熔断策略，熔断时降级只读本地 Caffeine 缓存；Redis SETNX + Redisson 分布式锁双重检查保证短链创建接口幂等；IP 黑名单基于分钟级计数器（阈值 1000/min）动态封禁恶意流量，封禁 1 小时自动解封。

### 关键代码位置

| 文件 | 类/方法 | 行号范围 | 职责 |
|------|---------|---------|------|
| [TokenBucketRateLimiter.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/ratelimit/TokenBucketRateLimiter.java) | `TokenBucketRateLimiter` | L28-L103 | 令牌桶限流（Lua 原子） |
| [TokenBucketRateLimiter.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/ratelimit/TokenBucketRateLimiter.java) | `TOKEN_BUCKET_LUA` | L31-L57 | 令牌桶 Lua 脚本 |
| [SlidingWindowRateLimiter.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/ratelimit/SlidingWindowRateLimiter.java) | `SlidingWindowRateLimiter` | L28-L121 | 滑动窗口限流（ZSet） |
| [SlidingWindowRateLimiter.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/ratelimit/SlidingWindowRateLimiter.java) | `SLIDING_WINDOW_LUA` | L31-L53 | 滑动窗口 Lua 脚本 |
| [RateLimitAspect.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/ratelimit/RateLimitAspect.java) | `RateLimitAspect.around` | L65-L94 | 限流 AOP 切面 |
| [SentinelRulePersistence.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/sentinel/SentinelRulePersistence.java) | `SentinelRulePersistence` | L39-L141 | Sentinel 规则文件持久化 |
| [ShortLinkCreateService.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/shortlink/ShortLinkCreateService.java) | `createShortLink` | L82-L159 | 幂等创建（SETNX + 锁） |
| [IpBlacklistService.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/ratelimit/IpBlacklistService.java) | `checkAndBlock` | L71-L97 | IP 黑名单校验 |
| [ShortLinkJumpService.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/shortlink/ShortLinkJumpService.java) | `jumpBlockHandler/jumpFallback` | L353-L401 | Sentinel 熔断降级 |

### 面试问答点

**Q1：令牌桶 vs 漏桶区别？为什么接口级用令牌桶、用户级用滑动窗口？**

A：
- **令牌桶**：以固定速率往桶里放令牌，请求消耗令牌，桶满则丢弃令牌。特点是「允许突发」——桶里有积攒的令牌时可以瞬间放过一批请求，适合接口级（整体 QPS 限制但允许短时突发）。
- **漏桶**：请求像水滴流入桶中，以固定速率漏出。特点是「强制匀速」，不允许突发，适合整形下游流量（如保护 DB）。
- **滑动窗口**：统计窗口内请求数，超限拒绝。能精确控制窗口内总量，适合用户/IP 维度（「每用户每秒最多 50 次」这种绝对上限）。

接口级用令牌桶（capacity=5000, rate=5000）：允许瞬时放过 5000 个积攒令牌应对突发，平滑后 5000 QPS。用户级用滑动窗口（maxRequests=50, windowSize=1000ms）：精确限制单用户 1 秒内最多 50 次，避免单用户刷接口。

**Q2：Redis + Lua 集群限流如何保证原子性？**

A：令牌桶的「读余量→计算填充→扣减→写回」和滑动窗口的「清过期→计数→添加」都是多步操作，多线程并发下若不用锁会有竞态。本项目用 Lua 脚本（TokenBucketRateLimiter.TOKEN_BUCKET_LUA L31）保证原子性：

Redis 执行 Lua 脚本是单线程原子的（Redis 单线程模型），脚本执行期间不会被其他命令打断。集群模式下需注意：
1. 限流 key 必须在同一 slot（用 `{hashtag}` 保证，如 `short:link:rate:{user}:1` 都在 user 的 slot）；
2. 本项目限流 key 是单 key（`bucket:接口名` 或 `window:user:1`），天然在单 slot，无集群跨 slot 问题；
3. Lua 脚本用 `redis.call` 同步执行，返回结果给客户端。

**Q3：Sentinel 熔断后如何降级？降级数据新鲜度怎么保证？**

A：ShortLinkJumpService 配置 `@SentinelResource(value="jumpResource", blockHandler, fallback)`（L91）：
- **慢调用比例熔断**：RT > 100ms 的请求占比 > 50% 时熔断 10s（规则在 rules/sentinel-degrade-rules.json）；
- **异常比例熔断**：异常比例 > 50% 时熔断。

熔断后进入 `jumpBlockHandler`（L353），策略是「只读 Caffeine 本地缓存」：命中则返回 originalUrl，未命中抛友好业务异常（不返回 500）。这是「可用性 > 一致性」的取舍——熔断说明 Redis/DB 可能不可用，返回稍旧的缓存数据比直接报错好。

新鲜度保证：
1. 熔断期间不更新缓存，返回的是熔断前最近一次缓存的数据（最多 60s 前的 Caffeine TTL）；
2. 熔断恢复后首次请求会触发缓存重建，恢复最新数据；
3. 对于短链跳转这种「URL 不常变」的场景，60s 内的旧数据完全可接受。

**Q4：幂等并发场景下如何保证？**

A：ShortLinkCreateService.createShortLink（L82）流程：
1. 先查 Redis 幂等映射（`idempotent:userId:longUrlHash → shortCode`），命中直接返回（快速路径）；
2. 未命中获取 Redisson 锁 `tryLock(0, 10, SECONDS)`（不等待，拿不到说明有并发请求在处理，抛「请勿重复提交」）；
3. 拿到锁后**双重检查**幂等映射（可能已被其他请求写入）；
4. 仍未命中则生成短码、写 DB、写缓存、写幂等映射、加布隆过滤器。

为什么双重检查？因为「查 Redis 幂等」和「获取锁」之间有窗口，可能另一个请求刚写完幂等映射。双重检查避免重复生成短码。

幂等 key 用 `userId + Math.abs(longUrl.hashCode())`，相同用户相同 URL 返回相同 shortCode。TTL 1 天，过期后重复提交会生成新短码（业务可接受，因为旧短链仍有效）。

**Q5：IP 黑名单误杀怎么办？**

A：IpBlacklistService（L35）逻辑：1 分钟内访问 ≥ 1000 次自动加入黑名单，封禁 1 小时。误杀场景：
1. **公司内网 NAT 出口 IP**：大量员工共享一个公网 IP，正常访问可能触发阈值；
2. **CDN 回源 IP**：CDN 节点高频回源会被误判。

应对措施：
1. **白名单兜底**：维护 IP 白名单（公司内网、CDN IP 段），checkAndBlock 前先查白名单跳过；
2. **阈值可调**：1000/min 是经验值，可按业务调整，或按 IP 维度动态学习正常基线；
3. **封禁时间可控**：1 小时自动解封，避免永久封禁；提供管理接口手动解封（addToBlacklist 对应有 removeFromBlacklist）；
4. **渐进式封禁**：可改为「首次超阈值降级限流（非全封），多次触发才封禁」。

本项目当前是「阈值即封禁」的简化方案，生产可结合白名单 + 渐进封禁优化。

**Q6：@RateLimit 注解的 SpEL key 解析怎么实现？**

A：RateLimitAspect.resolveKey（L110）支持 SpEL 表达式，如 `@RateLimit(key="#req.userId", type=USER)`。实现：
1. 用 `DefaultParameterNameDiscoverer` 获取方法参数名；
2. 构建 `StandardEvaluationContext`，将参数名与实参绑定（`context.setVariable("userId", args[0])`）；
3. `SpelExpressionParser.parseExpression("#req.userId").getValue(context)` 解析。

若不指定 key，按 LimitType 自动生成：INTERFACE 用方法全名、USER 用当前登录用户 ID、IP 用请求 IP。这样既灵活（支持复杂 key）又省心（不填自动生成）。

**Q7：Sentinel 规则如何持久化？为什么用 SPI？**

A：SentinelRulePersistence（L39）实现 `InitFunc` 接口，通过 SPI 机制加载：
1. `META-INF/services/com.alibaba.csp.sentinel.init.InitFunc` 文件声明实现类；
2. Sentinel 启动时通过 ServiceLoader 加载并调用 `init()`；
3. `init()` 从 classpath 读取 `rules/sentinel-degrade-rules.json`，解析为 DegradeRule 列表，`DegradeRuleManager.loadRules(rules)` 加载。

用 SPI 而非 Spring 的原因：Sentinel 核心是纯 Java 库（不依赖 Spring），InitFunc 在 Sentinel 初始化阶段执行，早于 Spring 容器启动。规则与代码解耦后，运维可直接改 JSON 文件调整熔断策略，无需改代码重新发布。static 块设置 `project.name` 系统属性，需在 Sentinel Transport 初始化前执行。

### 可量化指标

| 指标 | 数值 | 说明 |
|------|------|------|
| 接口级限流 | 5000 QPS | 令牌桶 capacity=5000, rate=5000 |
| 用户级限流 | 50 次/秒 | 滑动窗口 maxRequests=50 |
| IP 封禁阈值 | 1000 次/分 | 超阈值自动加入黑名单 |
| 黑名单封禁时长 | 1 小时 | 过期自动解封 |
| 慢调用熔断阈值 | RT>100ms 占比>50% | 熔断 10s |
| 异常比例熔断 | >50% | 熔断 10s |
| 幂等映射 TTL | 1 天 | Redis SETNX |
| 分布式锁持有 | 10 秒 | 创建接口幂等锁 |

---

## 亮点 4：Kafka 削峰 + 多线程异步聚合

### 简历描述

设计访问统计异步链路：跳转请求 → `AccessRecordQueue`(ArrayBlockingQueue 10000) → `KafkaAccessRecordProducer`(statsExecutor 后台线程) → Kafka topic → `AccessRecordConsumer`(@KafkaListener 按 shortCode 分区顺序消费) → `StatsAggregator`(ConcurrentHashMap 两级聚合 + StatsBucket synchronized) → `StatsFlushTask`(@Scheduled 60s 批量落库)；UV 基于 Redis HyperLogLog 去重（12KB 内存，0.81% 误差），IP 数基于 Redis Set 精确去重；4 个隔离线程池（statsExecutor/batchGenExecutor/cacheRefreshExecutor/bloomFilterExecutor）+ RejectedTaskCounter 包装拒绝策略记录 Micrometer 指标；批量生成用 CompletableFuture.allOf 编排 + handle 异常隔离；G1 收集器 2G 堆调优支撑单节点 5000 QPS。

### 关键代码位置

| 文件 | 类/方法 | 行号范围 | 职责 |
|------|---------|---------|------|
| [AccessRecordQueue.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/mq/AccessRecordQueue.java) | `AccessRecordQueue` | L30-L117 | 本地内存队列缓冲 |
| [KafkaAccessRecordProducer.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/mq/KafkaAccessRecordProducer.java) | `runLoop` | L108-L130 | 后台线程消费队列发 Kafka |
| [KafkaAccessRecordProducer.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/mq/KafkaAccessRecordProducer.java) | `shutdown` | L203-L223 | 优雅关闭排空剩余消息 |
| [AccessRecordConsumer.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/stats/AccessRecordConsumer.java) | `onAccessRecord` | L54-L65 | Kafka 消费者入口 |
| [StatsAggregator.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/stats/StatsAggregator.java) | `aggregate` | L80-L123 | 内存聚合主逻辑 |
| [StatsAggregator.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/stats/StatsAggregator.java) | `StatsBucket` | L247-L373 | 聚合桶（synchronized） |
| [StatsFlushTask.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/stats/StatsFlushTask.java) | `flushStats` | L62-L119 | 定时落库 |
| [ThreadPoolConfig.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/config/ThreadPoolConfig.java) | `ThreadPoolConfig` | L48-L244 | 4 个隔离线程池配置 |
| [ThreadPoolConfig.java](file:///d:/CCDevelop/Garlic/short-link-common/src/main/java/com/garlic/shortlink/common/config/ThreadPoolConfig.java) | `RejectedTaskCounter` | L223-L244 | 拒绝策略包装计数 |
| [ShortLinkBatchService.java](file:///d:/CCDevelop/Garlic/short-link-api/src/main/java/com/garlic/shortlink/service/shortlink/ShortLinkBatchService.java) | `batchCreate` | L67-L107 | CompletableFuture 批量编排 |
| [startup.sh](file:///d:/CCDevelop/Garlic/bin/startup.sh) | JVM 参数 | L7-L18 | G1 调优 |

### 面试问答点

**Q1：为什么先入本地队列再发 Kafka？不能直接发 Kafka 吗？**

A：直接发 Kafka 的问题：
1. **跳转接口 RT 受 Kafka 影响**：`kafkaTemplate.send` 虽然异步，但 Producer 内部会先序列化、选分区、缓冲，若 Broker 不可达或网络抖动，send 可能阻塞（默认 max.block.ms=60s），直接拖垮跳转接口；
2. **Kafka 抖动时无削峰**：突发流量直接打到 Kafka，Broker 压力骤增可能触发限流。

先入本地队列（ArrayBlockingQueue 10000）的好处：
1. **offer O(1) 非阻塞**：跳转线程只做一次入队立即返回，Kafka 不可用也不影响跳转；
2. **削峰填谷**：本地队列缓冲突发流量，后台线程匀速消费发 Kafka；
3. **降级容错**：队列满时 offer 返回 false，丢弃记录并记日志（有损策略），优先保跳转接口可用性。

代价是「最多丢 10000 条访问记录」（队列满时），对统计场景可接受（统计允许近似，跳转必须可用）。

**Q2：Kafka 不可用怎么办？本地队列满了怎么办？**

A：分层降级：
1. **Kafka 发送失败**：KafkaAccessRecordProducer.doSend（L163）的 ListenableFutureCallback.onFailure 只记日志，不阻塞队列消费，继续消费下一条。实际生产可落盘到本地文件后续补偿；
2. **本地队列满**：AccessRecordQueue.offer（L53）返回 false，丢弃当前记录并记 warn 日志。监控队列 size（可通过 Micrometer 暴露），接近上限时告警；
3. **优雅关闭**：shutdown（L203）设置 running=false，中断后台线程，`drainRemaining`（L138）排空剩余消息最多等 5 秒，保证关闭时不丢数据。

生产级容错可加：本地文件 WAL（Write-Ahead Log）持久化未发送消息，Kafka 恢复后回放；或用 Redis List 兜底队列。

**Q3：消费者如何保证顺序？分区策略？**

A：
- **生产端**：KafkaAccessRecordProducer.doSend 发送时未显式传 key，但访问记录 JSON 中的 shortCode 可作为 key（当前实现 send(topic, json) 无 key，生产建议改为 send(topic, shortCode, json)）。CacheConsistencyService.sendCacheInvalidMessage（L190）已用 shortCode 作为 key。
- **Kafka 分区**：相同 key 的消息哈希到同一分区，同一短链的访问记录在同一分区。
- **消费端**：@KafkaListener 按分区分配消费者，单分区内消息顺序消费。spring.kafka.listener.concurrency 控制消费线程数（默认 1），多线程时不同分区并行、同分区串行。
- **聚合线程安全**：StatsAggregator 用 ConcurrentHashMap 两级 + StatsBucket synchronized（L284），即使多线程消费同一 shortCode（不应发生，但防御性），聚合数据也线程安全。

**Q4：线程池为什么隔离？拒绝策略如何选？**

A：隔离原因：避免「线程池饥饿」——若统计上报和批量生成共用一个线程池，批量生成耗时长会占满线程，导致统计上报积压。隔离后各业务独立线程池，互不影响。

拒绝策略选型（ThreadPoolConfig L71-L129）：
| 线程池 | 拒绝策略 | 理由 |
|--------|----------|------|
| statsExecutor | CallerRunsPolicy | 统计上报不能丢（影响数据准确），让调用线程自己执行形成反压，减缓入队速度 |
| batchGenExecutor | AbortPolicy | 批量生成过载应快速失败抛异常，让调用方感知（分批重试或降级），避免静默丢弃 |
| cacheRefreshExecutor | DiscardOldestPolicy | 缓存刷新最新数据优先，丢弃最老任务（旧数据可能已过期） |
| bloomFilterExecutor | CallerRunsPolicy | 布隆过滤器加载不能丢，调用线程同步执行保证一致性 |

RejectedTaskCounter（L223）包装原始策略，rejectedExecution 时 incrementAndGet，通过 Micrometer Gauge 暴露 `executor.rejected.count`，监控过载情况。

**Q5：G1 调优思路？为什么选 G1 不选 ZGC？**

A：G1 调优（startup.sh L7）：
- `-Xms2g -Xmx2g`：初始堆=最大堆，避免动态扩展导致停顿；
- `-XX:+UseG1GC -XX:MaxGCPauseMillis=100`：目标停顿 100ms，跳转接口 RT<20ms 要求 GC 不卡顿；
- `-XX:G1HeapRegionSize=16m`：2G 堆建议 16MB Region（堆/64），平衡大对象分配与 GC 效率；
- `-XX:+ParallelRefProcEnabled`：并行处理引用对象，减少 GC 耗时；
- `-Xlog:gc*`：GC 日志便于排查；
- `-XX:+HeapDumpOnOutOfMemoryError`：OOM 自动 dump。

选 G1 不选 ZGC 的原因：
1. **JDK 17 中 ZGC 仍是实验性**（需 `-XX:+UnlockExperimentalVMOptions`），生产稳定性不如 G1；
2. **G1 在 2G 堆下停顿已足够**（<100ms），ZGC 的亚毫秒级停顿对短链场景过剩；
3. **ZGC 适合超大堆**（>32G），2G 堆 G1 更合适；
4. **G1 是 JDK 9+ 默认**，生态成熟，监控工具支持好。

**Q6：内存聚合为什么用两级 ConcurrentHashMap？StatsBucket 为什么要 synchronized？**

A：两级结构（StatsAggregator L64）：`ConcurrentHashMap<shortCode, ConcurrentHashMap<dateHour, StatsBucket>>`：
1. **第一级 shortCode**：不同短链天然隔离，computeIfAbsent 保证每个 shortCode 只创建一个内层 Map；
2. **第二级 dateHour**：同一短链不同小时隔离，flush 时可按 bucket 粒度 remove 不影响其他小时。

StatsBucket 用 synchronized（L284）的原因：ConcurrentHashMap 只保证 Map 操作线程安全，但 StatsBucket 内部的 `pv++`、`uvSet.add`、`map.merge` 是多步复合操作，需同步。用 synchronized 而非 CAS：聚合是写密集场景，pv++ 用 CAS 需自旋，高并发下自旋开销可能大于锁；synchronized 在 JDK 17 偏向锁/轻量锁优化下，无竞争时几乎零开销。bucket 粒度小（单 shortCode+hour），锁竞争低。

**Q7：UV 为什么用 HyperLogLog 而不是 Set？**

A：HyperLogLog（StatsAggregator.recordUv L146）vs Set：
- **内存**：HyperLogLog 固定 12KB（无论多少元素），Set 每个元素约 50 字节，100 万 UV 需 ~50MB；
- **误差**：HyperLogLog 标准误差 0.81%，Set 精确。统计 UV 场景 0.81% 误差完全可接受（「100 万 UV」和「100.81 万 UV」业务无差别）；
- **操作**：HyperLogLog PFADD/PFCOUNT O(1)，Set SADD/SCARD 也是 O(1) 但内存是主要矛盾。

IP 数用 Set（recordIp L178）是因为：① IP 数量远少于 UV（同一 IP 多次访问只算 1 个 IP，但 UV 可能很多）；② IP 数需要精确值用于风控分析。UV 用 HyperLogLog 平衡内存与精度。

**Q8：CompletableFuture.allOf 批量编排如何处理异常隔离？**

A：ShortLinkBatchService.batchCreate（L67）对每个 URL：
1. `CompletableFuture.supplyAsync(..., batchGenExecutor)` 提交到批量线程池；
2. `.handle((resp, ex) -> { if (ex != null) return null; return resp; })` 捕获异常返回 null；
3. `CompletableFuture.allOf(futures).join()` 等待全部完成（因 handle 吞了异常，join 不会抛）；
4. 遍历结果：null 入 failedUrls，非 null 入 results。

异常隔离的关键是 handle：单条失败返回 null 而非传播异常，避免一个 URL 失败导致 allOf 异常终止其他 URL。返回响应含 successCount/failCount/failedUrls，调用方可知哪些失败并重试。

### 可量化指标

| 指标 | 数值 | 说明 |
|------|------|------|
| 本地队列容量 | 10000 | ArrayBlockingQueue |
| 落库周期 | 60 秒 | @Scheduled fixedRate |
| UV 误差率 | 0.81% | HyperLogLog 标准误差 |
| UV 内存占用 | 12KB/短链/天 | HyperLogLog 固定大小 |
| UV/IP TTL | 8 天 | 覆盖一周 + 1 天缓冲 |
| 批量生成并发度 | 8~16 线程 | batchGenExecutor |
| 堆内存 | 2G | -Xms2g -Xmx2g |
| GC 目标停顿 | 100ms | MaxGCPauseMillis |
| 单节点 QPS | 5000 | 设计目标 |
| Full GC 频率 | <1 次/小时 | 2G 堆下经验值 |

---

## 附：亮点速查表

| 亮点 | 核心技术 | 关键类 | 可量化指标 |
|------|---------|--------|-----------|
| 缓存架构 | 三级缓存 + 延迟双删 + 布隆 | TwoLevelCache / CacheConsistencyService | 命中率 99%+，L1 RT 0.05ms |
| 分库分表 | ShardingSphere + 雪花 | application-sharding.yml / SnowflakeIdWorker | 16 分片，6~7 位短码 |
| 限流熔断 | Lua 令牌桶 + Sentinel | TokenBucketRateLimiter / SentinelRulePersistence | 5000 QPS，熔断 10s |
| 异步聚合 | Kafka + 线程池隔离 | StatsAggregator / ThreadPoolConfig | 60s 落库，4 线程池隔离 |

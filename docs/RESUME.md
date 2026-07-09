# Garlic 高并发短链接服务 2026年05月 - 2026年07月

**技术栈**：SpringBoot 2.7 + Java 17 + MyBatis-Plus + Redis + Redisson + Caffeine + Kafka + ShardingSphere-JDBC 5.2 + Sentinel

三级缓存架构：设计 Caffeine(L1 本地) → Redis(L2 分布式) → MySQL 三级缓存，通过滑动窗口 + LongAdder 实时探测热点 key 仅回填 L1，避免冷数据撑爆本地缓存；布隆过滤器(1000 万容量，0.1% 误判率)防缓存穿透，Redisson 分布式锁双重检查防缓存击穿，TTL 随机抖动(3600+random(0,300)s)防缓存雪崩，跳转接口缓存命中率达 99%+。

缓存一致性保证：基于 Cache Aside 模式解决数据库与缓存一致性问题。更新短链时采用「延迟双删(500ms) + Kafka 广播 + instanceId 去重」保证多实例 L1 缓存一致，第一次删除后更新数据库，延迟 500ms 覆盖主从复制延迟窗口再二次删除，并通过 Kafka 广播失效消息通知其他实例清理本地 Caffeine；删除失败由 60s TTL 兜底，达到最终一致。

分库分表与短码生成：基于 ShardingSphere-JDBC 5.2 设计分片方案，短链表按 short_code 哈希分 4 库 × 4 表 = 16 分片(INLINE 算法)，访问统计表与短链表绑定同分片键避免笛卡尔积，访问明细表按 access_time 月份 INTERVAL 自动分表便于归档，t_user/t_group 广播表避免跨库 join；雪花算法生成全局唯一 ID + Base62 编码生成 6~7 位短码，含时钟回拨检测，理论容量 62^6 ≈ 568 亿。

多级限流与熔断降级：实现接口级令牌桶(Redis + Lua 原子脚本，5000 QPS)、用户级/IP 级滑动窗口(ZSet，50 次/秒)三层限流，@RateLimit 注解 + AOP 切面支持 SpEL 动态 key 解析；Sentinel 配置慢调用比例(RT>100ms 占比>50%)与异常比例双熔断策略，熔断时降级只读本地 Caffeine 缓存保证可用性；IP 黑名单基于分钟级计数器(1000/min)动态封禁恶意流量，封禁 1 小时自动解封。

幂等创建与批量生成：短链创建接口通过 Redis SETNX + Redisson 分布式锁双重检查保证幂等，避免同一长链重复生成短码；批量生成采用 CompletableFuture.allOf 并发编排 + handle 异常隔离，单条失败不影响其他，配合 batchGen 线程池(8 核心/16 最大)隔离，单批 100 条 URL 耗时接近单条生成。

Kafka 削峰与异步聚合：设计访问统计异步链路，跳转请求 → 本地队列(ArrayBlockingQueue 10000 容量削峰) → Kafka → 消费者内存聚合(ConcurrentHashMap 两级 + StatsBucket synchronized) → @Scheduled 60s 批量落库，将统计写入与跳转主链路解耦；UV 基于 Redis HyperLogLog 去重(12KB 固定内存，0.81% 误差)，IP 数基于 Redis Set 精确去重；4 个隔离线程池(statsExecutor/batchGenExecutor/cacheRefresh/bloom) + RejectedTaskCounter 包装拒绝策略记录 Micrometer 指标，避免线程池饥饿。

监控运维与 JVM 调优：集成 Actuator + Micrometer + Prometheus 暴露 health/metrics/prometheus 端点，自定义线程池拒绝计数、缓存命中率、限流触发次数等业务指标；全局异常处理基于 @RestControllerAdvice + ErrorCode 错误码枚举 + Result<T> 统一响应封装，JSR-303 参数校验兜底；JVM 采用 G1 收集器，固定 2G 堆(-Xms=-Xmx)避免动态扩展，MaxGCPauseMillis=100ms 目标停顿，ParallelRefProcEnabled 并行处理引用，HeapDumpOnOutOfMemoryError 自动 dump，单节点支撑 5000 QPS，P99 < 20ms。

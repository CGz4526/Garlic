# JVM 调优说明

## 1. JVM 参数选择

### 1.1 堆内存配置
- -Xms2g -Xmx2g: 初始堆和最大堆均为 2G，避免堆动态扩展导致停顿
- -Xmn1g: 新生代 1G（约占堆 50%，G1 中由 Region 动态分配，此参数仅参考）

### 1.2 G1 收集器配置
- -XX:+UseG1GC: 使用 G1 收集器（JDK 9+ 默认，适合大堆 + 低延迟场景）
- -XX:MaxGCPauseMillis=100: 目标最大 GC 停顿 100ms
- -XX:G1HeapRegionSize=16m: Region 大小 16MB（堆 2G 时建议 16MB）
- -XX:+ParallelRefProcEnabled: 并行处理引用，减少 GC 耗时

### 1.3 GC 日志与诊断
- -Xlog:gc*:file=logs/gc.log: GC 日志输出到文件
- -XX:+HeapDumpOnOutOfMemoryError: OOM 时自动 dump 堆
- -XX:HeapDumpPath=logs/heapdump.hprof: dump 文件路径

## 2. 调优思路

### 2.1 为什么选 G1?
- 短链接服务跳转接口 RT 要求 < 20ms，CMS 的碎片问题可能导致长停顿
- G1 的 Region 化设计适合多核 + 大堆，可预测停顿时间
- JDK 17 中 G1 已是默认且持续优化

### 2.2 堆大小选择依据
- 跳转接口热点数据在 Caffeine + Redis 缓存，DB 查询少，堆压力主要来自:
  - Kafka 消费者聚合数据（ConcurrentHashMap）
  - 并发请求的短期对象（跳转 DTO、JSON 序列化）
- 2G 堆可支撑单节点 5000 QPS，Full GC 频率 < 1次/小时

### 2.3 监控指标
- /actuator/metrics/jvm.gc.pause: GC 停顿时间
- /actuator/metrics/jvm.gc.live.data.size: 存活数据大小
- /actuator/metrics/jvm.threads.live: 活跃线程数
- /actuator/threaddump: 线程转储（排查死锁）
- Prometheus + Grafana 可视化监控

## 3. 线程池配置

| 线程池 | 核心/最大 | 队列 | 拒绝策略 | 用途 |
|--------|-----------|------|----------|------|
| statsExecutor | 4/8 | 1000 | CallerRunsPolicy | 统计异步上报（反压保护） |
| batchGenExecutor | 8/16 | 500 | AbortPolicy | 批量生成（过载快速失败） |
| cacheRefreshExecutor | 2/4 | 200 | DiscardOldestPolicy | 缓存刷新（保新弃旧） |
| bloomFilterExecutor | 1/2 | 100 | CallerRunsPolicy | 布隆过滤器加载 |

### 3.1 线程池隔离原则
- 不同业务用不同线程池，避免互相影响
- 统计任务用 CallerRunsPolicy 反压（队列满时降速）
- 批量生成用 AbortPolicy 快速失败（让调用方感知过载）
- 缓存刷新用 DiscardOldestPolicy（新数据更重要）

## 4. 常见问题排查

### 4.1 GC 频繁
- 检查 jvm.gc.pause 指标
- 分析 gc.log，看是 Young GC 还是 Mixed GC
- 可能原因: 缓存对象过大、Kafka 聚合数据堆积

### 4.2 OOM
- 查看 logs/heapdump.hprof（用 MAT 或 jvisualvm 分析）
- 可能原因: 队列堆积、内存泄漏

### 4.3 线程阻塞
- /actuator/threaddump 查看线程状态
- 关注 BLOCKED/WAITING 线程

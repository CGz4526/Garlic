package com.garlic.shortlink.common.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 定制化线程池配置：按业务场景隔离线程资源 + Micrometer 指标暴露。
 *
 * <p>设计目的：避免不同业务互相影响（如批量生成耗时长拖垮统计上报），
 * 每个业务拥有独立线程池、独立队列、独立拒绝策略，互不干扰、独立监控。</p>
 *
 * <p>线程池清单：</p>
 * <ul>
 *   <li>{@code statsExecutor}：统计异步上报（核心 4 / 最大 8 / 队列 1000，CallerRunsPolicy 反压）；</li>
 *   <li>{@code batchGenExecutor}：批量短链生成（核心 8 / 最大 16 / 队列 500，AbortPolicy 抛异常）；</li>
 *   <li>{@code cacheRefreshExecutor}：缓存异步刷新（核心 2 / 最大 4 / 队列 200，DiscardOldestPolicy 丢旧）；</li>
 *   <li>{@code bloomFilterExecutor}：布隆过滤器异步加载（核心 1 / 最大 2 / 队列 100，CallerRunsPolicy 同步）。</li>
 * </ul>
 *
 * <p>暴露 Micrometer 指标（按 {@code name} 标签区分线程池）：</p>
 * <ul>
 *   <li>{@code executor.active.count}：活跃线程数；</li>
 *   <li>{@code executor.queue.size}：队列任务数；</li>
 *   <li>{@code executor.completed.count}：累计完成任务数；</li>
 *   <li>{@code executor.rejected.count}：累计拒绝任务数（通过包装拒绝策略统计）。</li>
 * </ul>
 *
 * @author garlic
 */
@Slf4j
@Configuration
public class ThreadPoolConfig {

    /** 空闲线程存活时间（秒） */
    private static final long KEEP_ALIVE_SECONDS = 60L;

    /** MeterRegistry 可选注入：在非 Actuator 环境下也能启动 */
    @Autowired(required = false)
    private MeterRegistry meterRegistry;

    /** 创建的线程池引用（@PostConstruct 注册指标时使用） */
    private final Map<String, ThreadPoolExecutor> executors = new LinkedHashMap<>();

    /** 每个线程池对应的拒绝计数器 */
    private final Map<String, RejectedTaskCounter> rejectedCounters = new LinkedHashMap<>();

    /**
     * 统计异步上报线程池。
     *
     * <p>用途：访问记录异步上报 Kafka、统计聚合等。</p>
     * <p>策略：CallerRunsPolicy 反压，让调用线程执行，保护系统不被淹没。</p>
     *
     * @return statsExecutor 线程池
     */
    @Bean(name = "statsExecutor")
    public ThreadPoolExecutor statsExecutor() {
        return createExecutor(
                "stats-async",
                "stats-async-",
                4, 8, 1000,
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * 批量短链生成线程池。
     *
     * <p>用途：批量创建短链 CompletableFuture 编排。</p>
     * <p>策略：AbortPolicy 拒绝并抛异常，让调用方感知过载，自行处理（如分批重试、降级）。</p>
     *
     * @return batchGenExecutor 线程池
     */
    @Bean(name = "batchGenExecutor")
    public ThreadPoolExecutor batchGenExecutor() {
        return createExecutor(
                "batch-gen",
                "batch-gen-",
                8, 16, 500,
                new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * 缓存异步刷新线程池。
     *
     * <p>用途：缓存击穿后的异步回源、热点 key 主动刷新。</p>
     * <p>策略：DiscardOldestPolicy 丢弃最老任务，保新任务（最新数据优先）。</p>
     *
     * @return cacheRefreshExecutor 线程池
     */
    @Bean(name = "cacheRefreshExecutor")
    public ThreadPoolExecutor cacheRefreshExecutor() {
        return createExecutor(
                "cache-refresh",
                "cache-refresh-",
                2, 4, 200,
                new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    /**
     * 布隆过滤器异步加载线程池。
     *
     * <p>用途：启动时布隆过滤器预热、增量加载。</p>
     * <p>策略：CallerRunsPolicy 同步执行，保证一致性。</p>
     *
     * @return bloomFilterExecutor 线程池
     */
    @Bean(name = "bloomFilterExecutor")
    public ThreadPoolExecutor bloomFilterExecutor() {
        return createExecutor(
                "bloom-filter",
                "bloom-filter-",
                1, 2, 100,
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * 创建 ThreadPoolExecutor：显式构造（不使用 Executors），包装拒绝策略以统计拒绝次数。
     *
     * @param name          指标 tag 名称（如 stats-async）
     * @param namePrefix    线程名前缀（如 stats-async-）
     * @param core          核心线程数
     * @param max           最大线程数
     * @param queueCapacity 队列容量
     * @param delegate      原始拒绝策略
     * @return ThreadPoolExecutor 实例
     */
    private ThreadPoolExecutor createExecutor(String name,
                                              String namePrefix,
                                              int core, int max, int queueCapacity,
                                              RejectedExecutionHandler delegate) {
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(queueCapacity);
        ThreadFactory threadFactory = createThreadFactory(namePrefix);
        RejectedTaskCounter rejectedCounter = new RejectedTaskCounter(delegate);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                core, max, KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
                workQueue, threadFactory, rejectedCounter);
        executor.allowCoreThreadTimeOut(true);

        executors.put(name, executor);
        rejectedCounters.put(name, rejectedCounter);
        log.info("线程池已创建：name={}, core={}, max={}, queue={}, prefix={}",
                name, core, max, queueCapacity, namePrefix);
        return executor;
    }

    /**
     * 自定义 ThreadFactory：设置线程名前缀 + 守护线程。
     *
     * <p>守护线程：JVM 退出时自动终止，不阻塞进程关闭。</p>
     *
     * @param namePrefix 线程名前缀
     * @return ThreadFactory 实例
     */
    private ThreadFactory createThreadFactory(String namePrefix) {
        AtomicInteger counter = new AtomicInteger(0);
        return r -> {
            Thread t = new Thread(r, namePrefix + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
    }

    /**
     * 注册 Micrometer 指标：所有创建的线程池。
     *
     * <p>在所有 Bean 创建完成后注册，确保 {@link MeterRegistry} 已注入。</p>
     */
    @PostConstruct
    public void registerMetrics() {
        if (meterRegistry == null) {
            log.warn("MeterRegistry 未注入（非 Actuator 环境），跳过线程池指标注册");
            return;
        }
        executors.forEach((name, executor) -> {
            RejectedTaskCounter counter = rejectedCounters.get(name);
            // 活跃线程数
            Gauge.builder("executor.active.count", executor, ThreadPoolExecutor::getActiveCount)
                    .tag("name", name)
                    .description("线程池活跃线程数")
                    .register(meterRegistry);
            // 队列任务数
            Gauge.builder("executor.queue.size", executor, e -> e.getQueue().size())
                    .tag("name", name)
                    .description("线程池队列任务数")
                    .register(meterRegistry);
            // 累计完成任务数
            Gauge.builder("executor.completed.count", executor, ThreadPoolExecutor::getCompletedTaskCount)
                    .tag("name", name)
                    .description("线程池累计完成任务数")
                    .register(meterRegistry);
            // 累计拒绝任务数（自定义包装策略统计）
            Gauge.builder("executor.rejected.count", counter, RejectedTaskCounter::getCount)
                    .tag("name", name)
                    .description("线程池累计拒绝任务数")
                    .register(meterRegistry);
            log.info("线程池指标已注册：{}", name);
        });
    }

    /**
     * 拒绝任务计数器：包装原始拒绝策略，并统计拒绝次数。
     *
     * <p>委托模式：在不改变原拒绝策略行为的前提下，附加计数功能，
     * 便于通过 Micrometer 暴露 {@code executor.rejected.count} 指标，
     * 监控线程池过载情况并及时告警。</p>
     */
    public static class RejectedTaskCounter implements RejectedExecutionHandler {

        /** 被包装的原始拒绝策略 */
        private final RejectedExecutionHandler delegate;

        /** 累计拒绝任务数 */
        private final AtomicLong rejectedCount = new AtomicLong(0);

        public RejectedTaskCounter(RejectedExecutionHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            rejectedCount.incrementAndGet();
            delegate.rejectedExecution(r, executor);
        }

        public long getCount() {
            return rejectedCount.get();
        }
    }
}

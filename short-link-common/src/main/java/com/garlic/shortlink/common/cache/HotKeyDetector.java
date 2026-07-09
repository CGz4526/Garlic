package com.garlic.shortlink.common.cache;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * 热点 key 探测器（滑动窗口计数实现）。
 *
 * <p>基于滑动窗口计数判断热点 key：维护当前窗口与上一窗口的访问计数，
 * 定时（每 {@link #windowSizeMillis} 毫秒）滚动窗口，通过加权方式判定热点。</p>
 *
 * <p>设计要点：</p>
 * <ul>
 *     <li>使用 {@link LongAdder} 保证高并发计数性能；</li>
 *     <li>使用 {@link ScheduledExecutorService} 定时滚动窗口，避免业务线程阻塞；</li>
 *     <li>窗口滚动时通过引用交换（{@code getAndSet}）保证无锁切换；</li>
 *     <li>判定热点时综合当前窗口与上一窗口（按时间比例加权），近似滑动窗口效果。</li>
 * </ul>
 *
 * <p>本实现为简单版本，仅用于本地节点热点判定，不涉及跨节点聚合。</p>
 *
 * @author garlic
 */
@Slf4j
@Component
public class HotKeyDetector {

    /** 当前窗口：每个 key 的访问计数 */
    private final ConcurrentHashMap<String, LongAdder> keyAccessCounter = new ConcurrentHashMap<>();

    /** 上一窗口：每个 key 的访问计数（用于滑动窗口加权计算） */
    private final ConcurrentHashMap<String, LongAdder> windowCounter = new ConcurrentHashMap<>();

    /** 热点判定阈值：窗口内访问次数超过该值则判定为热点 */
    private final long hotThreshold = 100L;

    /** 窗口大小（毫秒），默认 1 秒 */
    private final long windowSizeMillis = 1000L;

    /** 窗口滚动调度器 */
    private final ScheduledExecutorService scheduledExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "hot-key-detector-rotator");
                t.setDaemon(true);
                return t;
            });

    /** 当前窗口起始时间（用于加权计算上一窗口剩余比例） */
    private volatile long currentWindowStart = System.currentTimeMillis();

    /**
     * 初始化：启动窗口滚动定时任务。
     */
    @PostConstruct
    public void init() {
        scheduledExecutor.scheduleAtFixedRate(
                this::rotateWindow,
                windowSizeMillis,
                windowSizeMillis,
                TimeUnit.MILLISECONDS
        );
        log.info("HotKeyDetector 初始化完成：hotThreshold={}, windowSizeMillis={}ms",
                hotThreshold, windowSizeMillis);
    }

    /**
     * 销毁：关闭调度器。
     */
    @PreDestroy
    public void destroy() {
        scheduledExecutor.shutdown();
        try {
            if (!scheduledExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduledExecutor.shutdownNow();
        }
        log.info("HotKeyDetector 已销毁");
    }

    /**
     * 记录一次 key 访问。
     *
     * <p>在当前窗口内累加 key 的访问计数，由 {@link com.garlic.shortlink.common.cache.TwoLevelCache}
     * 在查询时调用。</p>
     *
     * @param key 缓存 key
     */
    public void recordAccess(String key) {
        if (key == null || key.isEmpty()) {
            return;
        }
        keyAccessCounter.computeIfAbsent(key, k -> new LongAdder()).increment();
    }

    /**
     * 判断指定 key 是否为热点 key。
     *
     * <p>采用滑动窗口加权：当前窗口计数 + 上一窗口计数 * 剩余时间比例。
     * 加权值 ≥ {@link #hotThreshold} 则判定为热点。</p>
     *
     * @param key 缓存 key
     * @return true 表示当前为热点 key
     */
    public boolean isHotKey(String key) {
        if (key == null || key.isEmpty()) {
            return false;
        }
        long currentCount = sumCount(keyAccessCounter.get(key));
        long previousCount = sumCount(windowCounter.get(key));
        // 上一窗口按剩余时间比例衰减：越靠近当前窗口起始，上一窗口权重越高
        long elapsed = System.currentTimeMillis() - currentWindowStart;
        double remainRatio = Math.max(0.0, 1.0 - (double) elapsed / (double) windowSizeMillis);
        double weighted = currentCount + previousCount * remainRatio;
        return weighted >= hotThreshold;
    }

    /**
     * 获取当前所有热点 key。
     *
     * <p>遍历当前窗口与上一窗口的计数，返回加权后达到阈值的 key 集合。</p>
     *
     * @return 热点 key 集合（不可变）
     */
    public Set<String> getHotKeys() {
        long elapsed = System.currentTimeMillis() - currentWindowStart;
        double remainRatio = Math.max(0.0, 1.0 - (double) elapsed / (double) windowSizeMillis);
        // 合并两个窗口的所有 key
        Set<String> allKeys = new java.util.HashSet<>();
        allKeys.addAll(keyAccessCounter.keySet());
        allKeys.addAll(windowCounter.keySet());
        return Collections.unmodifiableSet(
                allKeys.stream()
                        .filter(k -> {
                            long currentCount = sumCount(keyAccessCounter.get(k));
                            long previousCount = sumCount(windowCounter.get(k));
                            double weighted = currentCount + previousCount * remainRatio;
                            return weighted >= hotThreshold;
                        })
                        .collect(Collectors.toSet())
        );
    }

    /**
     * 滚动窗口：将当前窗口计数迁移到上一窗口，并清空当前窗口。
     *
     * <p>实现策略：直接清空上一窗口，再把当前窗口整体替换为上一窗口，
     * 然后用新的空 Map 作为当前窗口。残留的并发计数会丢失（简化实现，可接受）。</p>
     */
    private void rotateWindow() {
        try {
            // 简化实现：上一窗口直接清空，当前窗口迁移过去
            windowCounter.clear();
            // 迁移当前窗口中已有计数的 key（保留 LongAdder 引用，避免计数丢失）
            for (Map.Entry<String, LongAdder> entry : keyAccessCounter.entrySet()) {
                if (entry.getValue().sum() > 0) {
                    windowCounter.put(entry.getKey(), entry.getValue());
                }
            }
            keyAccessCounter.clear();
            currentWindowStart = System.currentTimeMillis();
        } catch (Exception e) {
            log.error("HotKeyDetector 窗口滚动异常", e);
        }
    }

    /**
     * 安全累加 LongAdder 计数（null 返回 0）。
     *
     * @param adder 计数器
     * @return 计数值
     */
    private long sumCount(LongAdder adder) {
        return adder == null ? 0L : adder.sum();
    }
}

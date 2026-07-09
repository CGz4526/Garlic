package com.garlic.shortlink.common.bloom;

import com.garlic.shortlink.common.constant.ShortLinkConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

/**
 * 双布隆过滤器扩容方案（简化版）。
 *
 * <p>当主布隆过滤器容量接近上限时，自动创建扩容过滤器（容量翻倍），
 * 查询时叠加主 + 扩容两个过滤器，写入时同步写入，避免单过滤器过载导致误判率上升。</p>
 *
 * <p>设计思路：</p>
 * <ul>
 *     <li>主过滤器容量使用率超过 {@link #capacityThreshold}（默认 80%）时触发扩容；</li>
 *     <li>扩容过滤器容量 = 主过滤器容量 × 2，误判率沿用主过滤器；</li>
 *     <li>扩容期间写入同时落入主与扩容过滤器，保证数据一致；</li>
 *     <li>查询时先查主过滤器，未命中再查扩容过滤器；</li>
 *     <li>迁移完成后切换主从（本简化版仅给出注释说明）。</li>
 * </ul>
 *
 * <p><b>注意</b>：这是简化版实现，主要展示设计思路。实际 {@code contains}/{@code add}
 * 委托给 {@link BloomFilterService}，扩容时叠加 {@code expansionFilter}。
 * 实际生产可用 Canal 监听 binlog 或定时任务完成迁移切换。</p>
 *
 * @author garlic
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ScalableBloomFilter {

    /** 注入 Redisson 客户端（用于创建扩容过滤器） */
    private final RedissonClient redissonClient;

    /** 主过滤器（委托 {@link BloomFilterService}） */
    private final BloomFilterService mainFilter;

    /** 扩容过滤器（初始为 null，触发扩容时创建） */
    private volatile RBloomFilter<String> expansionFilter;

    /** 容量使用率阈值：达到 80% 触发扩容 */
    private double capacityThreshold = 0.8;

    /**
     * 检查主过滤器容量使用率，必要时触发扩容。
     *
     * <p>判定条件：{@code count / expectedInsertions > capacityThreshold}。
     * 扩容时创建扩容过滤器（容量翻倍），名称为 {@code short_link:bloom:expansion}。</p>
     */
    public void checkAndExpand() {
        long count = mainFilter.count();
        long expected = mainFilter.getExpectedInsertions();
        double usage = expected == 0L ? 0.0 : (double) count / (double) expected;
        if (usage <= capacityThreshold) {
            // 未达阈值，无需扩容
            return;
        }
        if (expansionFilter != null) {
            // 已存在扩容过滤器，不重复创建
            log.info("布隆过滤器扩容已存在，跳过：usage={}, expansionFilter != null", usage);
            return;
        }
        // 创建扩容过滤器（容量翻倍）
        String expansionName = ShortLinkConstants.BLOOM_FILTER_NAME + ":expansion";
        expansionFilter = redissonClient.getBloomFilter(expansionName);
        long expansionExpected = mainFilter.getExpectedInsertions() * 2L;
        // 误判率沿用主过滤器（0.001）
        boolean initialized = expansionFilter.tryInit(expansionExpected, 0.001);
        log.warn("布隆过滤器触发扩容：mainCount={}, expected={}, usage={}, expansionName={}, expansionExpected={}, initialized={}",
                count, expected, usage, expansionName, expansionExpected, initialized);
    }

    /**
     * 判断元素是否可能存在（主 + 扩容）。
     *
     * <p>查询顺序：</p>
     * <ol>
     *     <li>先查主过滤器，命中直接返回 true；</li>
     *     <li>主未命中，若存在扩容过滤器，再查扩容过滤器。</li>
     * </ol>
     *
     * @param key 元素 key
     * @return true 表示可能存在，false 表示一定不存在
     */
    public boolean contains(String key) {
        if (key == null || key.isEmpty()) {
            return false;
        }
        // 先查主过滤器，命中返回 true
        if (mainFilter.contains(key)) {
            return true;
        }
        // 主未命中，若存在扩容过滤器，查扩容过滤器
        RBloomFilter<String> expansion = this.expansionFilter;
        if (expansion != null) {
            return expansion.contains(key);
        }
        return false;
    }

    /**
     * 添加元素（扩容期间同时写入主与扩容过滤器）。
     *
     * <p>策略：</p>
     * <ul>
     *     <li>若 {@code expansionFilter != null}（扩容中），同时写入主和扩容过滤器；</li>
     *     <li>否则只写主过滤器。</li>
     * </ul>
     *
     * @param key 元素 key
     */
    public void add(String key) {
        if (key == null || key.isEmpty()) {
            return;
        }
        // 写主过滤器
        mainFilter.add(key);
        // 扩容中：同步写入扩容过滤器
        RBloomFilter<String> expansion = this.expansionFilter;
        if (expansion != null) {
            expansion.add(key);
        }
    }

    /**
     * 将主过滤器数据迁移到扩容过滤器（异步耗时操作）。
     *
     * <p>迁移完成后切换主从：将 expansionFilter 提升为主过滤器，清理旧主过滤器。
     * 本简化版仅给出注释说明，未实际执行迁移逻辑。</p>
     *
     * <p><b>实际生产方案</b>：</p>
     * <ol>
     *     <li>由于 {@link RBloomFilter} 本身不支持遍历，需借助外部数据源（如 DB）读取全量 shortCode；</li>
     *     <li>用 Canal 监听 binlog 或定时任务异步将数据写入扩容过滤器；</li>
     *     <li>迁移完成后切换：expansionFilter 提升为主过滤器，旧主过滤器清理；</li>
     *     <li>切换期间双写保证不丢数据。</li>
     * </ol>
     */
    public void migrate() {
        // TODO 实际生产可用 Canal 监听 binlog 或定时任务迁移
        // 1. 借助 DB 全量读取 shortCode（RBloomFilter 本身不支持遍历）
        // 2. 异步写入扩容过滤器
        // 3. 迁移完成后切换：expansionFilter 提升为主过滤器，旧主过滤器清理
        // 4. 切换期间双写保证不丢数据
        log.info("布隆过滤器迁移方法被调用（简化版占位，未实际执行迁移）");
    }
}

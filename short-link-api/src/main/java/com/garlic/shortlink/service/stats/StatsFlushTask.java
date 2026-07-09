package com.garlic.shortlink.service.stats;

import com.garlic.shortlink.dao.entity.LinkAccessStatsDO;
import com.garlic.shortlink.dao.mapper.LinkAccessStatsMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 访问统计定时落库任务。
 *
 * <p>每分钟将 {@link StatsAggregator} 内存中聚合的 PV/UV/IP/维度分布数据
 * 批量落库到 t_link_access_stats（按 short_code 哈希分表），并清空已 flush 的聚合桶。</p>
 *
 * <p>设计要点：</p>
 * <ul>
 *   <li>每分钟执行一次（{@code fixedRate = 60000}），平衡实时性与 DB 压力；</li>
 *   <li>遍历聚合 Map 时先 remove bucket 再落库，避免 flush 期间新数据写入旧桶导致丢数据；</li>
 *   <li>UV 从 Redis HyperLogLog 获取（PFCOUNT），IP 数从 Redis Set 获取（SCARD），
 *       保证跨消费实例的全局去重；</li>
 *   <li>每个 shortCode + date + hour 只存一条汇总记录（locale/browser/os/device/network
 *       取分布中最大的一个），简化处理；</li>
 *   <li>ShardingSphere 按 short_code 哈希分表，自动路由到 t_link_access_stats_0~3。</li>
 * </ul>
 *
 * @author garlic
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatsFlushTask {

    /** 注入内存聚合器（获取聚合数据） */
    private final StatsAggregator statsAggregator;

    /** 注入访问统计 Mapper（落库汇总记录，按 short_code 哈希分表） */
    private final LinkAccessStatsMapper linkAccessStatsMapper;

    /** 日期格式化器（解析聚合 Map 中的 dateHourKey） */
    private static final DateTimeFormatter YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * 定时 flush 聚合数据到 DB（每分钟执行一次）。
     *
     * <p>流程：</p>
     * <ol>
     *   <li>获取聚合 Map；</li>
     *   <li>遍历每个 shortCode 的每个 StatsBucket；</li>
     *   <li>先从 Map 中 remove bucket（避免 flush 期间新数据写入旧桶）；</li>
     *   <li>构建 LinkAccessStatsDO（uv/ipCount 从 Redis 获取）；</li>
     *   <li>insert 到 t_link_access_stats（ShardingSphere 自动路由分表）；</li>
     *   <li>记录 flush 数量日志。</li>
     * </ol>
     */
    @Scheduled(fixedRate = 60000)
    public void flushStats() {
        Map<String, ConcurrentHashMap<String, StatsAggregator.StatsBucket>> aggregateMap =
                statsAggregator.getAggregateMap();
        if (aggregateMap.isEmpty()) {
            log.debug("聚合 Map 为空，本次 flush 跳过");
            return;
        }

        int flushCount = 0;
        // 遍历每个 shortCode
        for (Map.Entry<String, ConcurrentHashMap<String, StatsAggregator.StatsBucket>> codeEntry : aggregateMap.entrySet()) {
            String shortCode = codeEntry.getKey();
            ConcurrentHashMap<String, StatsAggregator.StatsBucket> codeMap = codeEntry.getValue();

            // 遍历该 shortCode 下每个 dateHour 的 bucket
            Iterator<Map.Entry<String, StatsAggregator.StatsBucket>> bucketIt = codeMap.entrySet().iterator();
            while (bucketIt.hasNext()) {
                Map.Entry<String, StatsAggregator.StatsBucket> bucketEntry = bucketIt.next();
                String dateHourKey = bucketEntry.getKey();
                StatsAggregator.StatsBucket bucket = bucketEntry.getValue();

                // 先 remove bucket，避免 flush 期间新数据写入旧桶（新数据会创建新 bucket，下次 flush 处理）
                bucketIt.remove();

                try {
                    // 解析 dateHourKey（格式 yyyyMMdd:hour）
                    String[] parts = dateHourKey.split(":");
                    LocalDate statsDate = LocalDate.parse(parts[0], YYYYMMDD);
                    int hour = Integer.parseInt(parts[1]);

                    // 构建 LinkAccessStatsDO
                    LinkAccessStatsDO statsDO = new LinkAccessStatsDO();
                    statsDO.setShortCode(shortCode);
                    statsDO.setStatsDate(statsDate);
                    statsDO.setHour(hour);
                    statsDO.setPv(bucket.getPv());
                    // UV 从 Redis HyperLogLog 获取（跨实例全局去重）
                    statsDO.setUv((int) statsAggregator.getUv(shortCode, statsDate));
                    // IP 数从 Redis Set 获取（跨实例全局去重）
                    statsDO.setIpCount((int) statsAggregator.getIpCount(shortCode, statsDate));
                    // 维度字段取分布中最大的一个
                    statsDO.setLocale(bucket.topLocale());
                    statsDO.setBrowser(bucket.topBrowser());
                    statsDO.setOs(bucket.topOs());
                    statsDO.setDevice(bucket.topDevice());
                    statsDO.setNetwork(bucket.topNetwork());

                    // 落库（ShardingSphere 按 short_code 哈希分表自动路由）
                    linkAccessStatsMapper.insert(statsDO);
                    flushCount++;
                } catch (Exception e) {
                    log.error("flush 统计数据失败：shortCode={}, dateHourKey={}", shortCode, dateHourKey, e);
                }
            }
        }
        log.info("定时统计聚合 flush 完成，共写入 {} 条汇总记录", flushCount);
    }
}

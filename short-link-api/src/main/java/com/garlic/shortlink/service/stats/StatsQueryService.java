package com.garlic.shortlink.service.stats;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.garlic.shortlink.common.constant.ShortLinkConstants;
import com.garlic.shortlink.common.exception.BizException;
import com.garlic.shortlink.common.exception.ErrorCode;
import com.garlic.shortlink.dao.entity.LinkAccessStatsDO;
import com.garlic.shortlink.dao.entity.ShortLinkDO;
import com.garlic.shortlink.dao.mapper.LinkAccessStatsMapper;
import com.garlic.shortlink.dao.mapper.ShortLinkMapper;
import com.garlic.shortlink.dto.stats.StatsDistributionRespDTO;
import com.garlic.shortlink.dto.stats.StatsOverviewRespDTO;
import com.garlic.shortlink.dto.stats.StatsTrendRespDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 统计查询 Service。
 *
 * <p>提供短链访问统计的总览、趋势、维度分布、小时分布查询能力。</p>
 *
 * <p>ShardingSphere 路由说明：</p>
 * <ul>
 *   <li>t_link_access_stats 与 t_short_link 均按 short_code 哈希分表；</li>
 *   <li>所有查询均带 short_code 条件，触发精确路由（单分片查询），避免广播；</li>
 *   <li>实际生产环境统计查询可走从库或缓存，避免影响写库性能。</li>
 * </ul>
 *
 * <p>优化建议（暂不实现）：</p>
 * <ul>
 *   <li>总览数据可加 Redis 缓存（key = "short:link:stats:overview:" + shortCode，TTL 1 分钟）；</li>
 *   <li>趋势 / 分布数据可加 Redis 缓存（TTL 5 分钟），按 shortCode + 维度 + days 分桶；</li>
 *   <li>高频查询可走从库读写分离，减轻主库压力。</li>
 * </ul>
 *
 * @author garlic
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StatsQueryService {

    /** 注入访问统计 Mapper（按 short_code 哈希分表） */
    private final LinkAccessStatsMapper linkAccessStatsMapper;

    /** 注入短链 Mapper（查询 totalPv / totalUv） */
    private final ShortLinkMapper shortLinkMapper;

    /** 注入 StringRedisTemplate（从 Redis Set 获取当日 IP 数） */
    private final StringRedisTemplate stringRedisTemplate;

    /** 日期格式化器（yyyyMMdd，用于拼装 Redis IP Set key） */
    private static final DateTimeFormatter YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");

    /** 支持的分布维度 */
    private static final List<String> SUPPORTED_DIMENSIONS =
            List.of("locale", "browser", "os", "device", "network");

    // ==================== 总览查询 ====================

    /**
     * 查询短链统计总览。
     *
     * <p>流程：</p>
     * <ol>
     *   <li>查 ShortLinkDO 获取累计 totalPv / totalUv（按 shortCode 精确路由）；</li>
     *   <li>查今天的统计记录，内存求和得到 todayPv / todayUv；</li>
     *   <li>从 Redis Set SCARD 获取当日独立 IP 数；</li>
     *   <li>构建 DTO 返回。</li>
     * </ol>
     *
     * <p>注：ShortLinkDO 未维护 totalIpCount 字段，此处用当日 Redis Set 的 SCARD 值
     * 填充 totalIpCount（Redis IP Set 按天分桶，TTL 8 天，仅保留近期数据）。</p>
     *
     * @param shortCode 短码
     * @return 总览统计 DTO
     */
    public StatsOverviewRespDTO getOverview(String shortCode) {
        // 1. 查 ShortLinkDO 获取累计 totalPv / totalUv（ShardingSphere 按 shortCode 精确路由）
        ShortLinkDO shortLinkDO = shortLinkMapper.selectOne(
                new LambdaQueryWrapper<ShortLinkDO>()
                        .eq(ShortLinkDO::getShortCode, shortCode)
                        .eq(ShortLinkDO::getDelFlag, 0)
        );
        if (shortLinkDO == null) {
            log.warn("统计总览查询短链不存在：shortCode={}", shortCode);
            throw new BizException(ErrorCode.SHORT_LINK_NOT_FOUND);
        }

        // 2. 查今天的统计记录（简化：查当天所有记录在内存求和）
        LocalDate today = LocalDate.now();
        List<LinkAccessStatsDO> todayStatsList = linkAccessStatsMapper.selectList(
                new LambdaQueryWrapper<LinkAccessStatsDO>()
                        .eq(LinkAccessStatsDO::getShortCode, shortCode)
                        .eq(LinkAccessStatsDO::getStatsDate, today)
        );
        long todayPv = todayStatsList.stream()
                .mapToInt(LinkAccessStatsDO::getPv)
                .filter(java.util.Objects::nonNull)
                .sum();
        long todayUv = todayStatsList.stream()
                .mapToInt(LinkAccessStatsDO::getUv)
                .filter(java.util.Objects::nonNull)
                .sum();

        // 3. 从 Redis Set 获取当日独立 IP 数（SCARD）
        long todayIpCount = getIpCountFromRedis(shortCode, today);

        // 4. 构建 DTO 返回
        StatsOverviewRespDTO resp = new StatsOverviewRespDTO();
        resp.setShortCode(shortCode);
        resp.setTotalPv(shortLinkDO.getTotalPv() == null ? 0L : shortLinkDO.getTotalPv());
        resp.setTotalUv(shortLinkDO.getTotalUv() == null ? 0L : shortLinkDO.getTotalUv());
        // ShortLinkDO 未维护 totalIpCount，此处用当日 Redis Set 的 SCARD 值填充
        resp.setTotalIpCount(todayIpCount);
        resp.setTodayPv(todayPv);
        resp.setTodayUv(todayUv);
        return resp;
    }

    // ==================== 趋势查询 ====================

    /**
     * 查询最近 N 天的访问趋势。
     *
     * <p>流程：</p>
     * <ol>
     *   <li>查最近 N 天的统计记录（stats_date &gt;= today - N）；</li>
     *   <li>在内存中按 statsDate 分组，求和 pv / uv / ipCount；</li>
     *   <li>返回按日期升序排列的列表。</li>
     * </ol>
     *
     * @param shortCode 短码
     * @param days      最近天数（如 7）
     * @return 趋势列表（按日期升序）
     */
    public List<StatsTrendRespDTO> getTrend(String shortCode, int days) {
        LocalDate startDate = LocalDate.now().minusDays(days);

        // 查最近 N 天的统计记录（ShardingSphere 按 shortCode 精确路由）
        List<LinkAccessStatsDO> statsList = linkAccessStatsMapper.selectList(
                new LambdaQueryWrapper<LinkAccessStatsDO>()
                        .eq(LinkAccessStatsDO::getShortCode, shortCode)
                        .ge(LinkAccessStatsDO::getStatsDate, startDate)
        );

        // 内存中按 statsDate 分组求和
        Map<LocalDate, int[]> grouped = new HashMap<>();
        for (LinkAccessStatsDO stats : statsList) {
            LocalDate date = stats.getStatsDate();
            if (date == null) {
                continue;
            }
            int[] sum = grouped.computeIfAbsent(date, k -> new int[3]);
            sum[0] += stats.getPv() == null ? 0 : stats.getPv();
            sum[1] += stats.getUv() == null ? 0 : stats.getUv();
            sum[2] += stats.getIpCount() == null ? 0 : stats.getIpCount();
        }

        // 转换为 DTO 并按日期升序排序
        return grouped.entrySet().stream()
                .map(entry -> {
                    StatsTrendRespDTO dto = new StatsTrendRespDTO();
                    dto.setStatsDate(entry.getKey());
                    dto.setPv(entry.getValue()[0]);
                    dto.setUv(entry.getValue()[1]);
                    dto.setIpCount(entry.getValue()[2]);
                    return dto;
                })
                .sorted(Comparator.comparing(StatsTrendRespDTO::getStatsDate))
                .collect(Collectors.toList());
    }

    // ==================== 维度分布查询 ====================

    /**
     * 查询最近 N 天指定维度的访问分布。
     *
     * <p>流程：</p>
     * <ol>
     *   <li>校验 dimension（locale / browser / os / device / network）；</li>
     *   <li>查最近 N 天的统计记录；</li>
     *   <li>在内存中按 dimension 字段分组，对 pv 求和；</li>
     *   <li>计算 percentage = count / total * 100；</li>
     *   <li>返回按 count 降序排列的列表。</li>
     * </ol>
     *
     * @param shortCode 短码
     * @param dimension 维度（locale / browser / os / device / network）
     * @param days      最近天数
     * @return 分布列表（按 count 降序）
     */
    public List<StatsDistributionRespDTO> getDistribution(String shortCode, String dimension, int days) {
        // 校验维度合法性
        if (!SUPPORTED_DIMENSIONS.contains(dimension)) {
            log.warn("不支持的分布维度：dimension={}", dimension);
            throw new BizException(ErrorCode.PARAM_ERROR, "不支持的分布维度：" + dimension);
        }

        LocalDate startDate = LocalDate.now().minusDays(days);

        // 查最近 N 天的统计记录（ShardingSphere 按 shortCode 精确路由）
        List<LinkAccessStatsDO> statsList = linkAccessStatsMapper.selectList(
                new LambdaQueryWrapper<LinkAccessStatsDO>()
                        .eq(LinkAccessStatsDO::getShortCode, shortCode)
                        .ge(LinkAccessStatsDO::getStatsDate, startDate)
        );

        // 内存中按维度字段分组，对 pv 求和
        Map<String, Integer> grouped = new HashMap<>();
        for (LinkAccessStatsDO stats : statsList) {
            String value = extractDimensionValue(stats, dimension);
            if (value == null || value.isEmpty()) {
                continue;
            }
            int pv = stats.getPv() == null ? 0 : stats.getPv();
            grouped.merge(value, pv, Integer::sum);
        }

        // 计算总数
        int total = grouped.values().stream().mapToInt(Integer::intValue).sum();

        // 构建 DTO 并按 count 降序排序
        return grouped.entrySet().stream()
                .map(entry -> {
                    StatsDistributionRespDTO dto = new StatsDistributionRespDTO();
                    dto.setDimension(dimension);
                    dto.setValue(entry.getKey());
                    dto.setCount(entry.getValue());
                    dto.setPercentage(total == 0 ? 0.0 : (entry.getValue() * 100.0) / total);
                    return dto;
                })
                .sorted(Comparator.comparingInt(StatsDistributionRespDTO::getCount).reversed())
                .collect(Collectors.toList());
    }

    // ==================== 小时分布查询 ====================

    /**
     * 查询指定日期按小时分布的 PV。
     *
     * <p>流程：</p>
     * <ol>
     *   <li>查指定日期 hour IS NOT NULL 的统计记录；</li>
     *   <li>按小时（0-23）聚合 PV；</li>
     *   <li>返回 0-23 小时的 PV 分布（无数据的小时补 0）。</li>
     * </ol>
     *
     * @param shortCode 短码
     * @param date      指定日期
     * @return 24 小时的 PV 分布列表（dimension="hour"，value=小时字符串）
     */
    public List<StatsDistributionRespDTO> getHourDistribution(String shortCode, LocalDate date) {
        // 查指定日期 hour IS NOT NULL 的统计记录（ShardingSphere 按 shortCode 精确路由）
        List<LinkAccessStatsDO> statsList = linkAccessStatsMapper.selectList(
                new LambdaQueryWrapper<LinkAccessStatsDO>()
                        .eq(LinkAccessStatsDO::getShortCode, shortCode)
                        .eq(LinkAccessStatsDO::getStatsDate, date)
                        .isNotNull(LinkAccessStatsDO::getHour)
        );

        // 按小时聚合 PV
        int[] hourPv = new int[24];
        for (LinkAccessStatsDO stats : statsList) {
            Integer hour = stats.getHour();
            if (hour == null || hour < 0 || hour >= 24) {
                continue;
            }
            hourPv[hour] += stats.getPv() == null ? 0 : stats.getPv();
        }

        // 计算总数
        int total = 0;
        for (int pv : hourPv) {
            total += pv;
        }

        // 构建 0-23 小时的分布列表（无数据的小时补 0）
        List<StatsDistributionRespDTO> result = new ArrayList<>(24);
        for (int h = 0; h < 24; h++) {
            StatsDistributionRespDTO dto = new StatsDistributionRespDTO();
            dto.setDimension("hour");
            dto.setValue(String.valueOf(h));
            dto.setCount(hourPv[h]);
            dto.setPercentage(total == 0 ? 0.0 : (hourPv[h] * 100.0) / total);
            result.add(dto);
        }
        return result;
    }

    // ==================== 私有工具方法 ====================

    /**
     * 从 Redis Set 获取指定短码 + 日期的独立 IP 数（SCARD）。
     *
     * <p>key = {@link ShortLinkConstants#IP_SET_PREFIX} + shortCode + ":" + yyyyMMdd。</p>
     *
     * @param shortCode 短码
     * @param date      日期
     * @return 独立 IP 数（精确值）
     */
    private long getIpCountFromRedis(String shortCode, LocalDate date) {
        String key = ShortLinkConstants.IP_SET_PREFIX + shortCode + ":" + date.format(YYYYMMDD);
        Long size = stringRedisTemplate.opsForSet().size(key);
        return size == null ? 0L : size;
    }

    /**
     * 从统计记录中提取指定维度的值。
     *
     * @param stats     统计记录
     * @param dimension 维度（locale / browser / os / device / network）
     * @return 维度值，不存在返回 null
     */
    private String extractDimensionValue(LinkAccessStatsDO stats, String dimension) {
        switch (dimension) {
            case "locale":
                return stats.getLocale();
            case "browser":
                return stats.getBrowser();
            case "os":
                return stats.getOs();
            case "device":
                return stats.getDevice();
            case "network":
                return stats.getNetwork();
            default:
                return null;
        }
    }
}

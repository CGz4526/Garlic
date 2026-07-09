package com.garlic.shortlink.service.stats;

import cn.hutool.json.JSONObject;
import com.garlic.shortlink.common.constant.ShortLinkConstants;
import com.garlic.shortlink.dao.entity.LinkAccessRecordDO;
import com.garlic.shortlink.dao.mapper.LinkAccessRecordMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 访问记录内存聚合器。
 *
 * <p>消费者端核心组件：接收 Kafka 消费者投递的访问记录 JSON，在内存中按
 * shortCode + 日期 + 小时 维度聚合 PV / UV / IP / 维度分布，并同步写入访问明细到 DB。</p>
 *
 * <p>设计要点：</p>
 * <ul>
 *   <li>两级 ConcurrentHashMap 结构：key1=shortCode, key2=statsDate:hour（如 "20260708:14"），
 *       value=StatsBucket，保证多线程消费时聚合数据线程安全；</li>
 *   <li>UV 使用 Redis HyperLogLog 去重（固定 12KB 内存，适合海量用户去重，标准误差 0.81%）；</li>
 *   <li>IP 使用 Redis Set 去重（SCARD 统计独立 IP 数，精确值）；</li>
 *   <li>UV/IP 的 Redis key 按天生成，TTL 8 天，自动过期清理；</li>
 *   <li>StatsBucket 内部用 synchronized 保证 pv++/Set.add/Map.merge 的原子性；</li>
 *   <li>访问明细同步落库（LinkAccessRecordDO），ShardingSphere 按 access_time 月份分表路由。</li>
 * </ul>
 *
 * @author garlic
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatsAggregator {

    /** 注入 StringRedisTemplate（HyperLogLog UV 去重 + IP Set 去重） */
    private final StringRedisTemplate stringRedisTemplate;

    /** 注入访问明细 Mapper（落库访问明细，按 access_time 月份分表） */
    private final LinkAccessRecordMapper linkAccessRecordMapper;

    /** 日期格式化器（yyyyMMdd，用于 statsDate 与 Redis key） */
    private static final DateTimeFormatter YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");

    /** UV/IP Redis key 过期时间（天，8 天覆盖一周 + 1 天缓冲） */
    private static final long UV_IP_TTL_DAYS = 8L;

    /**
     * 两级聚合 Map：key1=shortCode, key2=statsDate:hour, value=StatsBucket。
     *
     * <p>使用 ConcurrentHashMap 保证多线程消费时的聚合数据线程安全：
     * computeIfAbsent 保证每个 shortCode+dateHour 只创建一个 StatsBucket。</p>
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, StatsBucket>> aggregateMap = new ConcurrentHashMap<>();

    /**
     * 聚合一条访问记录。
     *
     * <p>流程：</p>
     * <ol>
     *   <li>提取 shortCode、accessTime，解析 statsDate 与 hour；</li>
     *   <li>获取或创建对应 shortCode + dateHour 的 StatsBucket；</li>
     *   <li>累加 PV，UV Set / IP Set 去重，维护 locale/browser/os/device/network 分布；</li>
     *   <li>同步写入 Redis HyperLogLog（UV）与 Set（IP）；</li>
     *   <li>同步写入访问明细 LinkAccessRecordDO 到 DB。</li>
     * </ol>
     *
     * @param record 访问记录 JSON（含 shortCode/accessTime/userIdentifier/ip/browser/os/device/network/locale）
     */
    public void aggregate(JSONObject record) {
        try {
            // 1. 提取核心字段
            String shortCode = record.getStr("shortCode");
            String accessTimeStr = record.getStr("accessTime");
            if (shortCode == null || accessTimeStr == null) {
                log.warn("访问记录缺少必要字段 shortCode/accessTime：{}", record);
                return;
            }

            // 2. 解析访问时间 → statsDate + hour
            LocalDateTime accessTime = LocalDateTime.parse(accessTimeStr);
            LocalDate statsDate = accessTime.toLocalDate();
            int hour = accessTime.getHour();
            String dateHourKey = statsDate.format(YYYYMMDD) + ":" + hour;

            // 3. 获取或创建 StatsBucket（computeIfAbsent 保证线程安全）
            ConcurrentHashMap<String, StatsBucket> codeMap =
                    aggregateMap.computeIfAbsent(shortCode, k -> new ConcurrentHashMap<>());
            StatsBucket bucket = codeMap.computeIfAbsent(dateHourKey, k -> new StatsBucket());

            // 4. 提取维度字段
            String userIdentifier = record.getStr("userIdentifier");
            String ip = record.getStr("ip");
            String locale = record.getStr("locale");
            String browser = record.getStr("browser");
            String os = record.getStr("os");
            String device = record.getStr("device");
            String network = record.getStr("network");

            // 5. 内存聚合（StatsBucket 内部 synchronized 保证原子性）
            bucket.increment(userIdentifier, ip, locale, browser, os, device, network);

            // 6. Redis HyperLogLog UV 去重 + IP Set 去重
            recordUv(shortCode, statsDate, userIdentifier);
            recordIp(shortCode, statsDate, ip);

            // 7. 落库访问明细（ShardingSphere 按 access_time 月份分表路由）
            saveAccessRecord(shortCode, accessTime, userIdentifier, ip, locale, browser, os, device, network);
        } catch (Exception e) {
            // 聚合失败仅记录日志，不抛出（避免毒丸消息阻塞消费）
            log.error("访问记录聚合失败：{}", record, e);
        }
    }

    /**
     * 返回当前聚合数据（供定时 flush 任务使用）。
     *
     * @return 聚合 Map（引用，flush 任务遍历后清理对应 bucket）
     */
    public Map<String, ConcurrentHashMap<String, StatsBucket>> getAggregateMap() {
        return aggregateMap;
    }

    // ==================== UV / IP Redis 去重 ====================

    /**
     * 记录 UV（基于 Redis HyperLogLog 去重）。
     *
     * <p>key = {@link ShortLinkConstants#HYPERLOGLOG_UV_PREFIX} + shortCode + ":" + yyyyMMdd，
     * 按天分桶。HyperLogLog 固定 12KB 内存，适合海量用户去重，标准误差 0.81%。</p>
     *
     * @param shortCode      短码
     * @param statsDate      统计日期
     * @param userIdentifier 用户标识（IP + UA 的 MD5）
     */
    public void recordUv(String shortCode, LocalDate statsDate, String userIdentifier) {
        if (userIdentifier == null) {
            return;
        }
        String key = ShortLinkConstants.HYPERLOGLOG_UV_PREFIX + shortCode + ":" + statsDate.format(YYYYMMDD);
        stringRedisTemplate.opsForHyperLogLog().add(key, userIdentifier);
        stringRedisTemplate.expire(key, UV_IP_TTL_DAYS, TimeUnit.DAYS);
    }

    /**
     * 获取指定短码 + 日期的 UV 数（PFCOUNT）。
     *
     * @param shortCode 短码
     * @param statsDate 统计日期
     * @return UV 数（近似值，标准误差 0.81%）
     */
    public long getUv(String shortCode, LocalDate statsDate) {
        String key = ShortLinkConstants.HYPERLOGLOG_UV_PREFIX + shortCode + ":" + statsDate.format(YYYYMMDD);
        Long size = stringRedisTemplate.opsForHyperLogLog().size(key);
        return size == null ? 0L : size;
    }

    /**
     * 记录 IP（基于 Redis Set 去重）。
     *
     * <p>key = {@link ShortLinkConstants#IP_SET_PREFIX} + shortCode + ":" + yyyyMMdd，
     * 按天分桶。SCARD 统计独立 IP 数（精确值）。</p>
     *
     * @param shortCode 短码
     * @param statsDate 统计日期
     * @param ip        客户端 IP
     */
    public void recordIp(String shortCode, LocalDate statsDate, String ip) {
        if (ip == null) {
            return;
        }
        String key = ShortLinkConstants.IP_SET_PREFIX + shortCode + ":" + statsDate.format(YYYYMMDD);
        stringRedisTemplate.opsForSet().add(key, ip);
        stringRedisTemplate.expire(key, UV_IP_TTL_DAYS, TimeUnit.DAYS);
    }

    /**
     * 获取指定短码 + 日期的独立 IP 数（SCARD）。
     *
     * @param shortCode 短码
     * @param statsDate 统计日期
     * @return 独立 IP 数（精确值）
     */
    public long getIpCount(String shortCode, LocalDate statsDate) {
        String key = ShortLinkConstants.IP_SET_PREFIX + shortCode + ":" + statsDate.format(YYYYMMDD);
        Long size = stringRedisTemplate.opsForSet().size(key);
        return size == null ? 0L : size;
    }

    // ==================== 访问明细落库 ====================

    /**
     * 同步写入访问明细到 DB。
     *
     * <p>ShardingSphere 按 access_time 月份分表自动路由（t_link_access_record_YYYYMM）。
     * 实际生产可改为异步批量写入（攒批 + 线程池），此处简化为同步单条插入。</p>
     *
     * @param shortCode      短码
     * @param accessTime     访问时间
     * @param userIdentifier 用户标识
     * @param ip             IP
     * @param locale         地域
     * @param browser        浏览器
     * @param os             操作系统
     * @param device         设备类型
     * @param network        网络类型
     */
    private void saveAccessRecord(String shortCode, LocalDateTime accessTime, String userIdentifier,
                                  String ip, String locale, String browser, String os,
                                  String device, String network) {
        try {
            LinkAccessRecordDO recordDO = new LinkAccessRecordDO();
            recordDO.setShortCode(shortCode);
            recordDO.setUserIdentifier(userIdentifier);
            recordDO.setIp(ip);
            recordDO.setBrowser(browser);
            recordDO.setOs(os);
            recordDO.setDevice(device);
            recordDO.setNetwork(network);
            recordDO.setLocale(locale);
            recordDO.setAccessTime(accessTime);
            linkAccessRecordMapper.insert(recordDO);
        } catch (Exception e) {
            // 明细落库失败仅记录日志，不影响聚合（聚合数据仍可用）
            log.error("访问明细落库失败：shortCode={}, accessTime={}", shortCode, accessTime, e);
        }
    }

    // ==================== StatsBucket 内部类 ====================

    /**
     * 单个 shortCode + dateHour 的聚合桶。
     *
     * <p>内部所有变更方法用 synchronized 修饰，保证 pv++/Set.add/Map.merge 的原子性
     * （多线程消费时同一桶可能被并发写入）。</p>
     */
    static class StatsBucket {

        /** PV（页面浏览量） */
        private int pv;

        /** UV 去重集合（内存中保留，用于校验；实际 UV 以 Redis HyperLogLog 为准） */
        private final Set<String> uvSet = new HashSet<>();

        /** IP 去重集合（内存中保留，用于校验；实际 IP 数以 Redis Set 为准） */
        private final Set<String> ipSet = new HashSet<>();

        /** 地域分布 */
        private final Map<String, Integer> localeMap = new HashMap<>();

        /** 浏览器分布 */
        private final Map<String, Integer> browserMap = new HashMap<>();

        /** 操作系统分布 */
        private final Map<String, Integer> osMap = new HashMap<>();

        /** 设备类型分布 */
        private final Map<String, Integer> deviceMap = new HashMap<>();

        /** 网络类型分布 */
        private final Map<String, Integer> networkMap = new HashMap<>();

        /**
         * 累加一条访问记录到当前桶。
         *
         * @param userIdentifier 用户标识
         * @param ip             IP
         * @param locale         地域
         * @param browser        浏览器
         * @param os             操作系统
         * @param device         设备类型
         * @param network        网络类型
         */
        synchronized void increment(String userIdentifier, String ip, String locale,
                                    String browser, String os, String device, String network) {
            pv++;
            if (userIdentifier != null) {
                uvSet.add(userIdentifier);
            }
            if (ip != null) {
                ipSet.add(ip);
            }
            mergeCount(localeMap, locale);
            mergeCount(browserMap, browser);
            mergeCount(osMap, os);
            mergeCount(deviceMap, device);
            mergeCount(networkMap, network);
        }

        /**
         * 维度分布计数（null 跳过）。
         *
         * @param map   维度 Map
         * @param value 维度值
         */
        private void mergeCount(Map<String, Integer> map, String value) {
            if (value == null) {
                return;
            }
            map.merge(value, 1, Integer::sum);
        }

        /** @return PV */
        synchronized int getPv() {
            return pv;
        }

        /** @return UV Set 大小（内存去重数，实际 UV 以 Redis HyperLogLog 为准） */
        synchronized int getUvSetSize() {
            return uvSet.size();
        }

        /** @return IP Set 大小（内存去重数，实际 IP 数以 Redis Set 为准） */
        synchronized int getIpSetSize() {
            return ipSet.size();
        }

        /** @return 地域分布中出现次数最多的一个（用于汇总记录） */
        synchronized String topLocale() {
            return topKey(localeMap);
        }

        /** @return 浏览器分布中出现次数最多的一个 */
        synchronized String topBrowser() {
            return topKey(browserMap);
        }

        /** @return 操作系统分布中出现次数最多的一个 */
        synchronized String topOs() {
            return topKey(osMap);
        }

        /** @return 设备类型分布中出现次数最多的一个 */
        synchronized String topDevice() {
            return topKey(deviceMap);
        }

        /** @return 网络类型分布中出现次数最多的一个 */
        synchronized String topNetwork() {
            return topKey(networkMap);
        }

        /**
         * 取分布 Map 中出现次数最多的 key（次数相同取任意一个）。
         *
         * @param map 维度分布 Map
         * @return 出现次数最多的 key，Map 为空返回 null
         */
        private String topKey(Map<String, Integer> map) {
            if (map.isEmpty()) {
                return null;
            }
            String topKey = null;
            int maxCount = -1;
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxCount = entry.getValue();
                    topKey = entry.getKey();
                }
            }
            return topKey;
        }
    }
}

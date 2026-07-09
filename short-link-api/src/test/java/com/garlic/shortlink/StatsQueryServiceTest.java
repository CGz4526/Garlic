package com.garlic.shortlink;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.garlic.shortlink.common.exception.BizException;
import com.garlic.shortlink.common.exception.ErrorCode;
import com.garlic.shortlink.dao.entity.LinkAccessStatsDO;
import com.garlic.shortlink.dao.entity.ShortLinkDO;
import com.garlic.shortlink.dao.mapper.LinkAccessStatsMapper;
import com.garlic.shortlink.dao.mapper.ShortLinkMapper;
import com.garlic.shortlink.dto.stats.StatsOverviewRespDTO;
import com.garlic.shortlink.dto.stats.StatsTrendRespDTO;
import com.garlic.shortlink.service.stats.StatsQueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * StatsQueryService 单元测试（纯 Mockito，不依赖真实 DB/Redis）。
 *
 * <p>覆盖总览查询与趋势查询场景。</p>
 *
 * @author garlic
 */
@ExtendWith(MockitoExtension.class)
class StatsQueryServiceTest {

    @Mock
    private LinkAccessStatsMapper linkAccessStatsMapper;

    @Mock
    private ShortLinkMapper shortLinkMapper;

    @Mock
    private StringRedisTemplate stringRedisTemplate;

    @Mock
    private SetOperations<String, String> setOperations;

    @InjectMocks
    private StatsQueryService statsQueryService;

    @BeforeEach
    void setUp() {
        lenient().when(stringRedisTemplate.opsForSet()).thenReturn(setOperations);
    }

    /**
     * 总览查询场景：ShortLinkDO 存在，今日有统计记录，Redis IP Set 有 5 个独立 IP。
     */
    @Test
    @DisplayName("总览查询：返回累计 PV/UV 与当日 PV/UV/IP 数")
    void getOverview_shouldReturnStats() {
        String shortCode = "stats001";

        ShortLinkDO shortLinkDO = new ShortLinkDO();
        shortLinkDO.setShortCode(shortCode);
        shortLinkDO.setTotalPv(1000L);
        shortLinkDO.setTotalUv(500L);
        shortLinkDO.setDelFlag(0);

        when(shortLinkMapper.selectOne(any(LambdaQueryWrapper.class))).thenReturn(shortLinkDO);

        // 今日统计记录（两条汇总：浏览器维度 + 地域维度）
        LocalDate today = LocalDate.now();
        LinkAccessStatsDO today1 = new LinkAccessStatsDO();
        today1.setShortCode(shortCode);
        today1.setStatsDate(today);
        today1.setPv(30);
        today1.setUv(15);
        today1.setIpCount(10);

        LinkAccessStatsDO today2 = new LinkAccessStatsDO();
        today2.setShortCode(shortCode);
        today2.setStatsDate(today);
        today2.setPv(20);
        today2.setUv(8);
        today2.setIpCount(5);

        when(linkAccessStatsMapper.selectList(any(LambdaQueryWrapper.class)))
                .thenReturn(Arrays.asList(today1, today2));

        // mock Redis SCARD 返回 5
        when(setOperations.size(anyString())).thenReturn(5L);

        StatsOverviewRespDTO resp = statsQueryService.getOverview(shortCode);

        assertThat(resp).isNotNull();
        assertThat(resp.getShortCode()).isEqualTo(shortCode);
        assertThat(resp.getTotalPv()).isEqualTo(1000L);
        assertThat(resp.getTotalUv()).isEqualTo(500L);
        assertThat(resp.getTotalIpCount()).isEqualTo(5L);
        assertThat(resp.getTodayPv()).isEqualTo(50L);
        assertThat(resp.getTodayUv()).isEqualTo(23L);

        verify(shortLinkMapper, times(1)).selectOne(any(LambdaQueryWrapper.class));
        verify(linkAccessStatsMapper, times(1)).selectList(any(LambdaQueryWrapper.class));
    }

    /**
     * 总览查询场景：ShortLinkDO 不存在，抛 SHORT_LINK_NOT_FOUND。
     */
    @Test
    @DisplayName("总览查询：短链不存在抛 SHORT_LINK_NOT_FOUND")
    void getOverview_shouldThrowWhenShortLinkNotExists() {
        String shortCode = "notexist";

        when(shortLinkMapper.selectOne(any(LambdaQueryWrapper.class))).thenReturn(null);

        BizException ex = assertThrows(BizException.class,
                () -> statsQueryService.getOverview(shortCode));

        assertThat(ex.getCode()).isEqualTo(ErrorCode.SHORT_LINK_NOT_FOUND.getCode());
        verify(linkAccessStatsMapper, never()).selectList(any(LambdaQueryWrapper.class));
    }

    /**
     * 趋势查询场景：mock 返回 3 天的统计记录，验证按日期分组求和与升序排序。
     */
    @Test
    @DisplayName("趋势查询：按日期分组求和并升序排列")
    void getTrend_shouldGroupByDateAndSortAscending() {
        String shortCode = "stats001";
        int days = 7;

        LocalDate today = LocalDate.now();
        LocalDate day1 = today.minusDays(2);
        LocalDate day2 = today.minusDays(1);

        // day1 两条记录（不同维度），day2 一条记录
        LinkAccessStatsDO d1r1 = new LinkAccessStatsDO();
        d1r1.setShortCode(shortCode);
        d1r1.setStatsDate(day1);
        d1r1.setPv(10);
        d1r1.setUv(5);
        d1r1.setIpCount(3);

        LinkAccessStatsDO d1r2 = new LinkAccessStatsDO();
        d1r2.setShortCode(shortCode);
        d1r2.setStatsDate(day1);
        d1r2.setPv(20);
        d1r2.setUv(8);
        d1r2.setIpCount(4);

        LinkAccessStatsDO d2r1 = new LinkAccessStatsDO();
        d2r1.setShortCode(shortCode);
        d2r1.setStatsDate(day2);
        d2r1.setPv(40);
        d2r1.setUv(20);
        d2r1.setIpCount(15);

        when(linkAccessStatsMapper.selectList(any(LambdaQueryWrapper.class)))
                .thenReturn(Arrays.asList(d1r1, d1r2, d2r1));

        List<StatsTrendRespDTO> trend = statsQueryService.getTrend(shortCode, days);

        assertThat(trend).hasSize(2);
        // 验证按日期升序排列（day1 在前，day2 在后）
        assertThat(trend.get(0).getStatsDate()).isEqualTo(day1);
        assertThat(trend.get(1).getStatsDate()).isEqualTo(day2);
        // 验证 day1 求和：pv=30, uv=13, ipCount=7
        StatsTrendRespDTO firstDay = trend.get(0);
        assertThat(firstDay.getPv()).isEqualTo(30);
        assertThat(firstDay.getUv()).isEqualTo(13);
        assertThat(firstDay.getIpCount()).isEqualTo(7);
        // 验证 day2 求和：pv=40, uv=20, ipCount=15
        StatsTrendRespDTO secondDay = trend.get(1);
        assertThat(secondDay.getPv()).isEqualTo(40);
        assertThat(secondDay.getUv()).isEqualTo(20);
        assertThat(secondDay.getIpCount()).isEqualTo(15);

        verify(linkAccessStatsMapper, times(1)).selectList(any(LambdaQueryWrapper.class));
    }

    /**
     * 趋势查询场景：无统计数据，返回空列表。
     */
    @Test
    @DisplayName("趋势查询：无统计数据返回空列表")
    void getTrend_shouldReturnEmptyListWhenNoData() {
        String shortCode = "stats001";
        int days = 7;

        when(linkAccessStatsMapper.selectList(any(LambdaQueryWrapper.class)))
                .thenReturn(Collections.emptyList());

        List<StatsTrendRespDTO> trend = statsQueryService.getTrend(shortCode, days);

        assertThat(trend).isEmpty();
        verify(linkAccessStatsMapper, times(1)).selectList(any(LambdaQueryWrapper.class));
    }
}

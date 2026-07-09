package com.garlic.shortlink.controller;

import com.garlic.shortlink.common.result.Result;
import com.garlic.shortlink.dto.stats.StatsDistributionRespDTO;
import com.garlic.shortlink.dto.stats.StatsOverviewRespDTO;
import com.garlic.shortlink.dto.stats.StatsTrendRespDTO;
import com.garlic.shortlink.service.stats.StatsQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;

/**
 * 统计查询接口。
 *
 * <p>提供短链访问统计的总览、趋势、维度分布、小时分布查询能力。
 * 所有查询均带 short_code 条件，ShardingSphere 按 short_code 精确路由（单分片查询）。</p>
 *
 * @author garlic
 */
@Slf4j
@RestController
@RequestMapping("/api/stats")
@RequiredArgsConstructor
public class StatsController {

    /** 注入统计查询 Service */
    private final StatsQueryService statsQueryService;

    /**
     * 查询统计总览。
     *
     * @param code 短码
     * @return 总览统计
     */
    @GetMapping("/{code}/overview")
    public Result<StatsOverviewRespDTO> overview(@PathVariable("code") String code) {
        log.info("收到统计总览查询请求：shortCode={}", code);
        StatsOverviewRespDTO resp = statsQueryService.getOverview(code);
        return Result.success(resp);
    }

    /**
     * 查询访问趋势。
     *
     * @param code 短码
     * @param days 最近天数（默认 7）
     * @return 趋势列表
     */
    @GetMapping("/{code}/trend")
    public Result<List<StatsTrendRespDTO>> trend(
            @PathVariable("code") String code,
            @RequestParam(value = "days", defaultValue = "7") int days) {
        log.info("收到访问趋势查询请求：shortCode={}, days={}", code, days);
        List<StatsTrendRespDTO> list = statsQueryService.getTrend(code, days);
        return Result.success(list);
    }

    /**
     * 查询维度分布。
     *
     * @param code      短码
     * @param dimension 维度（locale / browser / os / device / network）
     * @param days      最近天数（默认 7）
     * @return 分布列表
     */
    @GetMapping("/{code}/distribution")
    public Result<List<StatsDistributionRespDTO>> distribution(
            @PathVariable("code") String code,
            @RequestParam("dimension") String dimension,
            @RequestParam(value = "days", defaultValue = "7") int days) {
        log.info("收到维度分布查询请求：shortCode={}, dimension={}, days={}", code, dimension, days);
        List<StatsDistributionRespDTO> list = statsQueryService.getDistribution(code, dimension, days);
        return Result.success(list);
    }

    /**
     * 查询指定日期的小时分布。
     *
     * @param code 短码
     * @param date 日期（格式 yyyy-MM-dd，默认今天）
     * @return 24 小时的 PV 分布列表
     */
    @GetMapping("/{code}/hour")
    public Result<List<StatsDistributionRespDTO>> hour(
            @PathVariable("code") String code,
            @RequestParam(value = "date", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        if (date == null) {
            date = LocalDate.now();
        }
        log.info("收到小时分布查询请求：shortCode={}, date={}", code, date);
        List<StatsDistributionRespDTO> list = statsQueryService.getHourDistribution(code, date);
        return Result.success(list);
    }
}

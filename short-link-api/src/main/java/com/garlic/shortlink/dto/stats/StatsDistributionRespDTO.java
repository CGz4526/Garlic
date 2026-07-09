package com.garlic.shortlink.dto.stats;

import lombok.Data;

import java.io.Serializable;

/**
 * 访问分布响应 DTO。
 *
 * <p>用于返回 locale / browser / os / device / network / hour 等维度的分布情况，
 * 包含维度值、计数及占比。</p>
 *
 * @author garlic
 */
@Data
public class StatsDistributionRespDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 维度名称：locale / browser / os / device / network / hour */
    private String dimension;

    /** 维度值（如 "Chrome" / "Windows" / "0"~"23"） */
    private String value;

    /** 该维度值的计数 */
    private int count;

    /** 该维度值占总数的百分比（0~100） */
    private double percentage;
}

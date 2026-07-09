package com.garlic.shortlink.dto.stats;

import lombok.Data;

import java.io.Serializable;

/**
 * 统计总览响应 DTO。
 *
 * <p>包含短链的累计 PV / UV / IP 数以及当日 PV / UV。</p>
 *
 * @author garlic
 */
@Data
public class StatsOverviewRespDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 短码 */
    private String shortCode;

    /** 累计总 PV */
    private Long totalPv;

    /** 累计总 UV */
    private Long totalUv;

    /** 累计总 IP 数 */
    private Long totalIpCount;

    /** 当日 PV */
    private Long todayPv;

    /** 当日 UV */
    private Long todayUv;
}

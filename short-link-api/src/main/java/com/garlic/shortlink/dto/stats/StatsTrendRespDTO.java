package com.garlic.shortlink.dto.stats;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDate;

/**
 * 访问趋势响应 DTO。
 *
 * <p>按日期维度返回 PV / UV / IP 数，用于绘制趋势图。</p>
 *
 * @author garlic
 */
@Data
public class StatsTrendRespDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 统计日期 */
    private LocalDate statsDate;

    /** 当日 PV */
    private int pv;

    /** 当日 UV */
    private int uv;

    /** 当日 IP 数 */
    private int ipCount;
}

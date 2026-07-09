package com.garlic.shortlink.dao.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * 访问统计表实体（逻辑表 t_link_access_stats，物理分表 t_link_access_stats_0~3）。
 *
 * <p>按 short_code 哈希分表。</p>
 *
 * @author garlic
 */
@Data
@TableName("t_link_access_stats")
public class LinkAccessStatsDO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 雪花ID */
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    /** 短码 */
    private String shortCode;

    /** 统计日期 */
    private LocalDate statsDate;

    /** 小时 0-23，NULL表示天汇总 */
    private Integer hour;

    /** PV */
    private Integer pv;

    /** UV */
    private Integer uv;

    /** IP数 */
    private Integer ipCount;

    /** 地域，NULL表示全量 */
    private String locale;

    /** 浏览器 */
    private String browser;

    /** 操作系统 */
    private String os;

    /** 设备类型 */
    private String device;

    /** 网络类型 */
    private String network;

    /** 创建时间 */
    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;
}

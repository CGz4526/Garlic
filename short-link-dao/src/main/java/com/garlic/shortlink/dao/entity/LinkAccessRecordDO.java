package com.garlic.shortlink.dao.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 访问明细表实体（逻辑表 t_link_access_record，按 access_time 月份分表）。
 *
 * @author garlic
 */
@Data
@TableName("t_link_access_record")
public class LinkAccessRecordDO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 雪花ID */
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    /** 短码 */
    private String shortCode;

    /** UV去重标识 */
    private String userIdentifier;

    /** IP地址 */
    private String ip;

    /** 浏览器 */
    private String browser;

    /** 操作系统 */
    private String os;

    /** 设备类型 */
    private String device;

    /** 网络类型 */
    private String network;

    /** 地域 */
    private String locale;

    /** 访问时间 */
    private LocalDateTime accessTime;

    /** 创建时间 */
    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;
}

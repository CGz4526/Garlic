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
 * 短链表实体（逻辑表 t_short_link，物理分表 t_short_link_0~3）。
 *
 * <p>按 short_code 哈希分库分表，分片键为 short_code。</p>
 *
 * @author garlic
 */
@Data
@TableName("t_short_link")
public class ShortLinkDO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 雪花ID */
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    /** 短码（分片键） */
    private String shortCode;

    /** 原始长链 */
    private String originalUrl;

    /** 用户ID */
    private Long userId;

    /** 分组ID */
    private Long groupId;

    /** 过期时间，NULL表示永不过期 */
    private LocalDateTime expireTime;

    /** 描述（describe 为 MySQL 关键字，需反引号） */
    @TableField("`describe`")
    private String describe;

    /** 总访问PV */
    private Long totalPv;

    /** 总访问UV */
    private Long totalUv;

    /** 状态：0正常 1禁用 */
    private Integer status;

    /** 删除标志：0未删除 1已删除 */
    private Integer delFlag;

    /** 创建时间 */
    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;

    /** 更新时间 */
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updateTime;
}

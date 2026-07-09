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
 * 短链分组表实体（广播表 t_group，每个库结构与数据完全一致）。
 *
 * @author garlic
 */
@Data
@TableName("t_group")
public class GroupDO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 雪花ID */
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    /** 分组名称 */
    private String name;

    /** 用户ID */
    private Long userId;

    /** 排序序号 */
    private Integer sortOrder;

    /** 创建时间 */
    @TableField(fill = FieldFill.INSERT)
    private LocalDateTime createTime;

    /** 更新时间 */
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updateTime;
}

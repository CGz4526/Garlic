package com.garlic.shortlink.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.garlic.shortlink.dao.entity.GroupDO;
import org.apache.ibatis.annotations.Mapper;

/**
 * 短链分组表 Mapper（广播表 t_group）。
 *
 * @author garlic
 */
@Mapper
public interface GroupMapper extends BaseMapper<GroupDO> {
}

package com.garlic.shortlink.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.garlic.shortlink.dao.entity.UserDO;
import org.apache.ibatis.annotations.Mapper;

/**
 * 用户表 Mapper（广播表 t_user）。
 *
 * @author garlic
 */
@Mapper
public interface UserMapper extends BaseMapper<UserDO> {
}

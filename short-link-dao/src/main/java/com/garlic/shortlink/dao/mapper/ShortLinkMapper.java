package com.garlic.shortlink.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.garlic.shortlink.dao.entity.ShortLinkDO;
import org.apache.ibatis.annotations.Mapper;

/**
 * 短链表 Mapper（逻辑表 t_short_link，按 short_code 哈希分库分表）。
 *
 * @author garlic
 */
@Mapper
public interface ShortLinkMapper extends BaseMapper<ShortLinkDO> {
}

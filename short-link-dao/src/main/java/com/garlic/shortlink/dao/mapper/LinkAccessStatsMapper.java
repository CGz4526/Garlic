package com.garlic.shortlink.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.garlic.shortlink.dao.entity.LinkAccessStatsDO;
import org.apache.ibatis.annotations.Mapper;

/**
 * 访问统计表 Mapper（逻辑表 t_link_access_stats，按 short_code 哈希分表）。
 *
 * @author garlic
 */
@Mapper
public interface LinkAccessStatsMapper extends BaseMapper<LinkAccessStatsDO> {
}

package com.garlic.shortlink.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.garlic.shortlink.dao.entity.LinkAccessRecordDO;
import org.apache.ibatis.annotations.Mapper;

/**
 * 访问明细表 Mapper（逻辑表 t_link_access_record，按 access_time 月份分表）。
 *
 * @author garlic
 */
@Mapper
public interface LinkAccessRecordMapper extends BaseMapper<LinkAccessRecordDO> {
}

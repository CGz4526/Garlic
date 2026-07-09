package com.garlic.shortlink.service.shortlink;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.garlic.shortlink.common.constant.ShortLinkConstants;
import com.garlic.shortlink.common.exception.BizException;
import com.garlic.shortlink.common.exception.ErrorCode;
import com.garlic.shortlink.common.result.PageResponse;
import com.garlic.shortlink.dao.entity.ShortLinkDO;
import com.garlic.shortlink.dao.mapper.ShortLinkMapper;
import com.garlic.shortlink.dto.ShortLinkPageReqDTO;
import com.garlic.shortlink.dto.ShortLinkRespDTO;
import com.garlic.shortlink.dto.ShortLinkUpdateReqDTO;
import com.garlic.shortlink.service.cache.CacheConsistencyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 短链管理 Service（CRUD）。
 *
 * <p>提供短链的分页查询、详情、更新、逻辑删除能力。</p>
 *
 * <p>注：本类置于 short-link-api 模块下 com.garlic.shortlink.service.shortlink 包，
 * 与 ShortLinkCreateService / ShortLinkJumpService 保持一致，Spring 组件扫描可正常生效。</p>
 *
 * <p>ShardingSphere 路由说明：</p>
 * <ul>
 *   <li>按 short_code 查询 → 精确路由到对应分库分表（详情/更新/删除）；</li>
 *   <li>无分片键查询 → 广播到所有分表（分页查询），故 userId 过滤是必要的。</li>
 * </ul>
 *
 * @author garlic
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShortLinkManageService {

    /** 注入短链 Mapper */
    private final ShortLinkMapper shortLinkMapper;

    /** 注入 StringRedisTemplate（缓存清理） */
    private final StringRedisTemplate stringRedisTemplate;

    /** 注入缓存一致性服务（延迟双删 + Kafka 广播，Task 13） */
    private final CacheConsistencyService cacheConsistencyService;

    /**
     * 分页查询短链列表。
     *
     * <p>查询条件：del_flag = 0 AND userId = ?（必填），groupId / shortCode LIKE / originalUrl LIKE 可选。
     * 由于无 short_code 分片键，ShardingSphere 会广播到所有分表，userId 过滤必要。</p>
     *
     * @param req 分页查询请求
     * @return 分页响应（records 已转为 ShortLinkRespDTO）
     */
    public PageResponse<ShortLinkRespDTO> pageShortLinks(ShortLinkPageReqDTO req) {
        // userId 必填，默认 1L
        Long userId = req.getUserId() != null ? req.getUserId() : 1L;
        Long current = req.getCurrent() != null && req.getCurrent() > 0 ? req.getCurrent() : 1L;
        Long size = req.getSize() != null && req.getSize() > 0 ? req.getSize() : 10L;

        // 构造分页对象
        Page<ShortLinkDO> page = new Page<>(current, size);

        // 构造查询条件
        LambdaQueryWrapper<ShortLinkDO> wrapper = new LambdaQueryWrapper<ShortLinkDO>()
                .eq(ShortLinkDO::getDelFlag, 0)
                .eq(ShortLinkDO::getUserId, userId)
                .eq(req.getGroupId() != null, ShortLinkDO::getGroupId, req.getGroupId())
                .like(req.getShortCode() != null && !req.getShortCode().isEmpty(),
                        ShortLinkDO::getShortCode, req.getShortCode())
                .like(req.getOriginalUrl() != null && !req.getOriginalUrl().isEmpty(),
                        ShortLinkDO::getOriginalUrl, req.getOriginalUrl())
                .orderByDesc(ShortLinkDO::getCreateTime);

        // 执行分页查询（ShardingSphere 广播 + 合并）
        Page<ShortLinkDO> resultPage = shortLinkMapper.selectPage(page, wrapper);

        // 转换 records 为 ShortLinkRespDTO
        List<ShortLinkRespDTO> records = resultPage.getRecords().stream()
                .map(this::convertToRespDTO)
                .collect(Collectors.toList());

        return PageResponse.of(
                resultPage.getCurrent(),
                resultPage.getSize(),
                resultPage.getTotal(),
                records
        );
    }

    /**
     * 查询短链详情。
     *
     * <p>按 shortCode 精确路由查询，条件：shortCode = ? AND del_flag = 0。
     * 查不到抛 BizException(SHORT_LINK_NOT_FOUND)。</p>
     *
     * @param shortCode 短码
     * @return 短链详情响应 DTO
     */
    public ShortLinkRespDTO getShortLinkDetail(String shortCode) {
        ShortLinkDO shortLinkDO = shortLinkMapper.selectOne(
                new LambdaQueryWrapper<ShortLinkDO>()
                        .eq(ShortLinkDO::getShortCode, shortCode)
                        .eq(ShortLinkDO::getDelFlag, 0)
        );
        if (shortLinkDO == null) {
            log.warn("短链详情查询不存在：shortCode={}", shortCode);
            throw new BizException(ErrorCode.SHORT_LINK_NOT_FOUND);
        }
        return convertToRespDTO(shortLinkDO);
    }

    /**
     * 更新短链。
     *
     * <p>按 shortCode 定位记录（精确路由），仅更新非空字段，更新后清理缓存。</p>
     *
     * @param req 更新请求
     */
    public void updateShortLink(ShortLinkUpdateReqDTO req) {
        String shortCode = req.getShortCode();

        // 先查记录是否存在（精确路由）
        ShortLinkDO existing = shortLinkMapper.selectOne(
                new LambdaQueryWrapper<ShortLinkDO>()
                        .eq(ShortLinkDO::getShortCode, shortCode)
                        .eq(ShortLinkDO::getDelFlag, 0)
        );
        if (existing == null) {
            log.warn("短链更新失败，记录不存在：shortCode={}", shortCode);
            throw new BizException(ErrorCode.SHORT_LINK_NOT_FOUND);
        }

        // 按非空字段更新
        LambdaUpdateWrapper<ShortLinkDO> updateWrapper = new LambdaUpdateWrapper<ShortLinkDO>()
                .eq(ShortLinkDO::getShortCode, shortCode)
                .eq(ShortLinkDO::getDelFlag, 0)
                .set(req.getOriginalUrl() != null, ShortLinkDO::getOriginalUrl, req.getOriginalUrl())
                .set(req.getGroupId() != null, ShortLinkDO::getGroupId, req.getGroupId())
                .set(req.getExpireTime() != null, ShortLinkDO::getExpireTime, req.getExpireTime())
                .set(req.getDescribe() != null, ShortLinkDO::getDescribe, req.getDescribe())
                .set(req.getStatus() != null, ShortLinkDO::getStatus, req.getStatus());

        int rows = shortLinkMapper.update(null, updateWrapper);
        log.info("短链更新完成：shortCode={}, 影响行数={}", shortCode, rows);

        // 更新后清理缓存（延迟双删 + Kafka 广播清理 Caffeine，Task 13）
        // 注：DB 已更新，这里传空 Runnable，仅享受延迟双删 + 广播能力
        evictCache(shortCode);
    }

    /**
     * 逻辑删除短链。
     *
     * <p>执行 UPDATE t_short_link SET del_flag = 1 WHERE short_code = ?，
     * 删除后清理缓存。</p>
     *
     * @param shortCode 短码
     */
    public void deleteShortLink(String shortCode) {
        LambdaUpdateWrapper<ShortLinkDO> updateWrapper = new LambdaUpdateWrapper<ShortLinkDO>()
                .eq(ShortLinkDO::getShortCode, shortCode)
                .eq(ShortLinkDO::getDelFlag, 0)
                .set(ShortLinkDO::getDelFlag, 1);

        int rows = shortLinkMapper.update(null, updateWrapper);
        log.info("短链逻辑删除完成：shortCode={}, 影响行数={}", shortCode, rows);

        // 删除后清理缓存（延迟双删 + Kafka 广播清理 Caffeine，Task 13）
        // 注：DB 已逻辑删除，这里传空 Runnable，仅享受延迟双删 + 广播能力
        evictCache(shortCode);

        // TODO Task12: 布隆过滤器删除（暂不实现，布隆过滤器不支持删除，Task12 会用双布隆方案）
    }

    /**
     * 清理短链缓存（延迟双删 + Kafka 广播）。
     *
     * <p>委托 {@link CacheConsistencyService#evictWithDoubleDelete} 执行：
     * 第一次删 Redis → 执行 DB 更新（此处传空 Runnable，因 DB 已由调用方更新）→
     * 延迟 500ms 二次删除 Redis + 清理本地 Caffeine + Kafka 广播清理其他实例 Caffeine。</p>
     *
     * <p>注：本方法在 updateShortLink / deleteShortLink 之后调用，DB 已经更新，
     * 故传入空 Runnable，仅享受延迟双删 + 广播能力。</p>
     *
     * @param shortCode 短码
     */
    public void evictCache(String shortCode) {
        // 延迟双删 + Kafka 广播清理 Caffeine（Task 13）
        // DB 已由调用方更新，传空 Runnable
        cacheConsistencyService.evictWithDoubleDelete(shortCode, () -> {});
    }

    /**
     * 将 ShortLinkDO 转换为 ShortLinkRespDTO，并填充 fullShortUrl。
     *
     * @param shortLinkDO 短链 DO
     * @return 短链响应 DTO
     */
    private ShortLinkRespDTO convertToRespDTO(ShortLinkDO shortLinkDO) {
        ShortLinkRespDTO respDTO = new ShortLinkRespDTO();
        BeanUtil.copyProperties(shortLinkDO, respDTO);
        respDTO.setFullShortUrl(ShortLinkConstants.DEFAULT_DOMAIN + shortLinkDO.getShortCode());
        return respDTO;
    }
}

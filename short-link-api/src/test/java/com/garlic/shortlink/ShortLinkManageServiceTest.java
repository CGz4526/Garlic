package com.garlic.shortlink;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.garlic.shortlink.common.exception.BizException;
import com.garlic.shortlink.common.exception.ErrorCode;
import com.garlic.shortlink.common.result.PageResponse;
import com.garlic.shortlink.dao.entity.ShortLinkDO;
import com.garlic.shortlink.dao.mapper.ShortLinkMapper;
import com.garlic.shortlink.dto.ShortLinkPageReqDTO;
import com.garlic.shortlink.dto.ShortLinkRespDTO;
import com.garlic.shortlink.dto.ShortLinkUpdateReqDTO;
import com.garlic.shortlink.service.cache.CacheConsistencyService;
import com.garlic.shortlink.service.shortlink.ShortLinkManageService;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * ShortLinkManageService 单元测试（纯 Mockito，不依赖真实 DB/Redis）。
 *
 * <p>覆盖分页查询、更新触发缓存清理、逻辑删除等场景。</p>
 *
 * @author garlic
 */
@ExtendWith(MockitoExtension.class)
class ShortLinkManageServiceTest {

    @Mock
    private ShortLinkMapper shortLinkMapper;

    @Mock
    private CacheConsistencyService cacheConsistencyService;

    @InjectMocks
    private ShortLinkManageService shortLinkManageService;

    /**
     * 初始化 MyBatis Plus TableInfo 缓存。
     *
     * <p>纯 Mockito 测试不会启动 Spring 容器，MyBatis Plus 不会自动扫描 Mapper 初始化
     * TableInfo。而 {@link LambdaUpdateWrapper#set} 在构造时会立即调用
     * {@code columnToString} 解析字段名，需要 TableInfo lambda 缓存。
     * 这里手动调用 {@link TableInfoHelper#initTableInfo} 初始化缓存，
     * 避免 "can not find lambda cache for this entity" 异常。</p>
     */
    @BeforeEach
    void setUp() {
        Configuration configuration = new Configuration();
        MapperBuilderAssistant builderAssistant = new MapperBuilderAssistant(configuration, "");
        TableInfoHelper.initTableInfo(builderAssistant, ShortLinkDO.class);
    }

    /**
     * 构造一个 ShortLinkDO。
     */
    private ShortLinkDO buildShortLink(String shortCode, String originalUrl, Long userId) {
        ShortLinkDO shortLinkDO = new ShortLinkDO();
        shortLinkDO.setId(1L);
        shortLinkDO.setShortCode(shortCode);
        shortLinkDO.setOriginalUrl(originalUrl);
        shortLinkDO.setUserId(userId);
        shortLinkDO.setGroupId(10L);
        shortLinkDO.setTotalPv(100L);
        shortLinkDO.setTotalUv(50L);
        shortLinkDO.setStatus(0);
        shortLinkDO.setDelFlag(0);
        shortLinkDO.setCreateTime(LocalDateTime.now());
        shortLinkDO.setUpdateTime(LocalDateTime.now());
        return shortLinkDO;
    }

    /**
     * 分页查询场景：mock selectPage 返回 2 条记录，验证分页响应字段。
     */
    @Test
    @DisplayName("分页查询：返回正确分页字段与记录列表")
    void pageShortLinks_shouldReturnPagedResult() {
        ShortLinkPageReqDTO req = new ShortLinkPageReqDTO();
        req.setUserId(1L);
        req.setCurrent(1L);
        req.setSize(10L);

        ShortLinkDO do1 = buildShortLink("code1", "https://www.example.com/1", 1L);
        ShortLinkDO do2 = buildShortLink("code2", "https://www.example.com/2", 1L);
        List<ShortLinkDO> records = Arrays.asList(do1, do2);

        Page<ShortLinkDO> resultPage = new Page<>(1L, 10L, 2L);
        resultPage.setRecords(records);

        when(shortLinkMapper.selectPage(any(Page.class), any(LambdaQueryWrapper.class)))
                .thenReturn(resultPage);

        PageResponse<ShortLinkRespDTO> resp = shortLinkManageService.pageShortLinks(req);

        assertThat(resp).isNotNull();
        assertThat(resp.getCurrent()).isEqualTo(1L);
        assertThat(resp.getSize()).isEqualTo(10L);
        assertThat(resp.getTotal()).isEqualTo(2L);
        assertThat(resp.getRecords()).hasSize(2);
        // 验证 records 转换为 ShortLinkRespDTO，fullShortUrl 已填充
        ShortLinkRespDTO first = resp.getRecords().get(0);
        assertThat(first.getShortCode()).isEqualTo("code1");
        assertThat(first.getFullShortUrl()).isEqualTo("http://localhost:8001/code1");
        // 验证调用 selectPage 1 次
        verify(shortLinkMapper, times(1)).selectPage(any(Page.class), any(LambdaQueryWrapper.class));
    }

    /**
     * 更新短链场景：记录存在，更新成功后应触发缓存清理。
     */
    @Test
    @DisplayName("更新短链：记录存在时触发缓存清理")
    void updateShortLink_shouldEvictCacheWhenRecordExists() {
        ShortLinkUpdateReqDTO req = new ShortLinkUpdateReqDTO();
        req.setShortCode("code1");
        req.setOriginalUrl("https://www.example.com/updated");
        req.setDescribe("更新描述");

        ShortLinkDO existing = buildShortLink("code1", "https://www.example.com/old", 1L);

        when(shortLinkMapper.selectOne(any(LambdaQueryWrapper.class))).thenReturn(existing);
        when(shortLinkMapper.update(any(), any(LambdaUpdateWrapper.class))).thenReturn(1);

        shortLinkManageService.updateShortLink(req);

        // 验证调用 selectOne 1 次
        verify(shortLinkMapper, times(1)).selectOne(any(LambdaQueryWrapper.class));
        // 验证调用 update 1 次
        verify(shortLinkMapper, times(1)).update(any(), any(LambdaUpdateWrapper.class));
        // 验证触发了缓存清理（延迟双删）
        verify(cacheConsistencyService, times(1))
                .evictWithDoubleDelete(eq("code1"), any(Runnable.class));
    }

    /**
     * 更新短链场景：记录不存在，应抛 SHORT_LINK_NOT_FOUND，不触发缓存清理。
     */
    @Test
    @DisplayName("更新短链：记录不存在抛 SHORT_LINK_NOT_FOUND，不触发缓存清理")
    void updateShortLink_shouldThrowWhenRecordNotExists() {
        ShortLinkUpdateReqDTO req = new ShortLinkUpdateReqDTO();
        req.setShortCode("notexist");

        when(shortLinkMapper.selectOne(any(LambdaQueryWrapper.class))).thenReturn(null);

        try {
            shortLinkManageService.updateShortLink(req);
        } catch (BizException ex) {
            assertThat(ex.getCode()).isEqualTo(ErrorCode.SHORT_LINK_NOT_FOUND.getCode());
        }

        // 验证未触发 update 与缓存清理
        verify(shortLinkMapper, never()).update(any(), any(LambdaUpdateWrapper.class));
        verify(cacheConsistencyService, never()).evictWithDoubleDelete(anyString(), any(Runnable.class));
    }

    /**
     * 删除短链（逻辑删除）场景：执行 del_flag=1 更新并触发缓存清理。
     */
    @Test
    @DisplayName("删除短链：逻辑删除（del_flag=1）并触发缓存清理")
    void deleteShortLink_shouldLogicDeleteAndEvictCache() {
        String shortCode = "code1";

        // mock update 返回 1 行（逻辑删除成功）
        when(shortLinkMapper.update(any(), any(LambdaUpdateWrapper.class))).thenReturn(1);

        shortLinkManageService.deleteShortLink(shortCode);

        // 验证调用 update 1 次
        ArgumentCaptor<LambdaUpdateWrapper<ShortLinkDO>> wrapperCaptor =
                ArgumentCaptor.forClass(LambdaUpdateWrapper.class);
        verify(shortLinkMapper, times(1)).update(any(), wrapperCaptor.capture());
        // 验证触发了缓存清理
        verify(cacheConsistencyService, times(1))
                .evictWithDoubleDelete(eq(shortCode), any(Runnable.class));
    }
}

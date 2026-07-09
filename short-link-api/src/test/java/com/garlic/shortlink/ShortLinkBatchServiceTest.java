package com.garlic.shortlink;

import com.garlic.shortlink.dto.ShortLinkBatchCreateReqDTO;
import com.garlic.shortlink.dto.ShortLinkBatchCreateRespDTO;
import com.garlic.shortlink.dto.ShortLinkCreateReqDTO;
import com.garlic.shortlink.dto.ShortLinkCreateRespDTO;
import com.garlic.shortlink.service.shortlink.ShortLinkBatchService;
import com.garlic.shortlink.service.shortlink.ShortLinkCreateService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * ShortLinkBatchService 单元测试（纯 Mockito，不依赖真实 DB/Redis/线程池）。
 *
 * <p>覆盖批量生成全部成功、部分失败异常隔离、空列表入参等场景。</p>
 *
 * <p>说明：由于 {@link ShortLinkBatchService} 构造函数通过 {@code @Qualifier} 注入
 * {@code batchGenExecutor}，此处手动构造被测对象，使用同步执行器
 * {@code Runnable::run} 替代真实线程池，保证测试可预测性。</p>
 *
 * @author garlic
 */
@ExtendWith(MockitoExtension.class)
class ShortLinkBatchServiceTest {

    @Mock
    private ShortLinkCreateService createService;

    /** 被测对象，手动构造以注入同步执行器 */
    private ShortLinkBatchService batchService;

    @BeforeEach
    void setUp() {
        // 使用同步执行器：supplyAsync 提交的任务在当前线程直接执行，便于断言与可预测性
        batchService = new ShortLinkBatchService(createService, Runnable::run);
    }

    /**
     * 构造批量请求。
     */
    private ShortLinkBatchCreateReqDTO buildReq(List<String> urls) {
        ShortLinkBatchCreateReqDTO req = new ShortLinkBatchCreateReqDTO();
        req.setUrls(urls);
        req.setGroupId(100L);
        req.setDescribe("批量测试");
        return req;
    }

    /**
     * 构造单条创建响应。
     */
    private ShortLinkCreateRespDTO buildResp(String url, String shortCode) {
        return new ShortLinkCreateRespDTO(shortCode, "http://localhost:8001/" + shortCode, url);
    }

    /**
     * 批量生成全部成功场景：3 条 URL 均生成成功。
     */
    @Test
    @DisplayName("批量生成全部成功：successCount=3，failCount=0")
    void batchCreate_allSuccess_shouldReturnAllResults() {
        List<String> urls = Arrays.asList(
                "https://www.example.com/1",
                "https://www.example.com/2",
                "https://www.example.com/3"
        );
        ShortLinkBatchCreateReqDTO req = buildReq(urls);

        // mock 每个单条创建均成功
        when(createService.createShortLink(any(ShortLinkCreateReqDTO.class)))
                .thenAnswer(invocation -> {
                    ShortLinkCreateReqDTO single = invocation.getArgument(0);
                    String url = single.getOriginalUrl();
                    return buildResp(url, "code" + url.charAt(url.length() - 1));
                });

        ShortLinkBatchCreateRespDTO resp = batchService.batchCreate(req);

        assertThat(resp.getSuccessCount()).isEqualTo(3);
        assertThat(resp.getFailCount()).isEqualTo(0);
        assertThat(resp.getResults()).hasSize(3);
        assertThat(resp.getFailedUrls()).isEmpty();
        // 验证调用 createService 3 次
        verify(createService, times(3)).createShortLink(any(ShortLinkCreateReqDTO.class));
    }

    /**
     * 批量生成部分失败场景：第 2 条 URL 抛异常，其他成功（异常隔离）。
     */
    @Test
    @DisplayName("批量生成部分失败：异常隔离，失败条目进 failedUrls")
    void batchCreate_partialFailure_shouldIsolateException() {
        List<String> urls = Arrays.asList(
                "https://www.example.com/ok1",
                "https://www.example.com/bad",
                "https://www.example.com/ok2"
        );
        ShortLinkBatchCreateReqDTO req = buildReq(urls);

        // mock：第 2 个 URL 抛异常，其他成功
        when(createService.createShortLink(any(ShortLinkCreateReqDTO.class)))
                .thenAnswer(invocation -> {
                    ShortLinkCreateReqDTO single = invocation.getArgument(0);
                    String url = single.getOriginalUrl();
                    if (url.endsWith("/bad")) {
                        throw new RuntimeException("DB 异常模拟");
                    }
                    return buildResp(url, "code" + url.charAt(url.length() - 1));
                });

        ShortLinkBatchCreateRespDTO resp = batchService.batchCreate(req);

        assertThat(resp.getSuccessCount()).isEqualTo(2);
        assertThat(resp.getFailCount()).isEqualTo(1);
        assertThat(resp.getResults()).hasSize(2);
        assertThat(resp.getFailedUrls()).containsExactly("https://www.example.com/bad");
        // 验证调用 createService 3 次（异常被 handle 捕获，不影响其他条目）
        verify(createService, times(3)).createShortLink(any(ShortLinkCreateReqDTO.class));
    }

    /**
     * 空列表入参场景：urls 为空列表，返回空结果。
     */
    @Test
    @DisplayName("空列表入参：successCount=0，failCount=0，results 为空")
    void batchCreate_emptyUrls_shouldReturnEmptyResult() {
        ShortLinkBatchCreateReqDTO req = buildReq(Collections.emptyList());

        ShortLinkBatchCreateRespDTO resp = batchService.batchCreate(req);

        assertThat(resp.getSuccessCount()).isEqualTo(0);
        assertThat(resp.getFailCount()).isEqualTo(0);
        assertThat(resp.getResults()).isEmpty();
        assertThat(resp.getFailedUrls()).isEmpty();
        // 验证未调用 createService
        verify(createService, times(0)).createShortLink(any(ShortLinkCreateReqDTO.class));
    }
}

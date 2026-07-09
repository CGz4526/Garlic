package com.garlic.shortlink.service.shortlink;

import com.garlic.shortlink.dto.ShortLinkBatchCreateReqDTO;
import com.garlic.shortlink.dto.ShortLinkBatchCreateRespDTO;
import com.garlic.shortlink.dto.ShortLinkCreateReqDTO;
import com.garlic.shortlink.dto.ShortLinkCreateRespDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * 批量短链生成 Service。
 *
 * <p>基于 {@link CompletableFuture} 编排，对 urls 列表并行调用
 * {@link ShortLinkCreateService#createShortLink} 完成批量生成。</p>
 *
 * <p>异常隔离：单条生成失败（抛异常）不影响其他条目，失败 URL 收集到
 * failedUrls 中返回，整体接口不抛异常（参数校验失败除外）。</p>
 *
 * <p>线程池隔离：使用自定义 {@code batchGenExecutor}（核心 8 / 最大 16 / 队列 500，
 * AbortPolicy），避免批量任务拖垮其他业务（统计上报、缓存刷新等）。</p>
 *
 * @author garlic
 */
@Slf4j
@Service
public class ShortLinkBatchService {

    /** 注入单条短链生成 Service */
    private final ShortLinkCreateService createService;

    /** 批量生成专用线程池（自定义，与统计/缓存等业务隔离） */
    private final Executor batchGenExecutor;

    /**
     * 构造函数注入：通过 @Qualifier 指定使用 batchGenExecutor 线程池。
     *
     * @param createService     单条短链生成 Service
     * @param batchGenExecutor  批量生成专用线程池
     */
    public ShortLinkBatchService(ShortLinkCreateService createService,
                                 @Qualifier("batchGenExecutor") Executor batchGenExecutor) {
        this.createService = createService;
        this.batchGenExecutor = batchGenExecutor;
    }

    /**
     * 批量创建短链。
     *
     * <p>编排流程：</p>
     * <ol>
     *   <li>遍历 urls，为每个 url 构建单条请求并提交 CompletableFuture.supplyAsync；</li>
     *   <li>用 handle 捕获单条异常，失败返回 null 并记录日志；</li>
     *   <li>CompletableFuture.allOf().join() 等待全部完成；</li>
     *   <li>顺序收集结果：非 null 入 results，null 入 failedUrls；</li>
     *   <li>返回汇总响应（含 successCount / failCount / results / failedUrls）。</li>
     * </ol>
     *
     * @param req 批量请求
     * @return 批量响应（始终非 null，失败条目体现在 failedUrls）
     */
    public ShortLinkBatchCreateRespDTO batchCreate(ShortLinkBatchCreateReqDTO req) {
        List<String> urls = req.getUrls();
        log.info("收到批量创建短链请求：size={}, groupId={}", urls.size(), req.getGroupId());

        // Task20 已实现：使用自定义 batchGenExecutor 线程池（核心 8 / 最大 16 / 队列 500，AbortPolicy）
        // 替代默认 ForkJoinPool.commonPool()，实现业务隔离 + 过载保护
        List<CompletableFuture<ShortLinkCreateRespDTO>> futures = new ArrayList<>(urls.size());
        for (String url : urls) {
            CompletableFuture<ShortLinkCreateRespDTO> future = CompletableFuture
                    .supplyAsync(() -> buildAndCreate(url, req), batchGenExecutor)
                    // 单条失败不影响其他条目：捕获异常返回 null，并记录日志
                    .handle((resp, ex) -> {
                        if (ex != null) {
                            log.error("批量生成短链失败：originalUrl={}", url, ex);
                            return null;
                        }
                        return resp;
                    });
            futures.add(future);
        }

        // 等待全部完成（异常已在 handle 中吞掉，此处 join 不会抛）
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // 顺序收集结果（futures 与 urls 同序，按索引对应）
        List<ShortLinkCreateRespDTO> results = new ArrayList<>(urls.size());
        List<String> failedUrls = new ArrayList<>();
        for (int i = 0; i < urls.size(); i++) {
            ShortLinkCreateRespDTO resp = futures.get(i).join();
            if (resp != null) {
                results.add(resp);
            } else {
                failedUrls.add(urls.get(i));
            }
        }

        ShortLinkBatchCreateRespDTO resp = new ShortLinkBatchCreateRespDTO(
                results.size(), failedUrls.size(), results, failedUrls);
        log.info("批量创建短链完成：successCount={}, failCount={}", resp.getSuccessCount(), resp.getFailCount());
        return resp;
    }

    /**
     * 构建单条创建请求并调用生成。
     *
     * <p>整批共享 groupId / expireTime / describe，userId 默认 1L（模拟登录用户）。</p>
     *
     * @param url 原始 URL
     * @param req 批量请求（用于复制共享字段）
     * @return 单条创建响应
     */
    private ShortLinkCreateRespDTO buildAndCreate(String url, ShortLinkBatchCreateReqDTO req) {
        ShortLinkCreateReqDTO singleReq = new ShortLinkCreateReqDTO();
        singleReq.setOriginalUrl(url);
        singleReq.setGroupId(req.getGroupId());
        singleReq.setExpireTime(req.getExpireTime());
        singleReq.setDescribe(req.getDescribe());
        singleReq.setUserId(1L);
        return createService.createShortLink(singleReq);
    }
}

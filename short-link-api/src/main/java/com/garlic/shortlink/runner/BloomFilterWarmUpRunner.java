package com.garlic.shortlink.runner;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.garlic.shortlink.common.bloom.BloomFilterService;
import com.garlic.shortlink.dao.entity.ShortLinkDO;
import com.garlic.shortlink.dao.mapper.ShortLinkMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 布隆过滤器启动预热 Runner。
 *
 * <p>应用启动时从 DB 分批加载所有未删除的 short_code，写入布隆过滤器，
 * 用于防止缓存穿透。即使已预热过（count > 0）也重新预热，因为 DB 可能有新增。</p>
 *
 * <p><b>异常处理</b>：预热失败不影响应用启动，仅记录错误日志。</p>
 *
 * <p>放置说明：本类需要同时访问 {@link BloomFilterService}（位于 short-link-common）
 * 与 {@link ShortLinkMapper}（位于 short-link-dao），而 short-link-common 不依赖 dao，
 * 故置于 short-link-api 模块（通过 short-link-service 传递依赖 dao）下
 * {@code com.garlic.shortlink.runner} 包，Spring 组件扫描可正常生效。</p>
 *
 * @author garlic
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BloomFilterWarmUpRunner implements CommandLineRunner {

    /** 注入布隆过滤器服务 */
    private final BloomFilterService bloomFilterService;

    /** 注入短链 Mapper */
    private final ShortLinkMapper shortLinkMapper;

    /** 分页大小：每批 1000 条 */
    private static final long PAGE_SIZE = 1000L;

    @Override
    public void run(String... args) {
        long startTime = System.currentTimeMillis();
        log.info("布隆过滤器预热开始...");
        try {
            // 即使已预热过也重新预热（DB 可能有新增）
            long current = 1L;
            long total = 0L;
            while (true) {
                // 分页查询未删除的 short_code（仅 select short_code 字段）
                Page<ShortLinkDO> page = new Page<>(current, PAGE_SIZE);
                LambdaQueryWrapper<ShortLinkDO> wrapper = new LambdaQueryWrapper<ShortLinkDO>()
                        .select(ShortLinkDO::getShortCode)
                        .eq(ShortLinkDO::getDelFlag, 0);
                Page<ShortLinkDO> result = shortLinkMapper.selectPage(page, wrapper);

                List<ShortLinkDO> records = result.getRecords();
                if (records == null || records.isEmpty()) {
                    break;
                }

                // 遍历每批，写入布隆过滤器
                for (ShortLinkDO record : records) {
                    String shortCode = record.getShortCode();
                    if (shortCode != null && !shortCode.isEmpty()) {
                        bloomFilterService.add(shortCode);
                        total++;
                    }
                }

                // 判断是否还有下一页
                if (result.getCurrent() >= result.getPages()) {
                    break;
                }
                current++;
            }

            long elapsed = System.currentTimeMillis() - startTime;
            log.info("布隆过滤器预热完成：共加载 {} 条短码，耗时 {} ms，过滤器近似计数 {}",
                    total, elapsed, bloomFilterService.count());
        } catch (Exception e) {
            // 预热失败不影响应用启动
            log.error("布隆过滤器预热失败，不影响应用启动", e);
        }
    }
}

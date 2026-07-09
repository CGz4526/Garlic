package com.garlic.shortlink.common.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * Caffeine 本地缓存配置。
 *
 * <p>定义统一的基础本地缓存 {@code shortLinkLocalCache}，作为短链接查询的二级缓存兜底。
 * 热点 key 提升逻辑（如容量调优、权重策略）将在 Task 11 细化。</p>
 *
 * @author garlic
 */
@Configuration
public class CaffeineConfig {

    /** 本地缓存最大容量 */
    private static final long MAX_SIZE = 10000L;

    /** 写入后过期时间（秒） */
    private static final long EXPIRE_AFTER_WRITE_SECONDS = 60L;

    /**
     * 短链接本地缓存：最大容量 10000，写入后 60 秒过期。
     *
     * @return Caffeine 缓存实例
     */
    @Bean(name = "shortLinkLocalCache")
    public Cache<String, String> shortLinkLocalCache() {
        return Caffeine.newBuilder()
                .maximumSize(MAX_SIZE)
                .expireAfterWrite(EXPIRE_AFTER_WRITE_SECONDS, TimeUnit.SECONDS)
                .build();
    }
}

package com.garlic.shortlink.common.config;

import com.garlic.shortlink.common.util.SnowflakeIdWorker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 雪花算法 ID 生成器配置。
 *
 * <p>从 application.yml 读取 workerId 与 datacenterId，构造全局唯一的
 * {@link SnowflakeIdWorker} Bean，供短码生成器及需要分布式 ID 的场景注入使用。</p>
 *
 * <p>配置项：</p>
 * <ul>
 *     <li>{@code snowflake.worker-id}：工作节点 ID（0 ~ 31），默认 1</li>
 *     <li>{@code snowflake.datacenter-id}：数据中心 ID（0 ~ 31），默认 1</li>
 * </ul>
 *
 * @author garlic
 */
@Configuration
public class SnowflakeIdConfig {

    /**
     * 构造雪花算法 ID 生成器 Bean。
     *
     * @param workerId     工作节点 ID（从配置读取，默认 1）
     * @param datacenterId 数据中心 ID（从配置读取，默认 1）
     * @return SnowflakeIdWorker 实例
     */
    @Bean
    public SnowflakeIdWorker snowflakeIdWorker(
            @Value("${snowflake.worker-id:1}") long workerId,
            @Value("${snowflake.datacenter-id:1}") long datacenterId) {
        return new SnowflakeIdWorker(workerId, datacenterId);
    }
}

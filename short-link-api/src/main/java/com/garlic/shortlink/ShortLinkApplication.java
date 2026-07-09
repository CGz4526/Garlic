package com.garlic.shortlink;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 短链接服务主启动类。
 *
 * <p>{@code @EnableScheduling}：开启定时任务支持，供 {@code StatsFlushTask}
 * 每分钟 flush 内存聚合数据到 t_link_access_stats。</p>
 *
 * @author garlic
 */
@SpringBootApplication
@MapperScan("com.garlic.shortlink.dao.mapper")
@EnableScheduling
public class ShortLinkApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShortLinkApplication.class, args);
    }
}

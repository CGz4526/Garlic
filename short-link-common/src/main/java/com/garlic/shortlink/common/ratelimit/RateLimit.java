package com.garlic.shortlink.common.ratelimit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 限流注解，标记在需要限流的 Controller 方法上。
 *
 * <p>配合 {@link RateLimitAspect} 切面，根据 {@link LimitType} 选择不同限流策略：</p>
 * <ul>
 *   <li>{@link LimitType#INTERFACE}：接口级令牌桶（capacity/rate 生效）；</li>
 *   <li>{@link LimitType#USER}：用户级滑动窗口（windowSize/maxRequests 生效）；</li>
 *   <li>{@link LimitType#IP}：IP 级滑动窗口（windowSize/maxRequests 生效）。</li>
 * </ul>
 *
 * <p>{@code key} 支持 SpEL 表达式，可引用方法参数，如 {@code "#req.userId"} 或 {@code "'jump:' + #code"}。</p>
 *
 * @author garlic
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RateLimit {

    /**
     * 限流 key，支持 SpEL 表达式。
     *
     * <p>为空时按 {@link #type()} 自动生成：INTERFACE→方法全名，USER→userId，IP→请求IP。</p>
     *
     * @return SpEL 表达式
     */
    String key() default "";

    /**
     * 限流类型，默认接口级令牌桶。
     *
     * @return 限流类型
     */
    LimitType type() default LimitType.INTERFACE;

    /**
     * 令牌桶容量（最大令牌数），{@link LimitType#INTERFACE} 类型使用。
     *
     * @return 容量
     */
    long capacity() default 5000;

    /**
     * 每秒填充速率，{@link LimitType#INTERFACE} 类型使用。
     *
     * @return 每秒填充速率
     */
    long rate() default 5000;

    /**
     * 滑动窗口大小（毫秒），{@link LimitType#USER}/{@link LimitType#IP} 类型使用。
     *
     * @return 窗口大小（毫秒）
     */
    long windowSize() default 1000;

    /**
     * 窗口内最大请求数，{@link LimitType#USER}/{@link LimitType#IP} 类型使用。
     *
     * @return 最大请求数
     */
    long maxRequests() default 50;

    /**
     * 限流触发时的提示消息。
     *
     * @return 提示消息
     */
    String message() default "请求过于频繁，请稍后再试";
}

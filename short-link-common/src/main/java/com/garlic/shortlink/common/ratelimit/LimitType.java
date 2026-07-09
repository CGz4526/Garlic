package com.garlic.shortlink.common.ratelimit;

/**
 * 限流类型枚举。
 *
 * <p>不同粒度对应不同限流算法：</p>
 * <ul>
 *   <li>{@link #INTERFACE}：接口级，采用 Redis + Lua 令牌桶，集群统一限流；</li>
 *   <li>{@link #USER}：用户级，采用 Redis ZSet 滑动窗口，按 userId 限流；</li>
 *   <li>{@link #IP}：IP 级，采用 Redis ZSet 滑动窗口，按客户端 IP 限流。</li>
 * </ul>
 *
 * @author garlic
 */
public enum LimitType {

    /** 接口级令牌桶限流（集群统一） */
    INTERFACE,

    /** 用户级滑动窗口限流 */
    USER,

    /** IP 级滑动窗口限流 */
    IP
}

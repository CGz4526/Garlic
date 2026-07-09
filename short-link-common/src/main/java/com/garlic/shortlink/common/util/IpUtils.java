package com.garlic.shortlink.common.util;

import cn.hutool.core.util.StrUtil;

import javax.servlet.http.HttpServletRequest;

/**
 * IP 工具类。
 *
 * <p>支持从 HTTP 请求中获取客户端真实 IP，兼容多层代理转发场景。</p>
 *
 * @author garlic
 */
public final class IpUtils {

    /** 未知 IP 标识 */
    public static final String UNKNOWN = "unknown";

    /** 本地 IPv4 */
    private static final String LOCAL_IPV4 = "127.0.0.1";

    /** 本地 IPv6 */
    private static final String LOCAL_IPV6 = "0:0:0:0:0:0:0:1";

    private IpUtils() {
        throw new UnsupportedOperationException("工具类不可实例化");
    }

    /**
     * 从 HTTP 请求中获取客户端真实 IP。
     *
     * <p>依次尝试解析以下请求头：X-Forwarded-For、Proxy-Client-IP、WL-Proxy-Client-IP、
     * HTTP_CLIENT_IP、HTTP_X_FORWARDED_FOR，最后回退到 request.getRemoteAddr()。</p>
     *
     * @param request HTTP 请求
     * @return 客户端 IP
     */
    public static String getIpAddr(HttpServletRequest request) {
        if (request == null) {
            return UNKNOWN;
        }
        String ip = request.getHeader("X-Forwarded-For");
        if (isInvalidIp(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (isInvalidIp(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (isInvalidIp(ip)) {
            ip = request.getHeader("HTTP_CLIENT_IP");
        }
        if (isInvalidIp(ip)) {
            ip = request.getHeader("HTTP_X_FORWARDED_FOR");
        }
        if (isInvalidIp(ip)) {
            ip = request.getRemoteAddr();
        }
        // 处理本地 IPv6
        if (LOCAL_IPV6.equals(ip)) {
            ip = LOCAL_IPV4;
        }
        // X-Forwarded-For 可能包含多个 IP，取第一个（即客户端真实 IP）
        if (ip != null && ip.contains(StrUtil.COMMA)) {
            ip = ip.split(StrUtil.COMMA)[0].trim();
        }
        return ip;
    }

    /**
     * 判断 IP 是否无效。
     *
     * @param ip IP 地址
     * @return true 表示无效
     */
    private static boolean isInvalidIp(String ip) {
        return StrUtil.isBlank(ip) || UNKNOWN.equalsIgnoreCase(ip);
    }
}

package com.garlic.shortlink.common.util;

import cn.hutool.core.util.StrUtil;
import cn.hutool.core.util.URLUtil;

import java.net.URI;
import java.util.regex.Pattern;

/**
 * URL 工具类。
 *
 * <p>提供 URL 合法性校验、域名提取、URL 规范化等能力。</p>
 *
 * @author garlic
 */
public final class UrlUtils {

    /** http/https URL 正则 */
    private static final Pattern URL_PATTERN = Pattern.compile(
            "^https?://"
                    + "(([0-9a-zA-Z][0-9a-zA-Z-]{0,61}[0-9a-zA-Z]\\.)*[a-zA-Z]{2,}"
                    + "|"
                    + "([0-9]{1,3}\\.){3}[0-9]{1,3})"
                    + "(:[0-9]{1,5})?"
                    + "(/[0-9a-zA-Z_~!@#$%&'*+=:?,./-]*)?$",
            Pattern.CASE_INSENSITIVE);

    private UrlUtils() {
        throw new UnsupportedOperationException("工具类不可实例化");
    }

    /**
     * 校验 URL 是否合法（仅支持 http/https 协议）。
     *
     * @param url 待校验的 URL
     * @return true 表示合法
     */
    public static boolean isValidUrl(String url) {
        if (StrUtil.isBlank(url)) {
            return false;
        }
        try {
            URLUtil.url(url);
        } catch (Exception e) {
            return false;
        }
        return URL_PATTERN.matcher(url).matches();
    }

    /**
     * 提取 URL 中的域名（含端口）。
     *
     * @param url URL
     * @return 域名，如 example.com:8080；解析失败返回空字符串
     */
    public static String getDomain(String url) {
        if (StrUtil.isBlank(url)) {
            return StrUtil.EMPTY;
        }
        try {
            URI uri = URI.create(url);
            return uri.getHost() == null ? StrUtil.EMPTY : uri.getHost() + (uri.getPort() == -1 ? StrUtil.EMPTY : ":" + uri.getPort());
        } catch (Exception e) {
            return StrUtil.EMPTY;
        }
    }

    /**
     * 规范化 URL：去除首尾空格、统一协议小写、去除末尾多余斜杠。
     *
     * @param url 原始 URL
     * @return 规范化后的 URL
     */
    public static String normalize(String url) {
        if (StrUtil.isBlank(url)) {
            return StrUtil.EMPTY;
        }
        String normalized = url.trim();
        // 统一协议为小写
        if (normalized.toLowerCase().startsWith("http://")) {
            normalized = "http://" + normalized.substring(7);
        } else if (normalized.toLowerCase().startsWith("https://")) {
            normalized = "https://" + normalized.substring(8);
        }
        // 去除末尾多余的斜杠（保留根路径 /）
        while (normalized.endsWith("/") && normalized.length() > 1
                && !normalized.endsWith("://")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }
}

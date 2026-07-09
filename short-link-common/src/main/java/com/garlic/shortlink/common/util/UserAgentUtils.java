package com.garlic.shortlink.common.util;

import cn.hutool.core.util.StrUtil;
import cn.hutool.http.useragent.UserAgent;
import cn.hutool.http.useragent.UserAgentUtil;

/**
 * User-Agent 解析工具类。
 *
 * <p>基于 Hutool 的 UserAgentUtil，提供浏览器、操作系统、设备类型、网络类型解析。</p>
 *
 * @author garlic
 */
public final class UserAgentUtils {

    /** PC 标识 */
    public static final String DEVICE_PC = "PC";

    /** 移动端标识 */
    public static final String DEVICE_MOBILE = "MOBILE";

    /** 平板标识 */
    public static final String DEVICE_TABLET = "TABLET";

    /** 未知标识 */
    public static final String UNKNOWN = "UNKNOWN";

    /** WIFI 网络 */
    public static final String NETWORK_WIFI = "WIFI";

    /** 4G 网络 */
    public static final String NETWORK_4G = "4G";

    /** 5G 网络 */
    public static final String NETWORK_5G = "5G";

    private UserAgentUtils() {
        throw new UnsupportedOperationException("工具类不可实例化");
    }

    /**
     * 解析浏览器名称。
     *
     * @param ua User-Agent 字符串
     * @return 浏览器名称
     */
    public static String parseBrowser(String ua) {
        if (StrUtil.isBlank(ua)) {
            return UNKNOWN;
        }
        UserAgent userAgent = UserAgentUtil.parse(ua);
        if (userAgent == null || userAgent.getBrowser() == null) {
            return UNKNOWN;
        }
        String browser = userAgent.getBrowser().getName();
        return StrUtil.isBlank(browser) ? UNKNOWN : browser;
    }

    /**
     * 解析操作系统名称。
     *
     * @param ua User-Agent 字符串
     * @return 操作系统名称
     */
    public static String parseOs(String ua) {
        if (StrUtil.isBlank(ua)) {
            return UNKNOWN;
        }
        UserAgent userAgent = UserAgentUtil.parse(ua);
        if (userAgent == null || userAgent.getOs() == null) {
            return UNKNOWN;
        }
        String os = userAgent.getOs().getName();
        return StrUtil.isBlank(os) ? UNKNOWN : os;
    }

    /**
     * 解析设备类型（MOBILE/PC/TABLET）。
     *
     * @param ua User-Agent 字符串
     * @return 设备类型
     */
    public static String parseDevice(String ua) {
        if (StrUtil.isBlank(ua)) {
            return UNKNOWN;
        }
        UserAgent userAgent = UserAgentUtil.parse(ua);
        if (userAgent == null) {
            return UNKNOWN;
        }
        // Hutool 提供的移动端判断
        if (userAgent.isMobile()) {
            // 进一步区分平板：UA 中包含 iPad / Tablet 等关键字
            if (ua.contains("iPad") || ua.contains("Tablet") || ua.contains("PlayBook")) {
                return DEVICE_TABLET;
            }
            return DEVICE_MOBILE;
        }
        return DEVICE_PC;
    }

    /**
     * 解析网络类型（WIFI/4G/5G）。
     *
     * <p>简化处理：User-Agent 通常不直接携带网络类型，这里通过 NetType 关键字近似判断。</p>
     *
     * @param ua User-Agent 字符串
     * @return 网络类型
     */
    public static String parseNetwork(String ua) {
        if (StrUtil.isBlank(ua)) {
            return UNKNOWN;
        }
        // 部分客户端 UA 中会携带 NetType 字段，如 "NetType/WIFI"、"NetType/4G"、"NetType/5G"
        if (ua.contains("NetType/WIFI") || ua.contains("NetType/wifi")) {
            return NETWORK_WIFI;
        }
        if (ua.contains("NetType/5G") || ua.contains("NetType/5g")) {
            return NETWORK_5G;
        }
        if (ua.contains("NetType/4G") || ua.contains("NetType/4g")) {
            return NETWORK_4G;
        }
        // 无法识别网络类型时，PC 端默认 WIFI
        String device = parseDevice(ua);
        if (DEVICE_PC.equals(device)) {
            return NETWORK_WIFI;
        }
        return UNKNOWN;
    }
}

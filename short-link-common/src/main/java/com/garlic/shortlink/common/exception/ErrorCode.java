package com.garlic.shortlink.common.exception;

/**
 * 错误码枚举，统一管理系统中所有业务错误码。
 *
 * <p>错误码分段规则：</p>
 * <ul>
 *   <li>1xxxx - 参数与请求类错误</li>
 *   <li>2xxxx - 短链业务类错误</li>
 *   <li>3xxxx - 限流与幂等类错误</li>
 *   <li>5xxxx - 系统基础类错误</li>
 * </ul>
 *
 * @author garlic
 */
public enum ErrorCode {

    /** 成功 */
    SUCCESS(0, "成功"),

    /** 参数错误 */
    PARAM_ERROR(10001, "参数错误"),

    /** URL 不合法 */
    URL_INVALID(10002, "URL 不合法"),

    /** 短链不存在 */
    SHORT_LINK_NOT_FOUND(20001, "短链不存在"),

    /** 短链已过期 */
    SHORT_LINK_EXPIRED(20002, "短链已过期"),

    /** 短链已被禁用 */
    SHORT_LINK_BANNED(20003, "短链已被禁用"),

    /** 请求过于频繁 */
    RATE_LIMITED(30001, "请求过于频繁，请稍后再试"),

    /** 请勿重复提交 */
    IDEMPOTENT_REJECT(30002, "请勿重复提交"),

    /** 系统内部错误 */
    SYSTEM_ERROR(50000, "系统内部错误"),

    /** 数据库异常 */
    DB_ERROR(50001, "数据库异常"),

    /** 缓存异常 */
    CACHE_ERROR(50002, "缓存异常"),

    /** 消息队列异常 */
    MQ_ERROR(50003, "消息队列异常");

    /** 错误码 */
    private final Integer code;

    /** 错误消息 */
    private final String message;

    ErrorCode(Integer code, String message) {
        this.code = code;
        this.message = message;
    }

    /**
     * 获取错误码。
     *
     * @return 错误码
     */
    public Integer getCode() {
        return code;
    }

    /**
     * 获取错误消息。
     *
     * @return 错误消息
     */
    public String getMessage() {
        return message;
    }
}

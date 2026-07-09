package com.garlic.shortlink.common.result;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 统一响应体封装。
 *
 * <p>成功 code = 0，失败 code 非 0。</p>
 *
 * @author garlic
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Result<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 成功状态码 */
    public static final Integer SUCCESS_CODE = 0;

    /** 响应状态码：0 表示成功，非 0 表示失败 */
    private Integer code;

    /** 响应消息 */
    private String message;

    /** 响应数据 */
    private T data;

    /** 请求 ID，用于链路追踪 */
    private String requestId;

    /** 响应时间戳 */
    private Long timestamp;

    /**
     * 成功响应（无数据）。
     *
     * @param <T> 数据类型
     * @return 成功响应体
     */
    public static <T> Result<T> success() {
        return new Result<>(SUCCESS_CODE, "success", null, null, System.currentTimeMillis());
    }

    /**
     * 成功响应（带数据）。
     *
     * @param data 响应数据
     * @param <T>  数据类型
     * @return 成功响应体
     */
    public static <T> Result<T> success(T data) {
        return new Result<>(SUCCESS_CODE, "success", data, null, System.currentTimeMillis());
    }

    /**
     * 成功响应（带消息和数据）。
     *
     * @param message 响应消息
     * @param data    响应数据
     * @param <T>     数据类型
     * @return 成功响应体
     */
    public static <T> Result<T> success(String message, T data) {
        return new Result<>(SUCCESS_CODE, message, data, null, System.currentTimeMillis());
    }

    /**
     * 失败响应（字符串状态码，会被解析为 Integer）。
     *
     * @param code    状态码字符串
     * @param message 响应消息
     * @param <T>     数据类型
     * @return 失败响应体
     */
    public static <T> Result<T> failure(String code, String message) {
        Integer parsedCode;
        try {
            parsedCode = Integer.valueOf(code);
        } catch (NumberFormatException e) {
            parsedCode = 50000;
        }
        return new Result<>(parsedCode, message, null, null, System.currentTimeMillis());
    }

    /**
     * 失败响应（整数状态码）。
     *
     * @param code    状态码
     * @param message 响应消息
     * @param <T>     数据类型
     * @return 失败响应体
     */
    public static <T> Result<T> failure(Integer code, String message) {
        return new Result<>(code, message, null, null, System.currentTimeMillis());
    }

    /**
     * 判断是否成功。
     *
     * @return true 表示成功
     */
    public boolean isSuccess() {
        return SUCCESS_CODE.equals(this.code);
    }
}

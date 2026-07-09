package com.garlic.shortlink.common.exception;

import lombok.Getter;

/**
 * 业务异常，用于在业务逻辑中主动抛出并携带错误码。
 *
 * @author garlic
 */
@Getter
public class BizException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** 错误码 */
    private final Integer code;

    /**
     * 使用错误码枚举构造业务异常。
     *
     * @param errorCode 错误码枚举
     */
    public BizException(ErrorCode errorCode) {
        super(errorCode.getMessage());
        this.code = errorCode.getCode();
    }

    /**
     * 使用错误码枚举和自定义消息构造业务异常。
     *
     * @param errorCode 错误码枚举
     * @param message   自定义错误消息
     */
    public BizException(ErrorCode errorCode, String message) {
        super(message);
        this.code = errorCode.getCode();
    }

    /**
     * 使用错误码和消息构造业务异常。
     *
     * @param code    错误码
     * @param message 错误消息
     */
    public BizException(Integer code, String message) {
        super(message);
        this.code = code;
    }

    /**
     * 使用错误码枚举和原因构造业务异常。
     *
     * @param errorCode 错误码枚举
     * @param cause     异常原因
     */
    public BizException(ErrorCode errorCode, Throwable cause) {
        super(errorCode.getMessage(), cause);
        this.code = errorCode.getCode();
    }

    /**
     * 使用错误码、消息和原因构造业务异常。
     *
     * @param code    错误码
     * @param message 错误消息
     * @param cause   异常原因
     */
    public BizException(Integer code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }
}

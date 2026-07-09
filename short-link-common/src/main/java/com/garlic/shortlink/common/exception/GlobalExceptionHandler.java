package com.garlic.shortlink.common.exception;

import com.garlic.shortlink.common.result.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindException;
import org.springframework.validation.FieldError;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletRequest;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import java.util.stream.Collectors;

/**
 * 全局异常处理器，统一封装各类异常为标准响应体。
 *
 * @author garlic
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * 处理业务异常。
     *
     * @param e       业务异常
     * @param request HTTP 请求
     * @return 统一响应体
     */
    @ExceptionHandler(BizException.class)
    public Result<Void> handleBizException(BizException e, HttpServletRequest request) {
        log.warn("业务异常 | uri={} | code={} | message={}", request.getRequestURI(), e.getCode(), e.getMessage());
        return Result.failure(e.getCode(), e.getMessage());
    }

    /**
     * 处理参数校验异常（@RequestBody @Valid 校验失败）。
     *
     * @param e       参数校验异常
     * @param request HTTP 请求
     * @return 统一响应体
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public Result<Void> handleMethodArgumentNotValidException(MethodArgumentNotValidException e, HttpServletRequest request) {
        String message = e.getBindingResult().getFieldErrors().stream()
                .map(FieldError::getDefaultMessage)
                .collect(Collectors.joining("; "));
        log.warn("参数校验异常 | uri={} | message={}", request.getRequestURI(), message);
        return Result.failure(ErrorCode.PARAM_ERROR.getCode(), message);
    }

    /**
     * 处理参数绑定异常。
     *
     * @param e       绑定异常
     * @param request HTTP 请求
     * @return 统一响应体
     */
    @ExceptionHandler(BindException.class)
    public Result<Void> handleBindException(BindException e, HttpServletRequest request) {
        String message = e.getBindingResult().getFieldErrors().stream()
                .map(FieldError::getDefaultMessage)
                .collect(Collectors.joining("; "));
        log.warn("参数绑定异常 | uri={} | message={}", request.getRequestURI(), message);
        return Result.failure(ErrorCode.PARAM_ERROR.getCode(), message);
    }

    /**
     * 处理约束违反异常（@Validated + @RequestParam/@PathVariable 校验失败）。
     *
     * @param e       约束违反异常
     * @param request HTTP 请求
     * @return 统一响应体
     */
    @ExceptionHandler(ConstraintViolationException.class)
    public Result<Void> handleConstraintViolationException(ConstraintViolationException e, HttpServletRequest request) {
        String message = e.getConstraintViolations().stream()
                .map(ConstraintViolation::getMessage)
                .collect(Collectors.joining("; "));
        log.warn("约束校验异常 | uri={} | message={}", request.getRequestURI(), message);
        return Result.failure(ErrorCode.PARAM_ERROR.getCode(), message);
    }

    /**
     * 处理缺少请求参数异常。
     *
     * @param e       缺少请求参数异常
     * @param request HTTP 请求
     * @return 统一响应体
     */
    @ExceptionHandler(MissingServletRequestParameterException.class)
    public Result<Void> handleMissingServletRequestParameterException(MissingServletRequestParameterException e, HttpServletRequest request) {
        log.warn("缺少请求参数 | uri={} | param={}", request.getRequestURI(), e.getParameterName());
        return Result.failure(ErrorCode.PARAM_ERROR.getCode(), "缺少必要参数：" + e.getParameterName());
    }

    /**
     * 处理请求体不可读异常（JSON 格式错误等）。
     *
     * @param e       请求体不可读异常
     * @param request HTTP 请求
     * @return 统一响应体
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public Result<Void> handleHttpMessageNotReadableException(HttpMessageNotReadableException e, HttpServletRequest request) {
        log.warn("请求体解析失败 | uri={} | message={}", request.getRequestURI(), e.getMessage());
        return Result.failure(ErrorCode.PARAM_ERROR.getCode(), "请求体格式错误");
    }

    /**
     * 处理请求方法不支持异常（405）。
     *
     * @param e       请求方法不支持异常
     * @param request HTTP 请求
     * @return 统一响应体
     */
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    @ResponseStatus(HttpStatus.METHOD_NOT_ALLOWED)
    public Result<Void> handleHttpRequestMethodNotSupportedException(HttpRequestMethodNotSupportedException e, HttpServletRequest request) {
        log.warn("请求方法不支持 | uri={} | method={}", request.getRequestURI(), e.getMethod());
        return Result.failure(HttpStatus.METHOD_NOT_ALLOWED.value(), "请求方法不支持：" + e.getMethod());
    }

    /**
     * 兜底异常处理，捕获所有未明确处理的异常。
     *
     * @param e       未知异常
     * @param request HTTP 请求
     * @return 统一响应体
     */
    @ExceptionHandler(Exception.class)
    public Result<Void> handleException(Exception e, HttpServletRequest request) {
        log.error("系统异常 | uri={}", request.getRequestURI(), e);
        return Result.failure(ErrorCode.SYSTEM_ERROR.getCode(), ErrorCode.SYSTEM_ERROR.getMessage());
    }
}

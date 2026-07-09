package com.garlic.shortlink.common.validation;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 自定义 URL 校验注解，用于校验字符串是否为合法的 http/https URL。
 *
 * <p>使用示例：</p>
 * <pre>
 * {@literal @}Url(message = "URL 不合法")
 * private String originalUrl;
 * </pre>
 *
 * @author garlic
 */
@Documented
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = UrlValidator.class)
public @interface Url {

    /**
     * 校验失败时的默认消息。
     *
     * @return 默认消息
     */
    String message() default "URL 不合法";

    /**
     * 校验分组。
     *
     * @return 分组数组
     */
    Class<?>[] groups() default {};

    /**
     * 校验负载。
     *
     * @return 负载数组
     */
    Class<? extends Payload>[] payload() default {};
}

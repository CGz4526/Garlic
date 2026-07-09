package com.garlic.shortlink.common.validation;

import cn.hutool.core.util.StrUtil;
import cn.hutool.core.util.URLUtil;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.regex.Pattern;

/**
 * {@link Url} 注解的校验器实现。
 *
 * <p>校验逻辑：空值放行（由 @NotBlank 等注解负责非空校验），非空时校验是否为合法的 http/https URL。</p>
 *
 * @author garlic
 */
public class UrlValidator implements ConstraintValidator<Url, String> {

    /** http/https URL 正则 */
    private static final Pattern URL_PATTERN = Pattern.compile(
            "^https?://"
                    + "(([0-9a-zA-Z][0-9a-zA-Z-]{0,61}[0-9a-zA-Z]\\.)*[a-zA-Z]{2,}" // 域名
                    + "|"
                    + "([0-9]{1,3}\\.){3}[0-9]{1,3})" // IP
                    + "(:[0-9]{1,5})?" // 端口
                    + "(/[0-9a-zA-Z_~!@#$%&'*+=:?,./-]*)?$",
            Pattern.CASE_INSENSITIVE);

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        // 空值放行，由其他注解（如 @NotBlank）负责非空校验
        if (StrUtil.isBlank(value)) {
            return true;
        }
        // 先用 Hutool 做基础合法性校验
        try {
            URLUtil.url(value);
        } catch (Exception e) {
            return false;
        }
        // 再用正则严格校验 http/https 协议
        return URL_PATTERN.matcher(value).matches();
    }
}

package com.garlic.shortlink.common.ratelimit;

import com.garlic.shortlink.common.exception.BizException;
import com.garlic.shortlink.common.exception.ErrorCode;
import com.garlic.shortlink.common.util.IpUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;

/**
 * 限流 AOP 切面。
 *
 * <p>拦截标注了 {@link RateLimit} 注解的方法，根据 {@link LimitType} 执行对应限流策略：</p>
 * <ul>
 *   <li>{@link LimitType#INTERFACE}：调用 {@link TokenBucketRateLimiter} 令牌桶限流；</li>
 *   <li>{@link LimitType#USER}：调用 {@link SlidingWindowRateLimiter} 用户级滑动窗口限流；</li>
 *   <li>{@link LimitType#IP}：调用 {@link SlidingWindowRateLimiter} IP 级滑动窗口限流。</li>
 * </ul>
 *
 * <p>支持 SpEL 表达式解析限流 key，可引用方法参数（如 {@code "#req.userId"}）。
 * 限流失败时抛出 {@link BizException}（错误码 {@link ErrorCode#RATE_LIMITED}）。</p>
 *
 * @author garlic
 */
@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class RateLimitAspect {

    /** 注入令牌桶限流器（接口级） */
    private final TokenBucketRateLimiter tokenBucketRateLimiter;

    /** 注入滑动窗口限流器（用户级/IP级） */
    private final SlidingWindowRateLimiter slidingWindowRateLimiter;

    /** SpEL 表达式解析器 */
    private final SpelExpressionParser spelParser = new SpelExpressionParser();

    /** 方法参数名发现器，用于将 SpEL 中的 #参数名 映射到实际参数值 */
    private final DefaultParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();

    /**
     * 环绕通知：执行限流检查。
     *
     * @param joinPoint 切入点
     * @param rateLimit 限流注解
     * @return 原方法返回值
     * @throws Throwable 原方法抛出的异常，或限流触发的 {@link BizException}
     */
    @Around("@annotation(rateLimit)")
    public Object around(ProceedingJoinPoint joinPoint, RateLimit rateLimit) throws Throwable {
        // 解析限流 key
        String key = resolveKey(joinPoint, rateLimit);

        // 按 type 执行限流
        boolean allowed;
        switch (rateLimit.type()) {
            case INTERFACE:
                allowed = tokenBucketRateLimiter.tryAcquire(key, rateLimit.capacity(), rateLimit.rate());
                break;
            case USER:
                allowed = slidingWindowRateLimiter.tryAcquire("user:" + key, rateLimit.windowSize(), rateLimit.maxRequests());
                break;
            case IP:
                allowed = slidingWindowRateLimiter.tryAcquire("ip:" + key, rateLimit.windowSize(), rateLimit.maxRequests());
                break;
            default:
                allowed = true;
                break;
        }

        if (!allowed) {
            log.warn("限流触发：type={}, key={}, message={}", rateLimit.type(), key, rateLimit.message());
            throw new BizException(ErrorCode.RATE_LIMITED, rateLimit.message());
        }

        // 放行，执行原方法
        return joinPoint.proceed();
    }

    /**
     * 解析限流 key。
     *
     * <p>若注解指定了 SpEL 表达式，则解析表达式值；否则按 {@link LimitType} 自动生成：</p>
     * <ul>
     *   <li>INTERFACE → 方法全名（类名.方法名）；</li>
     *   <li>USER → 当前 userId（暂用 1L 模拟登录用户）；</li>
     *   <li>IP → 请求客户端 IP。</li>
     * </ul>
     *
     * @param joinPoint 切入点
     * @param rateLimit 限流注解
     * @return 解析后的限流 key
     */
    private String resolveKey(ProceedingJoinPoint joinPoint, RateLimit rateLimit) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Object[] args = joinPoint.getArgs();

        // 若指定了 SpEL key，解析表达式
        String spelKey = rateLimit.key();
        if (spelKey != null && !spelKey.isEmpty()) {
            EvaluationContext context = buildEvaluationContext(method, args);
            Expression expression = spelParser.parseExpression(spelKey);
            Object value = expression.getValue(context);
            return value != null ? value.toString() : "";
        }

        // key 为空，按 type 自动生成
        switch (rateLimit.type()) {
            case INTERFACE:
                // 方法全名：类名.方法名
                return method.getDeclaringClass().getName() + "." + method.getName();
            case USER:
                // 暂用 1L 模拟登录用户
                return "1";
            case IP:
                return getIpAddr();
            default:
                return method.getName();
        }
    }

    /**
     * 构建 SpEL 求值上下文，将方法参数名与实参绑定。
     *
     * @param method 目标方法
     * @param args   方法实参数组
     * @return SpEL 求值上下文
     */
    private EvaluationContext buildEvaluationContext(Method method, Object[] args) {
        StandardEvaluationContext context = new StandardEvaluationContext();
        String[] parameterNames = parameterNameDiscoverer.getParameterNames(method);
        if (parameterNames != null) {
            for (int i = 0; i < parameterNames.length; i++) {
                context.setVariable(parameterNames[i], args[i]);
            }
        }
        return context;
    }

    /**
     * 从当前请求上下文获取客户端 IP。
     *
     * @return 客户端 IP，无法获取时返回 "unknown"
     */
    private String getIpAddr() {
        ServletRequestAttributes attributes =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes != null) {
            HttpServletRequest request = attributes.getRequest();
            return IpUtils.getIpAddr(request);
        }
        return "unknown";
    }
}

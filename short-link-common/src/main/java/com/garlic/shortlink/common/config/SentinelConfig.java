package com.garlic.shortlink.common.config;

import com.alibaba.csp.sentinel.annotation.aspectj.SentinelResourceAspect;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import javax.annotation.PostConstruct;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Sentinel 流控/熔断配置。
 *
 * <p>注册 {@link SentinelResourceAspect} 使 {@code @SentinelResource} 注解生效；
 * 在 {@code @PostConstruct} 中初始化跳转接口 {@code jumpResource} 的基础熔断规则：</p>
 * <ul>
 *     <li>慢调用比例熔断：RT &gt; 100ms 视为慢调用，比例 &gt; 50% 时熔断 10s；</li>
 *     <li>异常比例熔断：异常比例 &gt; 50% 时熔断 10s。</li>
 * </ul>
 *
 * <p>更细粒度的限流/热点参数规则将在 Task 15 补充。</p>
 *
 * @author garlic
 */
@Configuration
public class SentinelConfig {

    /** 跳转接口资源名（与 @SentinelResource value 对应） */
    private static final String JUMP_RESOURCE = "jumpResource";

    /** 慢调用 RT 阈值（毫秒） */
    private static final double SLOW_RT_THRESHOLD = 100D;

    /** 慢调用比例阈值 */
    private static final double SLOW_RATIO_THRESHOLD = 0.5D;

    /** 异常比例阈值 */
    private static final double EXCEPTION_RATIO_THRESHOLD = 0.5D;

    /** 熔断时长（秒） */
    private static final int CIRCUIT_BREAK_SECONDS = 10;

    /** 最小请求数 */
    private static final int MIN_REQUEST_AMOUNT = 5;

    /** 统计窗口（毫秒） */
    private static final int STAT_INTERVAL_MS = 1000;

    /**
     * 注册 Sentinel 注解 AOP 切面，使 {@code @SentinelResource} 生效。
     *
     * @return SentinelResourceAspect 实例
     */
    @Bean
    public SentinelResourceAspect sentinelResourceAspect() {
        return new SentinelResourceAspect();
    }

    /**
     * 初始化 jumpResource 基础熔断规则。
     *
     * <p>使用 try-catch 包裹：Sentinel 在沙箱/受限环境下写日志文件（{@code .lck}）可能失败，
     * 此处容错处理，保证主应用可正常启动；熔断规则加载失败时仅打印警告。</p>
     */
    @PostConstruct
    public void initDegradeRules() {
        try {
            List<DegradeRule> rules = new ArrayList<>(2);

            // 1. 慢调用比例熔断：RT>100ms 且慢调用比例>50% 触发熔断 10s
            DegradeRule slowCallRule = new DegradeRule();
            slowCallRule.setResource(JUMP_RESOURCE);
            slowCallRule.setGrade(RuleConstant.DEGRADE_GRADE_RT);
            slowCallRule.setCount(SLOW_RT_THRESHOLD);
            slowCallRule.setSlowRatioThreshold(SLOW_RATIO_THRESHOLD);
            slowCallRule.setTimeWindow(CIRCUIT_BREAK_SECONDS);
            slowCallRule.setMinRequestAmount(MIN_REQUEST_AMOUNT);
            slowCallRule.setStatIntervalMs(STAT_INTERVAL_MS);
            rules.add(slowCallRule);

            // 2. 异常比例熔断：异常比例>50% 触发熔断 10s
            DegradeRule exceptionRatioRule = new DegradeRule();
            exceptionRatioRule.setResource(JUMP_RESOURCE);
            exceptionRatioRule.setGrade(RuleConstant.DEGRADE_GRADE_EXCEPTION_RATIO);
            exceptionRatioRule.setCount(EXCEPTION_RATIO_THRESHOLD);
            exceptionRatioRule.setTimeWindow(CIRCUIT_BREAK_SECONDS);
            exceptionRatioRule.setMinRequestAmount(MIN_REQUEST_AMOUNT);
            exceptionRatioRule.setStatIntervalMs(STAT_INTERVAL_MS);
            rules.add(exceptionRatioRule);

            DegradeRuleManager.loadRules(rules);
        } catch (Throwable e) {
            // Sentinel 日志初始化失败（如沙箱禁止 .lck 文件）不应阻塞应用启动
            System.err.println("[SentinelConfig] 熔断规则加载失败，已跳过（不影响应用启动）：" + e.getMessage());
        }
    }
}

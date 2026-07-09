package com.garlic.shortlink.common.sentinel;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.csp.sentinel.init.InitFunc;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Sentinel 规则持久化（文件方式）。
 *
 * <p>通过 Sentinel 的 {@link InitFunc} SPI 机制在客户端初始化阶段加载
 * {@code rules/sentinel-degrade-rules.json} 中定义的熔断规则，
 * 实现规则与代码解耦，便于运维调整。</p>
 *
 * <p>SPI 注册文件：{@code META-INF/services/com.alibaba.csp.sentinel.init.InitFunc}。</p>
 *
 * <p>说明：</p>
 * <ul>
 *     <li>{@code @Component} 使 Spring 也可扫描到本类（InitFunc 由 Sentinel SPI 加载，非 Spring 容器调用）；</li>
 *     <li>{@code @Slf4j} 由 Lombok 编译期生成静态 log 字段，SPI 加载时可用；</li>
 *     <li>static 块设置 {@code project.name} 等系统属性，需在 Sentinel Transport 初始化前执行。</li>
 * </ul>
 *
 * @author garlic
 */
@Slf4j
@Component
public class SentinelRulePersistence implements InitFunc {

    /** 规则文件 classpath 路径 */
    private static final String RULE_FILE_PATH = "rules/sentinel-degrade-rules.json";

    static {
        // 设置 Sentinel 项目名（Transport/API 上报标识，需在 Sentinel 初始化前设置）
        System.setProperty("project.name", "short-link-service");
        // Sentinel Dashboard 地址（如需接入 Dashboard，取消下行注释并启动 Dashboard 服务）
        // System.setProperty("csp.sentinel.dashboard.server", "localhost:8858");
        // 客户端与 Dashboard 心跳间隔（毫秒，默认 10s）
        // System.setProperty("csp.sentinel.heartbeat.interval.ms", "10000");
    }

    @Override
    public void init() throws Exception {
        log.info("SentinelRulePersistence.init() 开始加载熔断规则文件：{}", RULE_FILE_PATH);
        List<DegradeRule> rules = loadRulesFromFile();
        if (rules.isEmpty()) {
            log.warn("熔断规则文件为空或加载失败，将保留 SentinelConfig 中 @PostConstruct 的编程式规则");
            return;
        }
        DegradeRuleManager.loadRules(rules);
        log.info("Sentinel 熔断规则加载完成，共 {} 条", rules.size());
    }

    /**
     * 从 classpath 读取 JSON 规则文件并解析为 {@link DegradeRule} 列表。
     *
     * @return 熔断规则列表
     */
    private List<DegradeRule> loadRulesFromFile() {
        List<DegradeRule> rules = new ArrayList<>();
        try (InputStream in = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(RULE_FILE_PATH)) {
            if (in == null) {
                log.warn("未找到 Sentinel 规则文件：{}", RULE_FILE_PATH);
                return rules;
            }
            String json = IoUtil.read(in, CharsetUtil.CHARSET_UTF_8);
            JSONArray array = JSONUtil.parseArray(json);
            for (int i = 0; i < array.size(); i++) {
                JSONObject obj = array.getJSONObject(i);
                DegradeRule rule = parseRule(obj);
                if (rule != null) {
                    rules.add(rule);
                }
            }
        } catch (Exception e) {
            log.error("加载 Sentinel 熔断规则文件失败：{}", RULE_FILE_PATH, e);
        }
        return rules;
    }

    /**
     * 解析单个规则 JSON 对象为 {@link DegradeRule}。
     *
     * @param obj JSON 对象
     * @return DegradeRule，解析失败返回 null
     */
    private DegradeRule parseRule(JSONObject obj) {
        try {
            DegradeRule rule = new DegradeRule();
            rule.setResource(obj.getStr("resource"));
            rule.setGrade(obj.getInt("grade"));
            rule.setCount(obj.getDouble("count"));
            // 慢调用比例熔断专用
            if (obj.containsKey("slowRatioThreshold")) {
                rule.setSlowRatioThreshold(obj.getDouble("slowRatioThreshold"));
            }
            rule.setTimeWindow(obj.getInt("timeWindow"));
            rule.setMinRequestAmount(obj.getInt("minRequestAmount"));
            rule.setStatIntervalMs(obj.getInt("statIntervalMs"));
            log.info("解析熔断规则：resource={}, grade={}({}), count={}, timeWindow={}s",
                    rule.getResource(), rule.getGrade(), gradeDesc(rule.getGrade()),
                    rule.getCount(), rule.getTimeWindow());
            return rule;
        } catch (Exception e) {
            log.error("解析熔断规则失败：{}", obj, e);
            return null;
        }
    }

    /**
     * 熔断策略中文描述（仅用于日志）。
     *
     * @param grade 熔断策略
     * @return 中文描述
     */
    private String gradeDesc(int grade) {
        switch (grade) {
            case RuleConstant.DEGRADE_GRADE_RT:
                return "慢调用比例";
            case RuleConstant.DEGRADE_GRADE_EXCEPTION_RATIO:
                return "异常比例";
            case RuleConstant.DEGRADE_GRADE_EXCEPTION_COUNT:
                return "异常数";
            default:
                return "未知";
        }
    }
}

package com.garlic.shortlink.service.shortcode;

import com.garlic.shortlink.common.exception.BizException;
import com.garlic.shortlink.common.exception.ErrorCode;
import com.garlic.shortlink.common.util.Base62Utils;
import com.garlic.shortlink.common.util.SnowflakeIdWorker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.function.Predicate;

/**
 * 短码生成器。
 *
 * <p>基于雪花算法生成全局唯一 ID，再通过 62 进制编码转换为 6~7 位短码。</p>
 *
 * <p>提供两种生成方式：</p>
 * <ul>
 *     <li>{@link #generateShortCode()}：直接生成短码，不检查冲突；</li>
 *     <li>{@link #generateShortCodeWithConflictCheck(Predicate)}：生成时通过
 *     existsChecker 校验是否已存在（如查 Redis/DB），最多重试 3 次，全部冲突则抛出
 *     {@link BizException}，用于后续 Task 7 在写入前检查冲突。</li>
 * </ul>
 *
 * @author garlic
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShortCodeGenerator {

    /** 冲突重试最大次数 */
    private static final int MAX_RETRY = 3;

    /** 注入雪花算法 ID 生成器 */
    private final SnowflakeIdWorker snowflakeIdWorker;

    /**
     * 生成短码（不做冲突检查）。
     *
     * <p>流程：</p>
     * <ol>
     *     <li>调用 {@link SnowflakeIdWorker#nextId()} 获取 long 类型 ID；</li>
     *     <li>调用 {@link Base62Utils#encode(long)} 转为 6~7 位短码；</li>
     *     <li>返回短码。</li>
     * </ol>
     *
     * @return 6~7 位 62 进制短码
     */
    public String generateShortCode() {
        long id = snowflakeIdWorker.nextId();
        String shortCode = Base62Utils.encode(id);
        log.debug("生成短码：id={}, shortCode={}", id, shortCode);
        return shortCode;
    }

    /**
     * 生成短码并进行冲突检查。
     *
     * <p>循环调用 {@link #generateShortCode()} 生成短码，并使用 existsChecker 校验是否
     * 已存在（如查 Redis/DB），若存在则重试，最多重试 {@value #MAX_RETRY} 次。</p>
     *
     * @param existsChecker 冲突检查函数：返回 true 表示短码已存在，需要重试
     * @return 未冲突的短码
     * @throws BizException 当连续 {@value #MAX_RETRY} 次生成均冲突时抛出
     *                      （错误码 {@link ErrorCode#SYSTEM_ERROR}）
     */
    public String generateShortCodeWithConflictCheck(Predicate<String> existsChecker) {
        for (int attempt = 1; attempt <= MAX_RETRY; attempt++) {
            String shortCode = generateShortCode();
            if (!existsChecker.test(shortCode)) {
                log.debug("短码未冲突：shortCode={}, attempt={}", shortCode, attempt);
                return shortCode;
            }
            log.warn("短码冲突，重试：shortCode={}, attempt={}/{}", shortCode, attempt, MAX_RETRY);
        }
        log.error("短码生成冲突，连续 {} 次均存在冲突", MAX_RETRY);
        throw new BizException(ErrorCode.SYSTEM_ERROR, "短码生成冲突");
    }
}

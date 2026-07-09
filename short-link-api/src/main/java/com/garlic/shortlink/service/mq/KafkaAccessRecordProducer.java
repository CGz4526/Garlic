package com.garlic.shortlink.service.mq;

import com.garlic.shortlink.common.constant.ShortLinkConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Executor;

/**
 * Kafka 访问记录生产者：后台线程消费本地队列并发送到 Kafka。
 *
 * <p>核心职责：从 {@link AccessRecordQueue} 阻塞出队访问记录 JSON，异步发送到 Kafka topic
 * {@link ShortLinkConstants#TOPIC_ACCESS_RECORD}，实现"跳转接口 ←→ Kafka 上报"的解耦。</p>
 *
 * <p>设计要点：</p>
 * <ul>
 *   <li>单线程守护线程：当前实现为单线程消费，简化并发控制；</li>
 *   <li>异步 send：{@code kafkaTemplate.send()} 本身为异步（返回 ListenableFuture），
 *       配合 acks=all + retries=3 保证可靠性；</li>
 *   <li>异常隔离：发送失败仅记录日志，不阻塞队列消费（丢弃策略 + 日志告警）；</li>
 *   <li>优雅关闭：{@code @PreDestroy} 设置 running=false 并中断线程，排空剩余消息（最多等 5 秒）。</li>
 * </ul>
 *
 * <p>线程池扩展：注入 {@code statsExecutor}（统计异步上报专用线程池），
 * 当前主消费循环仍由单线程守护线程驱动（保证顺序消费 + 简化关闭流程），
 * 未来可扩展为：主线程仅负责出队，{@code doSend} 提交到 {@code statsExecutor} 并行发送，
 * 提升消费并行度。{@code statsExecutor} 采用 CallerRunsPolicy 反压，避免过载。</p>
 *
 * <p>降级策略：Kafka 不可用时，发送失败仅记录日志，不影响跳转接口可用性。
 * 实际生产可落盘到本地文件，后续补偿。</p>
 *
 * @author garlic
 */
@Slf4j
@Component
public class KafkaAccessRecordProducer {

    /** 注入本地内存队列（访问记录缓冲） */
    private final AccessRecordQueue accessRecordQueue;

    /** 注入 KafkaTemplate（发送访问记录到 Kafka） */
    private final KafkaTemplate<String, String> kafkaTemplate;

    /** 统计异步上报专用线程池（用于扩展：并行发送访问记录） */
    private final Executor statsExecutor;

    /** 控制后台线程运行状态 */
    private volatile boolean running = true;

    /** 后台发送线程引用（用于优雅关闭） */
    private Thread producerThread;

    /** 优雅关闭最大等待时间（毫秒，5 秒） */
    private static final long SHUTDOWN_TIMEOUT_MS = 5000L;

    /**
     * 构造函数注入：通过 @Qualifier 指定使用 statsExecutor 线程池。
     *
     * @param accessRecordQueue 本地内存队列（访问记录缓冲）
     * @param kafkaTemplate     KafkaTemplate（发送访问记录到 Kafka）
     * @param statsExecutor     统计异步上报专用线程池
     */
    public KafkaAccessRecordProducer(AccessRecordQueue accessRecordQueue,
                                     KafkaTemplate<String, String> kafkaTemplate,
                                     @Qualifier("statsExecutor") Executor statsExecutor) {
        this.accessRecordQueue = accessRecordQueue;
        this.kafkaTemplate = kafkaTemplate;
        this.statsExecutor = statsExecutor;
    }

    /**
     * 启动后台守护线程：循环消费本地队列并发送到 Kafka。
     *
     * <p>线程命名 {@code kafka-access-record-producer}，设置为守护线程（daemon），
     * 确保 JVM 退出时不会被阻塞。</p>
     */
    @PostConstruct
    public void start() {
        producerThread = new Thread(this::runLoop, "kafka-access-record-producer");
        producerThread.setDaemon(true);
        producerThread.start();
        log.info("Kafka 访问记录后台生产者线程已启动：{}", producerThread.getName());
    }

    /**
     * 后台消费主循环：阻塞出队 → 发送 Kafka。
     *
     * <p>循环逻辑：</p>
     * <ol>
     *   <li>{@code queue.take()} 阻塞出队（队列空时挂起，不消耗 CPU）；</li>
     *   <li>{@code kafkaTemplate.send(topic, json)} 异步发送到 Kafka；</li>
     *   <li>发送失败记录日志，继续消费下一条（不阻塞队列消费）。</li>
     * </ol>
     *
     * <p>异常处理：</p>
     * <ul>
     *   <li>{@link InterruptedException}：优雅关闭时被中断，退出循环并排空剩余消息；</li>
     *   <li>其他异常：发送失败记录日志，继续下一条（Kafka 不可用时降级，仅日志告警）。</li>
     * </ul>
     */
    private void runLoop() {
        // Task20 已实现：已注入 statsExecutor 线程池（统计异步上报专用）
        // 当前主消费循环仍由单线程守护线程驱动，保证顺序消费 + 简化优雅关闭
        // 扩展点：可将 doSend(json) 提交到 statsExecutor.submit() 并行发送，提升消费并行度
        // statsExecutor 配置：核心 4 / 最大 8 / 队列 1000 / CallerRunsPolicy（反压保护）
        while (running) {
            try {
                String json = accessRecordQueue.take();
                doSend(json);
            } catch (InterruptedException e) {
                // 优雅关闭时被中断（shutdown 调用 interrupt），退出主循环
                Thread.currentThread().interrupt();
                log.info("Kafka 访问记录后台线程被中断，退出主循环");
                break;
            } catch (Exception e) {
                // 发送失败记录日志，不阻塞队列消费，继续下一条
                // Kafka 不可用时降级：实际生产可落盘到本地文件，后续补偿
                log.error("Kafka 访问记录发送异常，继续消费下一条", e);
            }
        }
        // 退出前排空队列剩余消息
        drainRemaining();
    }

    /**
     * 优雅关闭时排空队列剩余消息。
     *
     * <p>主循环退出后，队列中可能还有未发送的访问记录。使用 {@link AccessRecordQueue#poll()}
     * 非阻塞出队，逐条发送，直到队列清空。</p>
     */
    private void drainRemaining() {
        long drained = 0L;
        String json;
        while ((json = accessRecordQueue.poll()) != null) {
            try {
                doSend(json);
                drained++;
            } catch (Exception e) {
                log.error("排空剩余访问记录时发送失败：{}", json, e);
            }
        }
        log.info("Kafka 访问记录后台线程剩余消息排空完成，共发送 {} 条", drained);
    }

    /**
     * 实际发送单条访问记录到 Kafka（内部方法）。
     *
     * <p>调用 {@code kafkaTemplate.send(topic, json)} 异步发送，并通过回调记录异步失败日志：
     * <ul>
     *   <li>同步异常（如 Producer 已关闭、序列化失败）→ 抛出，由上层 catch 处理；</li>
     *   <li>异步异常（如 Broker 不可达、超时）→ 通过 ListenableFutureCallback 记录日志。</li>
     * </ul>
     *
     * @param json 访问记录 JSON 字符串
     */
    private void doSend(String json) {
        ListenableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(ShortLinkConstants.TOPIC_ACCESS_RECORD, json);
        // 添加回调：异步发送失败时记录日志（Kafka 不可用时降级）
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                // 发送成功，无需处理（避免高频日志）
            }

            @Override
            public void onFailure(Throwable ex) {
                // 异步发送失败：Kafka 不可用或超时
                // 降级策略：仅记录日志，实际生产可落盘到本地文件后续补偿
                log.error("Kafka 访问记录异步发送失败：{}", json, ex);
            }
        });
    }

    /**
     * 直接发送访问记录到 Kafka（用于测试）。
     *
     * <p>绕过本地队列，直接调用 {@code kafkaTemplate.send}，便于单元测试验证 Kafka 连通性。</p>
     *
     * @param json 访问记录 JSON 字符串
     */
    public void send(String json) {
        doSend(json);
    }

    /**
     * 优雅关闭：停止后台线程并排空剩余消息。
     *
     * <p>关闭流程：</p>
     * <ol>
     *   <li>设置 {@code running = false}，主循环在下一次条件检查时退出；</li>
     *   <li>中断后台线程（唤醒可能在 {@code take()} 阻塞的线程）；</li>
     *   <li>等待线程结束（最多 5 秒），超时则强制退出。</li>
     * </ol>
     */
    @PreDestroy
    public void shutdown() {
        log.info("开始优雅关闭 Kafka 访问记录后台线程，剩余消息数：{}", accessRecordQueue.size());
        running = false;
        if (producerThread != null) {
            // 中断线程：唤醒可能在 queue.take() 阻塞的线程
            producerThread.interrupt();
            try {
                // 等待队列剩余数据发送完（最多等 5 秒）
                producerThread.join(SHUTDOWN_TIMEOUT_MS);
                if (producerThread.isAlive()) {
                    log.warn("Kafka 访问记录后台线程在 {}ms 内未结束，强制退出", SHUTDOWN_TIMEOUT_MS);
                } else {
                    log.info("Kafka 访问记录后台线程已优雅退出");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("等待 Kafka 访问记录后台线程退出时被中断");
            }
        }
    }
}

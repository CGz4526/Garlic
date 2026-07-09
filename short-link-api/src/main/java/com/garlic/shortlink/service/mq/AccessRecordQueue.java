package com.garlic.shortlink.service.mq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 访问记录本地内存队列缓冲。
 *
 * <p>作为短链跳转接口与 Kafka 之间的缓冲层，解耦"高 RT 跳转请求"与"Kafka 上报"：</p>
 * <ul>
 *   <li>跳转接口通过 {@link #offer(String)} 非阻塞入队，O(1) 返回，不影响跳转 RT；</li>
 *   <li>后台线程通过 {@link #take()} 阻塞出队，批量发送到 Kafka（{@link KafkaAccessRecordProducer}）；</li>
 *   <li>队列满时直接丢弃并记录日志，实现削峰填谷，防止 Kafka 阻塞拖垮跳转接口。</li>
 * </ul>
 *
 * <p>设计说明：</p>
 * <ul>
 *   <li>容量 10000：单机峰值缓冲，超出部分丢弃（实际生产可落盘补偿）；</li>
 *   <li>{@code volatile running}：控制后台线程优雅退出；</li>
 *   <li>使用 {@link ArrayBlockingQueue}：基于数组的有界阻塞队列，内存连续、cache 友好。</li>
 * </ul>
 *
 * @author garlic
 */
@Slf4j
@Component
public class AccessRecordQueue {

    /** 本地内存队列缓冲，容量 10000（单机峰值缓冲上限） */
    private final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(10000);

    /** 控制后台线程运行状态（volatile 保证多线程可见性） */
    private volatile boolean running = true;

    /**
     * 非阻塞入队：将访问记录 JSON 投递到本地队列。
     *
     * <p>使用 {@link ArrayBlockingQueue#offer(Object)} 非阻塞入队：</p>
     * <ul>
     *   <li>队列未满 → 入队成功，返回 true；</li>
     *   <li>队列已满 → 入队失败，记录日志并丢弃当前记录（防止反压阻塞跳转线程），返回 false。</li>
     * </ul>
     *
     * <p>注：队列满时丢弃是"有损"策略，实际生产可落盘到本地文件后续补偿，
     * 此处简化处理优先保证跳转接口可用性。</p>
     *
     * @param accessRecordJson 访问记录 JSON 字符串
     * @return true 表示入队成功，false 表示队列已满被丢弃
     */
    public boolean offer(String accessRecordJson) {
        boolean success = queue.offer(accessRecordJson);
        if (!success) {
            // 队列已满，丢弃当前记录，防止反压阻塞跳转线程
            // 实际生产可落盘到本地文件，后续补偿
            log.warn("访问记录队列已满（capacity=10000），丢弃当前记录：{}", accessRecordJson);
        }
        return success;
    }

    /**
     * 阻塞出队：从本地队列取出一条访问记录 JSON。
     *
     * <p>使用 {@link ArrayBlockingQueue#take()} 阻塞出队，队列空时挂起当前线程直到有数据入队。
     * 仅由后台消费线程（{@link KafkaAccessRecordProducer}）调用。</p>
     *
     * @return 访问记录 JSON 字符串
     * @throws InterruptedException 后台线程被中断（优雅关闭时）
     */
    public String take() throws InterruptedException {
        return queue.take();
    }

    /**
     * 非阻塞出队：从本地队列取出一条访问记录 JSON（不阻塞）。
     *
     * <p>用于优雅关闭时排空剩余消息：{@link ArrayBlockingQueue#poll()} 立即返回，
     * 队列为空返回 null。</p>
     *
     * @return 访问记录 JSON 字符串，队列空时返回 null
     */
    public String poll() {
        return queue.poll();
    }

    /**
     * 获取当前队列中未消费的消息数量。
     *
     * @return 队列大小
     */
    public int size() {
        return queue.size();
    }

    /**
     * 判断队列是否仍在运行（后台线程是否应继续消费）。
     *
     * @return true 表示运行中
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * 关闭队列：通知后台线程停止消费。
     *
     * <p>仅设置 {@code running = false}，不强制清空队列；
     * 后台线程的优雅排空逻辑由 {@link KafkaAccessRecordProducer#shutdown()} 负责。</p>
     */
    @PreDestroy
    public void shutdown() {
        running = false;
        log.info("访问记录本地队列已标记关闭，剩余未消费消息数：{}", queue.size());
    }
}

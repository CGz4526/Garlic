package com.garlic.shortlink.common.util;

/**
 * 雪花算法 ID 生成器。
 *
 * <p>结构：1 位符号位 + 41 位时间戳 + 5 位数据中心 ID + 5 位工作节点 ID + 12 位序列号。</p>
 * <p>起始时间（twepoch）固定为 2024-01-01 00:00:00，可用约 69 年。</p>
 *
 * @author garlic
 */
public class SnowflakeIdWorker {

    /** 起始时间戳：2024-01-01 00:00:00 UTC（毫秒） */
    private static final long TWEPOCH = 1704067200000L;

    /** 工作节点 ID 位数 */
    private static final long WORKER_ID_BITS = 5L;

    /** 数据中心 ID 位数 */
    private static final long DATACENTER_ID_BITS = 5L;

    /** 序列号位数 */
    private static final long SEQUENCE_BITS = 12L;

    /** 工作节点 ID 最大值：31 */
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);

    /** 数据中心 ID 最大值：31 */
    private static final long MAX_DATACENTER_ID = ~(-1L << DATACENTER_ID_BITS);

    /** 序列号掩码：4095 */
    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

    /** 工作节点 ID 左移位数：12 */
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;

    /** 数据中心 ID 左移位数：17 */
    private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;

    /** 时间戳左移位数：22 */
    private static final long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;

    /** 工作节点 ID */
    private final long workerId;

    /** 数据中心 ID */
    private final long datacenterId;

    /** 当前序列号 */
    private long sequence = 0L;

    /** 上次生成 ID 的时间戳 */
    private long lastTimestamp = -1L;

    /**
     * 构造雪花算法 ID 生成器。
     *
     * @param workerId     工作节点 ID（0 ~ 31）
     * @param datacenterId 数据中心 ID（0 ~ 31）
     */
    public SnowflakeIdWorker(long workerId, long datacenterId) {
        if (workerId > MAX_WORKER_ID || workerId < 0) {
            throw new IllegalArgumentException(
                    String.format("workerId 不能大于 %d 或小于 0", MAX_WORKER_ID));
        }
        if (datacenterId > MAX_DATACENTER_ID || datacenterId < 0) {
            throw new IllegalArgumentException(
                    String.format("datacenterId 不能大于 %d 或小于 0", MAX_DATACENTER_ID));
        }
        this.workerId = workerId;
        this.datacenterId = datacenterId;
    }

    /**
     * 生成下一个 ID（线程安全）。
     *
     * @return 雪花 ID
     */
    public synchronized long nextId() {
        long currentTimestamp = timeGen();

        // 时钟回拨检测
        if (currentTimestamp < lastTimestamp) {
            throw new IllegalStateException(
                    String.format("时钟回拨，拒绝生成 ID %d 毫秒", lastTimestamp - currentTimestamp));
        }

        if (currentTimestamp == lastTimestamp) {
            // 同一毫秒内，序列号递增
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                // 序列号溢出，等待下一毫秒
                currentTimestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            // 不同毫秒，序列号重置
            sequence = 0L;
        }

        lastTimestamp = currentTimestamp;

        return ((currentTimestamp - TWEPOCH) << TIMESTAMP_LEFT_SHIFT)
                | (datacenterId << DATACENTER_ID_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    /**
     * 阻塞等待到下一毫秒。
     *
     * @param lastTimestamp 上次时间戳
     * @return 下一毫秒时间戳
     */
    private long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    /**
     * 获取当前时间戳。
     *
     * @return 当前时间戳（毫秒）
     */
    private long timeGen() {
        return System.currentTimeMillis();
    }
}

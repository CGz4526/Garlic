-- =============================================================================
-- Garlic 短链接服务 - 建库建表脚本
-- MySQL 8.0 / InnoDB / utf8mb4
--
-- 分库分表规划（ShardingSphere）:
--   t_short_link          4库 × 4表 = 16 分片，按 short_code 哈希分库分表
--   t_link_access_stats   4库 × 4表 = 16 分片，按 short_code 哈希分表
--   t_link_access_record  按月份自动分表（行表达式），每库一张逻辑表
--   t_user / t_group      广播表，每个库结构数据完全一致
-- =============================================================================

-- ============================ 创建 4 个数据库 ================================
CREATE DATABASE IF NOT EXISTS link_db_0 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
CREATE DATABASE IF NOT EXISTS link_db_1 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
CREATE DATABASE IF NOT EXISTS link_db_2 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
CREATE DATABASE IF NOT EXISTS link_db_3 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;


-- =============================================================================
-- link_db_0：完整建表 DDL（其余库通过 CREATE TABLE ... LIKE 复制结构）
-- =============================================================================
USE link_db_0;

-- ----------------------- t_short_link 分表 0~3 -------------------------------
CREATE TABLE IF NOT EXISTS t_short_link_0 (
    id            BIGINT        NOT NULL                COMMENT '雪花ID',
    short_code    VARCHAR(16)   NOT NULL                COMMENT '短码（分片键）',
    original_url  VARCHAR(2048) NOT NULL                COMMENT '原始长链',
    user_id       BIGINT        NOT NULL                COMMENT '用户ID',
    group_id      BIGINT        DEFAULT NULL            COMMENT '分组ID',
    expire_time   DATETIME      DEFAULT NULL            COMMENT '过期时间，NULL表示永不过期',
    `describe`    VARCHAR(128)  DEFAULT NULL            COMMENT '描述',
    total_pv      BIGINT        DEFAULT 0               COMMENT '总访问PV',
    total_uv      BIGINT        DEFAULT 0               COMMENT '总访问UV',
    status        TINYINT       DEFAULT 0               COMMENT '0正常 1禁用',
    del_flag      TINYINT       DEFAULT 0               COMMENT '0未删除 1已删除',
    create_time   DATETIME      DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time   DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    KEY idx_short_code (short_code),
    KEY idx_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='短链表-分片0';

CREATE TABLE IF NOT EXISTS t_short_link_1 LIKE t_short_link_0;
CREATE TABLE IF NOT EXISTS t_short_link_2 LIKE t_short_link_0;
CREATE TABLE IF NOT EXISTS t_short_link_3 LIKE t_short_link_0;

-- ----------------------- t_link_access_record 逻辑表 -------------------------
CREATE TABLE IF NOT EXISTS t_link_access_record (
    id               BIGINT       NOT NULL                COMMENT '雪花ID',
    short_code       VARCHAR(16)  NOT NULL                COMMENT '短码',
    user_identifier  VARCHAR(64)  DEFAULT NULL            COMMENT 'UV去重标识',
    ip               VARCHAR(64)  DEFAULT NULL            COMMENT 'IP地址',
    browser          VARCHAR(32)  DEFAULT NULL            COMMENT '浏览器',
    os               VARCHAR(32)  DEFAULT NULL            COMMENT '操作系统',
    device           VARCHAR(32)  DEFAULT NULL            COMMENT '设备类型',
    network          VARCHAR(32)  DEFAULT NULL            COMMENT '网络类型',
    locale           VARCHAR(64)  DEFAULT NULL            COMMENT '地域',
    access_time      DATETIME     NOT NULL                COMMENT '访问时间',
    create_time      DATETIME     DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    KEY idx_short_code (short_code),
    KEY idx_access_time (access_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='访问明细表（按月份分表）';

-- ----------------------- t_link_access_stats 分表 0~3 ------------------------
CREATE TABLE IF NOT EXISTS t_link_access_stats_0 (
    id            BIGINT       NOT NULL                COMMENT '雪花ID',
    short_code    VARCHAR(16)  NOT NULL                COMMENT '短码',
    stats_date    DATE         NOT NULL                COMMENT '统计日期',
    hour          TINYINT      DEFAULT NULL            COMMENT '小时0-23，NULL表示天汇总',
    pv            INT          DEFAULT 0               COMMENT 'PV',
    uv            INT          DEFAULT 0               COMMENT 'UV',
    ip_count      INT          DEFAULT 0               COMMENT 'IP数',
    locale        VARCHAR(64)  DEFAULT NULL            COMMENT '地域，NULL表示全量',
    browser       VARCHAR(32)  DEFAULT NULL            COMMENT '浏览器',
    os            VARCHAR(32)  DEFAULT NULL            COMMENT '操作系统',
    device        VARCHAR(32)  DEFAULT NULL            COMMENT '设备类型',
    network       VARCHAR(32)  DEFAULT NULL            COMMENT '网络类型',
    create_time   DATETIME     DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    KEY idx_short_code (short_code),
    UNIQUE KEY uk_code_date_hour (short_code, stats_date, hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='访问统计表-分片0';

CREATE TABLE IF NOT EXISTS t_link_access_stats_1 LIKE t_link_access_stats_0;
CREATE TABLE IF NOT EXISTS t_link_access_stats_2 LIKE t_link_access_stats_0;
CREATE TABLE IF NOT EXISTS t_link_access_stats_3 LIKE t_link_access_stats_0;

-- ----------------------- t_user 广播表 ---------------------------------------
CREATE TABLE IF NOT EXISTS t_user (
    id            BIGINT       NOT NULL                COMMENT '雪花ID',
    username      VARCHAR(32)  NOT NULL                COMMENT '用户名',
    password      VARCHAR(64)  NOT NULL                COMMENT '密码',
    real_name     VARCHAR(32)  DEFAULT NULL            COMMENT '真实姓名',
    phone         VARCHAR(16)  DEFAULT NULL            COMMENT '手机号',
    email         VARCHAR(64)  DEFAULT NULL            COMMENT '邮箱',
    status        TINYINT      DEFAULT 0               COMMENT '状态：0正常 1禁用',
    del_flag      TINYINT      DEFAULT 0               COMMENT '删除标志：0未删除 1已删除',
    create_time   DATETIME     DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time   DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表（广播表）';

-- ----------------------- t_group 广播表 --------------------------------------
CREATE TABLE IF NOT EXISTS t_group (
    id            BIGINT       NOT NULL                COMMENT '雪花ID',
    name          VARCHAR(32)  NOT NULL                COMMENT '分组名称',
    user_id       BIGINT       NOT NULL                COMMENT '用户ID',
    sort_order    INT          DEFAULT 0               COMMENT '排序序号',
    create_time   DATETIME     DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time   DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='短链分组表（广播表）';


-- =============================================================================
-- link_db_1：通过 CREATE TABLE ... LIKE 复制 link_db_0 的表结构
-- =============================================================================
USE link_db_1;

CREATE TABLE IF NOT EXISTS t_short_link_0 LIKE link_db_0.t_short_link_0;
CREATE TABLE IF NOT EXISTS t_short_link_1 LIKE link_db_0.t_short_link_1;
CREATE TABLE IF NOT EXISTS t_short_link_2 LIKE link_db_0.t_short_link_2;
CREATE TABLE IF NOT EXISTS t_short_link_3 LIKE link_db_0.t_short_link_3;

CREATE TABLE IF NOT EXISTS t_link_access_record LIKE link_db_0.t_link_access_record;

CREATE TABLE IF NOT EXISTS t_link_access_stats_0 LIKE link_db_0.t_link_access_stats_0;
CREATE TABLE IF NOT EXISTS t_link_access_stats_1 LIKE link_db_0.t_link_access_stats_1;
CREATE TABLE IF NOT EXISTS t_link_access_stats_2 LIKE link_db_0.t_link_access_stats_2;
CREATE TABLE IF NOT EXISTS t_link_access_stats_3 LIKE link_db_0.t_link_access_stats_3;

CREATE TABLE IF NOT EXISTS t_user LIKE link_db_0.t_user;
CREATE TABLE IF NOT EXISTS t_group LIKE link_db_0.t_group;


-- =============================================================================
-- link_db_2：通过 CREATE TABLE ... LIKE 复制 link_db_0 的表结构
-- =============================================================================
USE link_db_2;

CREATE TABLE IF NOT EXISTS t_short_link_0 LIKE link_db_0.t_short_link_0;
CREATE TABLE IF NOT EXISTS t_short_link_1 LIKE link_db_0.t_short_link_1;
CREATE TABLE IF NOT EXISTS t_short_link_2 LIKE link_db_0.t_short_link_2;
CREATE TABLE IF NOT EXISTS t_short_link_3 LIKE link_db_0.t_short_link_3;

CREATE TABLE IF NOT EXISTS t_link_access_record LIKE link_db_0.t_link_access_record;

CREATE TABLE IF NOT EXISTS t_link_access_stats_0 LIKE link_db_0.t_link_access_stats_0;
CREATE TABLE IF NOT EXISTS t_link_access_stats_1 LIKE link_db_0.t_link_access_stats_1;
CREATE TABLE IF NOT EXISTS t_link_access_stats_2 LIKE link_db_0.t_link_access_stats_2;
CREATE TABLE IF NOT EXISTS t_link_access_stats_3 LIKE link_db_0.t_link_access_stats_3;

CREATE TABLE IF NOT EXISTS t_user LIKE link_db_0.t_user;
CREATE TABLE IF NOT EXISTS t_group LIKE link_db_0.t_group;


-- =============================================================================
-- link_db_3：通过 CREATE TABLE ... LIKE 复制 link_db_0 的表结构
-- =============================================================================
USE link_db_3;

CREATE TABLE IF NOT EXISTS t_short_link_0 LIKE link_db_0.t_short_link_0;
CREATE TABLE IF NOT EXISTS t_short_link_1 LIKE link_db_0.t_short_link_1;
CREATE TABLE IF NOT EXISTS t_short_link_2 LIKE link_db_0.t_short_link_2;
CREATE TABLE IF NOT EXISTS t_short_link_3 LIKE link_db_0.t_short_link_3;

CREATE TABLE IF NOT EXISTS t_link_access_record LIKE link_db_0.t_link_access_record;

CREATE TABLE IF NOT EXISTS t_link_access_stats_0 LIKE link_db_0.t_link_access_stats_0;
CREATE TABLE IF NOT EXISTS t_link_access_stats_1 LIKE link_db_0.t_link_access_stats_1;
CREATE TABLE IF NOT EXISTS t_link_access_stats_2 LIKE link_db_0.t_link_access_stats_2;
CREATE TABLE IF NOT EXISTS t_link_access_stats_3 LIKE link_db_0.t_link_access_stats_3;

CREATE TABLE IF NOT EXISTS t_user LIKE link_db_0.t_user;
CREATE TABLE IF NOT EXISTS t_group LIKE link_db_0.t_group;

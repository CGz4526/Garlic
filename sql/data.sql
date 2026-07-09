-- =============================================================================
-- Garlic 短链接服务 - 初始化测试数据
--
-- 说明：
--   t_user / t_group 为广播表，需在每个库中插入相同数据保持一致。
--   密码使用 123456 的 MD5 值：e10adc3949ba59abbe56e057f20f883e
-- =============================================================================

-- ----------------------------- link_db_0 -------------------------------------
USE link_db_0;

INSERT INTO t_user (id, username, password, real_name, phone, email, status, del_flag)
VALUES (1, 'admin', 'e10adc3949ba59abbe56e057f20f883e', '管理员', '13800000000', 'admin@garlic.com', 0, 0);

INSERT INTO t_group (id, name, user_id, sort_order) VALUES (1, '默认分组', 1, 0);
INSERT INTO t_group (id, name, user_id, sort_order) VALUES (2, '营销分组', 1, 1);

-- ----------------------------- link_db_1 -------------------------------------
USE link_db_1;

INSERT INTO t_user (id, username, password, real_name, phone, email, status, del_flag)
VALUES (1, 'admin', 'e10adc3949ba59abbe56e057f20f883e', '管理员', '13800000000', 'admin@garlic.com', 0, 0);

INSERT INTO t_group (id, name, user_id, sort_order) VALUES (1, '默认分组', 1, 0);
INSERT INTO t_group (id, name, user_id, sort_order) VALUES (2, '营销分组', 1, 1);

-- ----------------------------- link_db_2 -------------------------------------
USE link_db_2;

INSERT INTO t_user (id, username, password, real_name, phone, email, status, del_flag)
VALUES (1, 'admin', 'e10adc3949ba59abbe56e057f20f883e', '管理员', '13800000000', 'admin@garlic.com', 0, 0);

INSERT INTO t_group (id, name, user_id, sort_order) VALUES (1, '默认分组', 1, 0);
INSERT INTO t_group (id, name, user_id, sort_order) VALUES (2, '营销分组', 1, 1);

-- ----------------------------- link_db_3 -------------------------------------
USE link_db_3;

INSERT INTO t_user (id, username, password, real_name, phone, email, status, del_flag)
VALUES (1, 'admin', 'e10adc3949ba59abbe56e057f20f883e', '管理员', '13800000000', 'admin@garlic.com', 0, 0);

INSERT INTO t_group (id, name, user_id, sort_order) VALUES (1, '默认分组', 1, 0);
INSERT INTO t_group (id, name, user_id, sort_order) VALUES (2, '营销分组', 1, 1);

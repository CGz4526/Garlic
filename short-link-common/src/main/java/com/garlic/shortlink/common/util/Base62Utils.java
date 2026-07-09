package com.garlic.shortlink.common.util;

/**
 * 62 进制转换工具类。
 *
 * <p>字符集：0-9 + A-Z + a-z，共 62 个字符。</p>
 * <p>用于将雪花算法生成的 long 类型 ID 转换为 6~7 位短码。</p>
 *
 * @author garlic
 */
public final class Base62Utils {

    /** 62 进制字符集 */
    private static final String CHAR_SET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    /** 字符集长度 */
    private static final int BASE = CHAR_SET.length();

    private Base62Utils() {
        throw new UnsupportedOperationException("工具类不可实例化");
    }

    /**
     * 将 long 类型 ID 编码为 62 进制字符串。
     *
     * @param id 长整型 ID
     * @return 62 进制字符串
     */
    public static String encode(long id) {
        if (id < 0) {
            throw new IllegalArgumentException("ID 不能为负数");
        }
        if (id == 0) {
            return String.valueOf(CHAR_SET.charAt(0));
        }
        StringBuilder sb = new StringBuilder();
        long temp = id;
        while (temp > 0) {
            int remainder = (int) (temp % BASE);
            sb.insert(0, CHAR_SET.charAt(remainder));
            temp = temp / BASE;
        }
        return sb.toString();
    }

    /**
     * 将 62 进制字符串解码为 long 类型 ID。
     *
     * @param str 62 进制字符串
     * @return 长整型 ID
     */
    public static long decode(String str) {
        if (str == null || str.isEmpty()) {
            throw new IllegalArgumentException("待解码字符串不能为空");
        }
        long result = 0L;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            int index = CHAR_SET.indexOf(c);
            if (index < 0) {
                throw new IllegalArgumentException("非法字符：" + c);
            }
            result = result * BASE + index;
        }
        return result;
    }
}

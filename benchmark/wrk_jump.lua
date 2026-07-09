-- wrk 压测脚本：短链接跳转接口
--
-- 使用方式：
--   wrk -t8 -c500 -d30s -s wrk_jump.lua --latency http://localhost:8001
--
-- 参数说明：
--   -t8        8 个线程
--   -c500      500 个并发连接
--   -d30s      持续 30 秒
--   -s         指定 Lua 脚本
--   --latency  打印延迟分布（P50/P75/P90/P99）
--
-- 预期指标：
--   QPS        >= 5000
--   P99        < 20ms
--   缓存命中率  >= 95%

-- 预置 100 个短码（命名规则：bm + 三位数字，bm000~bm099）
-- 实际压测前请通过批量生成接口预置 100 个真实短码，并替换下方数组
local codes = {}
for i = 0, 99 do
    table.insert(codes, string.format("bm%03d", i))
end

local code_count = #codes

-- 请求方法：GET
wrk.method = "GET"

-- 请求初始化：每次请求随机选取一个短码构造路径
request = function()
    local idx = math.random(code_count)
    local code = codes[idx]
    -- 随机短码路径
    wrk.path = "/" .. code
    return wrk.format()
end

-- 响应处理：统计响应码分布（可选，用于分析 302 命中率）
local responses = { [200] = 0, [302] = 0, [404] = 0, [410] = 0, [429] = 0, [500] = 0, other = 0 }

response = function(status, headers, body)
    if responses[status] then
        responses[status] = responses[status] + 1
    else
        responses.other = responses.other + 1
    end
end

-- 压测结束时打印响应码分布
done = function(summary, latency, requests)
    print("\n========== 响应码分布 ==========")
    print(string.format("200 (OK)            : %d", responses[200]))
    print(string.format("302 (Redirect)      : %d", responses[302]))
    print(string.format("404 (Not Found)     : %d", responses[404]))
    print(string.format("410 (Expired)       : %d", responses[410]))
    print(string.format("429 (Too Many Reqs) : %d", responses[429]))
    print(string.format("500 (Server Error)  : %d", responses[500]))
    print(string.format("other               : %d", responses.other))
    print(string.format("\n总请求数: %d, 错误数: %d, QPS: %.2f",
            summary.requests, summary.errors.connect + summary.errors.read +
                    summary.errors.write + summary.errors.timeout,
            summary.requests / (summary.duration / 1000000)))
    print(string.format("延迟: P50=%.2fms, P90=%.2fms, P99=%.2fms, Max=%.2fms",
            latency:percentile(50) / 1000, latency:percentile(90) / 1000,
            latency:percentile(99) / 1000, latency.max / 1000))
end

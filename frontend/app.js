/* ============================================================
   Garlic Short Link Console — app.js
   配置 / Mock 数据 / API 层 / 视图切换 / 图表 / 交互
   ============================================================ */

/* ---------- 全局配置 ---------- */
const API_BASE_URL = 'http://localhost:8001';
const USE_MOCK = true; // 切换为 false 即对接真实后端
// 运行时可切换（受导航栏 Mock/Real 按钮控制），默认取 USE_MOCK
let useMock = USE_MOCK;

/* ---------- Mock 数据 ---------- */
const MOCK_LINKS = [
    { shortCode: 'Ab3xK9', originalUrl: 'https://github.com/garlic/short-link', describe: '项目主仓库', pv: 12480, uv: 3456, status: 0, createTime: '2026-06-28 14:21:09', expireTime: '2026-12-31 23:59:59' },
    { shortCode: 'Bc7mQ2', originalUrl: 'https://stackoverflow.com/questions/12345678/how-to-design-a-url-shortener', describe: '架构参考问答', pv: 8932, uv: 2103, status: 0, createTime: '2026-06-29 09:14:33', expireTime: '2027-01-15 23:59:59' },
    { shortCode: 'Cd9nL4', originalUrl: 'https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle', describe: 'Spring Boot 文档', pv: 6754, uv: 1890, status: 0, createTime: '2026-06-30 16:42:51', expireTime: '' },
    { shortCode: 'De4pR7', originalUrl: 'https://redis.io/docs/getting-started', describe: 'Redis 入门', pv: 5421, uv: 1542, status: 0, createTime: '2026-07-01 11:05:27', expireTime: '' },
    { shortCode: 'Ef6sT1', originalUrl: 'https://kafka.apache.org/documentation/', describe: 'Kafka 官方文档', pv: 4290, uv: 1287, status: 0, createTime: '2026-07-02 08:30:15', expireTime: '2026-10-01 00:00:00' },
    { shortCode: 'Fg8uW3', originalUrl: 'https://shardingsphere.apache.org/document/current/cn/overview', describe: 'ShardingSphere 分库分表', pv: 3865, uv: 998, status: 0, createTime: '2026-07-02 19:18:44', expireTime: '' },
    { shortCode: 'Gh0vY5', originalUrl: 'https://github.com/redisson/redisson', describe: 'Redisson 分布式锁', pv: 3120, uv: 845, status: 0, createTime: '2026-07-03 13:55:02', expireTime: '' },
    { shortCode: 'Hi2wA8', originalUrl: 'https://sentinelguard.io/zh-cn/docs/introduction.html', describe: 'Sentinel 流控', pv: 2784, uv: 712, status: 1, createTime: '2026-07-03 22:09:36', expireTime: '2026-09-30 23:59:59' },
    { shortCode: 'Ij4xB6', originalUrl: 'https://www.cnblogs.com/rickiyang/p/11019434.html', describe: '布隆过滤器原理', pv: 2310, uv: 634, status: 0, createTime: '2026-07-04 10:27:18', expireTime: '' },
    { shortCode: 'Kl6yC0', originalUrl: 'https://base62.io/', describe: 'Base62 编码演示', pv: 1876, uv: 521, status: 0, createTime: '2026-07-04 15:48:50', expireTime: '' },
    { shortCode: 'Mn8zD2', originalUrl: 'https://github.com/alibaba/Sentinel', describe: 'Sentinel 源码', pv: 1543, uv: 412, status: 2, createTime: '2026-07-05 07:33:11', expireTime: '' },
    { shortCode: 'Op0aF4', originalUrl: 'https://projectlombok.org/features/all', describe: 'Lombok 特性', pv: 982, uv: 287, status: 0, createTime: '2026-07-05 17:12:29', expireTime: '2026-08-31 23:59:59' },
    { shortCode: 'Qr2bG6', originalUrl: 'https://www.baeldung.com/java-rate-limiting', describe: 'Java 限流方案', pv: 754, uv: 198, status: 0, createTime: '2026-07-06 09:44:03', expireTime: '' },
    { shortCode: 'St4cH8', originalUrl: 'https://github.com/alibaba/canal', describe: 'Canal binlog 订阅', pv: 521, uv: 143, status: 0, createTime: '2026-07-06 14:20:47', expireTime: '' },
    { shortCode: 'Uv6eJ0', originalUrl: 'https://hutool.cn/docs', describe: 'Hutool 工具库', pv: 312, uv: 89, status: 1, createTime: '2026-07-07 18:55:12', expireTime: '' }
];

// 仪表盘概览
const MOCK_OVERVIEW = { total: MOCK_LINKS.length, todayPv: 18934, todayUv: 5421, hitRate: 94.6 };

// 7/14 天 PV 趋势
function genTrend(days) {
    const arr = [];
    const today = new Date('2026-07-09');
    let base = 1200;
    for (let i = days - 1; i >= 0; i--) {
        const d = new Date(today); d.setDate(d.getDate() - i);
        base = Math.max(400, base + Math.round((Math.random() - 0.4) * 320));
        arr.push({
            date: `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`,
            pv: base,
            uv: Math.round(base * (0.28 + Math.random() * 0.1))
        });
    }
    return arr;
}
const MOCK_TREND_7 = genTrend(7);
const MOCK_TREND_14 = genTrend(14);
const MOCK_TREND_30 = genTrend(30);

// 维度分布
const MOCK_DIST = {
    browser: [
        { name: 'Chrome', value: 5820 },
        { name: 'Safari', value: 2140 },
        { name: 'Edge', value: 1180 },
        { name: 'Firefox', value: 760 },
        { name: '其他', value: 320 }
    ],
    os: [
        { name: 'Windows', value: 4960 },
        { name: 'macOS', value: 2780 },
        { name: 'iOS', value: 1320 },
        { name: 'Android', value: 890 },
        { name: 'Linux', value: 470 }
    ],
    device: [
        { name: '桌面端', value: 7720 },
        { name: '手机', value: 2180 },
        { name: '平板', value: 520 }
    ],
    region: [
        { name: '北京', value: 2840 },
        { name: '上海', value: 2210 },
        { name: '深圳', value: 1870 },
        { name: '杭州', value: 1240 },
        { name: '广州', value: 980 },
        { name: '成都', value: 620 },
        { name: '其他', value: 660 }
    ]
};

// 24 小时分布
const MOCK_HOUR = Array.from({ length: 24 }, (_, h) => {
    let v;
    if (h < 6) v = 40 + Math.round(Math.random() * 60);
    else if (h < 9) v = 120 + Math.round(Math.random() * 180);
    else if (h < 12) v = 480 + Math.round(Math.random() * 220);
    else if (h < 14) v = 360 + Math.round(Math.random() * 160);
    else if (h < 18) v = 540 + Math.round(Math.random() * 260);
    else if (h < 22) v = 420 + Math.round(Math.random() * 240);
    else v = 180 + Math.round(Math.random() * 120);
    return { hour: h, pv: v };
});

// 单链统计概览
const MOCK_STATS_OVERVIEW = { pv: 12480, uv: 3456, ipCount: 2104 };

/* ---------- 状态 ---------- */
const state = {
    view: 'dashboard',
    links: [...MOCK_LINKS],
    page: { current: 1, size: 8 },
    statsCode: MOCK_LINKS[0].shortCode,
    trendDays: 7,
    statsTrendDays: 7,
    dim: 'browser',
    charts: {}
};

/* ---------- 工具函数 ---------- */
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

const fmt = (n) => Number(n).toLocaleString('en-US');
const shortUrlOf = (code) => `${API_BASE_URL}/${code}`;
const truncate = (s, n = 48) => s.length > n ? s.slice(0, n) + '…' : s;

function statusTag(status) {
    if (status === 0) return '<span class="tag ok">正常</span>';
    if (status === 1) return '<span class="tag expired">已过期</span>';
    return '<span class="tag disabled">已禁用</span>';
}

function toast(msg, isError = false) {
    const el = $('#toast');
    el.textContent = msg;
    el.classList.toggle('error', isError);
    el.hidden = false;
    requestAnimationFrame(() => el.classList.add('show'));
    clearTimeout(el._t);
    el._t = setTimeout(() => {
        el.classList.remove('show');
        setTimeout(() => { el.hidden = true; }, 200);
    }, 1800);
}

async function copyText(text) {
    try {
        await navigator.clipboard.writeText(text);
        toast('已复制到剪贴板');
    } catch {
        // 降级方案
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed'; ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        try { document.execCommand('copy'); toast('已复制到剪贴板'); }
        catch { toast('复制失败', true); }
        document.body.removeChild(ta);
    }
}

function randomCode(len = 6) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let s = '';
    for (let i = 0; i < len; i++) s += chars[Math.floor(Math.random() * chars.length)];
    return s;
}

/* ---------- API 层（带 Mock 兜底） ---------- */
const api = {
    async create(payload) {
        if (useMock) {
            await delay(300);
            const code = randomCode();
            return {
                shortCode: code,
                shortUrl: shortUrlOf(code),
                originalUrl: payload.originalUrl
            };
        }
        const res = await fetch(`${API_BASE_URL}/api/short-link/create`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ userId: 1, groupId: 1, ...payload })
        });
        const json = await res.json();
        return json.data;
    },

    async batchCreate(urls) {
        if (useMock) {
            await delay(500);
            return urls.map(u => {
                const code = randomCode();
                return { originalUrl: u, shortCode: code, shortUrl: shortUrlOf(code) };
            });
        }
        const res = await fetch(`${API_BASE_URL}/api/short-link/batch-create`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ urls, userId: 1, groupId: 1 })
        });
        const json = await res.json();
        return json.data;
    },

    async page(current, size) {
        if (useMock) {
            await delay(200);
            const all = [...state.links].sort((a, b) => b.createTime.localeCompare(a.createTime));
            const start = (current - 1) * size;
            return { records: all.slice(start, start + size), total: all.length, current, size };
        }
        const res = await fetch(`${API_BASE_URL}/api/short-link/page?userId=1&current=${current}&size=${size}`);
        const json = await res.json();
        return json.data;
    },

    async update(payload) {
        if (useMock) {
            await delay(200);
            const idx = state.links.findIndex(l => l.shortCode === payload.shortCode);
            if (idx >= 0) {
                if (payload.describe !== undefined) state.links[idx].describe = payload.describe;
                if (payload.expireTime !== undefined) state.links[idx].expireTime = payload.expireTime;
            }
            return true;
        }
        await fetch(`${API_BASE_URL}/api/short-link/update`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        return true;
    },

    async remove(code) {
        if (useMock) {
            await delay(200);
            state.links = state.links.filter(l => l.shortCode !== code);
            return true;
        }
        await fetch(`${API_BASE_URL}/api/short-link/${code}`, { method: 'DELETE' });
        return true;
    },

    async trend(days) {
        if (useMock) { await delay(150); return days <= 7 ? MOCK_TREND_7 : (days <= 14 ? MOCK_TREND_14 : MOCK_TREND_30); }
        const res = await fetch(`${API_BASE_URL}/api/stats/${state.statsCode}/trend?days=${days}`);
        const json = await res.json();
        return json.data;
    },

    async overview(code) {
        if (useMock) { await delay(150); return { ...MOCK_STATS_OVERVIEW }; }
        const res = await fetch(`${API_BASE_URL}/api/stats/${code}/overview`);
        const json = await res.json();
        return json.data;
    },

    async distribution(code, dim, days) {
        if (useMock) { await delay(150); return MOCK_DIST[dim]; }
        const res = await fetch(`${API_BASE_URL}/api/stats/${code}/distribution?dimension=${dim}&days=${days}`);
        const json = await res.json();
        return json.data;
    },

    async hour(code) {
        if (useMock) { await delay(150); return MOCK_HOUR; }
        const res = await fetch(`${API_BASE_URL}/api/stats/${code}/hour?date=2026-07-08`);
        const json = await res.json();
        return json.data;
    }
};

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

/* ---------- 图表主题 ---------- */
function chartTheme() {
    const css = getComputedStyle(document.documentElement);
    return {
        fg: css.getPropertyValue('--fg').trim(),
        muted: css.getPropertyValue('--muted-fg').trim(),
        grid: css.getPropertyValue('--chart-grid').trim(),
        accent: css.getPropertyValue('--accent').trim(),
        border: css.getPropertyValue('--border-soft').trim()
    };
}

const PALETTE = ['#22C55E', '#60A5FA', '#A78BFA', '#F59E0B', '#F472B6', '#34D399', '#94A3B8'];

/* ---------- 视图切换 ---------- */
function switchView(name) {
    state.view = name;
    $$('.view').forEach(v => v.classList.toggle('active', v.id === `view-${name}`));
    $$('.nav-item').forEach(b => b.classList.toggle('active', b.dataset.view === name));
    window.scrollTo({ top: 0, behavior: 'smooth' });

    // 视图首次进入时渲染
    if (name === 'dashboard') renderDashboard();
    if (name === 'manage') renderList();
    if (name === 'stats') renderStats();
}

/* ============================================================
   仪表盘
   ============================================================ */
function renderDashboard() {
    // 概览数字
    animateNum($('[data-stat="total"]'), MOCK_OVERVIEW.total);
    animateNum($('[data-stat="pv"]'), MOCK_OVERVIEW.todayPv);
    animateNum($('[data-stat="uv"]'), MOCK_OVERVIEW.todayUv);
    $('[data-stat="hit"]').textContent = MOCK_OVERVIEW.hitRate;

    // 趋势图
    renderDashTrend();

    // TOP 5
    const top5 = [...state.links].sort((a, b) => b.pv - a.pv).slice(0, 5);
    $('#topLinksBody').innerHTML = top5.map(l => `
        <tr>
            <td class="cell-code">${l.shortCode}</td>
            <td class="cell-url" title="${l.originalUrl}">${truncate(l.originalUrl, 60)}</td>
            <td class="cell-pv">${fmt(l.pv)}</td>
            <td>${statusTag(l.status)}</td>
        </tr>
    `).join('');
}

function animateNum(el, target) {
    if (!el) return;
    const start = 0;
    const dur = 700;
    const t0 = performance.now();
    function step(t) {
        const p = Math.min(1, (t - t0) / dur);
        const ease = 1 - Math.pow(1 - p, 3);
        el.textContent = fmt(Math.round(start + (target - start) * ease));
        if (p < 1) requestAnimationFrame(step);
    }
    requestAnimationFrame(step);
}

function renderDashTrend() {
    const data = state.trendDays === 7 ? MOCK_TREND_7 : MOCK_TREND_14;
    const th = chartTheme();
    const ctx = $('#dashTrendChart').getContext('2d');
    if (state.charts.dashTrend) state.charts.dashTrend.destroy();
    state.charts.dashTrend = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => d.date.slice(5)),
            datasets: [{
                label: 'PV',
                data: data.map(d => d.pv),
                borderColor: th.accent,
                backgroundColor: 'rgba(34,197,94,0.10)',
                borderWidth: 2,
                tension: 0.35,
                fill: true,
                pointRadius: 0,
                pointHoverRadius: 5,
                pointHoverBackgroundColor: th.accent,
                pointHoverBorderColor: th.fg
            }]
        },
        options: lineChartOpts(th)
    });
}

function lineChartOpts(th) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
            legend: { display: false },
            tooltip: {
                backgroundColor: '#1E293B',
                borderColor: th.border,
                borderWidth: 1,
                titleColor: th.fg,
                bodyColor: th.muted,
                padding: 12,
                cornerRadius: 8,
                displayColors: false
            }
        },
        scales: {
            x: { grid: { color: th.grid, drawBorder: false }, ticks: { color: th.muted, font: { family: 'DM Sans', size: 11 } } },
            y: { grid: { color: th.grid, drawBorder: false }, ticks: { color: th.muted, font: { family: 'DM Sans', size: 11 } }, beginAtZero: true }
        }
    };
}

/* ============================================================
   短链管理
   ============================================================ */
async function renderList() {
    const { records, total, current, size } = await api.page(state.page.current, state.page.size);
    const body = $('#linksBody');

    if (!records.length) {
        body.innerHTML = `<tr class="empty-row"><td colspan="7">暂无短链数据</td></tr>`;
    } else {
        body.innerHTML = records.map(l => `
            <tr data-code="${l.shortCode}">
                <td class="cell-code">${l.shortCode}</td>
                <td>
                    <span class="copy-cell">
                        <code>${shortUrlOf(l.shortCode)}</code>
                        <button class="act-btn accent" data-copy="${shortUrlOf(l.shortCode)}" title="复制短链">
                            ${iconCopy()}
                        </button>
                    </span>
                </td>
                <td class="cell-url" title="${l.originalUrl}">${truncate(l.originalUrl, 50)}</td>
                <td class="cell-pv">${fmt(l.pv)}</td>
                <td>${statusTag(l.status)}</td>
                <td class="cell-time">${l.createTime}</td>
                <td>
                    <div class="row-actions">
                        <button class="act-btn" data-edit="${l.shortCode}" title="编辑">${iconEdit()}</button>
                        <button class="act-btn danger" data-del="${l.shortCode}" title="删除">${iconTrash()}</button>
                    </div>
                </td>
            </tr>
        `).join('');
    }

    const totalPages = Math.max(1, Math.ceil(total / size));
    $('#pagerInfo').textContent = `共 ${total} 条 · 第 ${current} / ${totalPages} 页`;
    $('#pagerCur').textContent = current;
    $('#prevPage').disabled = current <= 1;
    $('#nextPage').disabled = current >= totalPages;
}

function iconCopy() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" width="15" height="15"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>';
}
function iconEdit() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" width="15" height="15"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.12 2.12 0 0 1 3 3L12 15l-4 1 1-4Z"/></svg>';
}
function iconTrash() {
    return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" width="15" height="15"><path d="M3 6h18"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>';
}

async function handleCreate(e) {
    e.preventDefault();
    const url = $('#fUrl').value.trim();
    const describe = $('#fDesc').value.trim();
    const expireTime = $('#fExpire').value.trim();
    if (!url) { toast('请输入原始长链', true); return; }

    try {
        const data = await api.create({ originalUrl: url, describe, expireTime });
        // 加入本地列表
        state.links.unshift({
            shortCode: data.shortCode,
            originalUrl: data.originalUrl,
            describe: describe || '',
            pv: 0, uv: 0, status: 0,
            createTime: nowStr(),
            expireTime: expireTime || ''
        });

        $('#formResult').hidden = false;
        $('#newShortUrl').textContent = data.shortUrl;

        // 复制按钮
        $('#copyNewBtn').onclick = () => copyText(data.shortUrl);

        // 重置表单
        $('#createForm').reset();
        state.page.current = 1;
        renderList();
        toast('短链创建成功');
    } catch {
        toast('创建失败，请检查后端', true);
    }
}

function nowStr() {
    const d = new Date();
    const p = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

/* 列表事件委托 */
function bindListEvents() {
    $('#linksBody').addEventListener('click', async (e) => {
        const copyBtn = e.target.closest('[data-copy]');
        const editBtn = e.target.closest('[data-edit]');
        const delBtn = e.target.closest('[data-del]');
        if (copyBtn) { copyText(copyBtn.dataset.copy); return; }
        if (editBtn) { openEdit(editBtn.dataset.edit); return; }
        if (delBtn) {
            const code = delBtn.dataset.del;
            if (!confirm(`确认删除短链 ${code}？`)) return;
            await api.remove(code);
            toast('已删除');
            renderList();
        }
    });

    $('#listSearch').addEventListener('input', (e) => {
        const kw = e.target.value.trim().toLowerCase();
        if (!kw) { renderList(); return; }
        const filtered = state.links.filter(l =>
            l.shortCode.toLowerCase().includes(kw) ||
            l.originalUrl.toLowerCase().includes(kw)
        );
        const body = $('#linksBody');
        if (!filtered.length) {
            body.innerHTML = `<tr class="empty-row"><td colspan="7">未匹配到结果</td></tr>`;
        } else {
            body.innerHTML = filtered.map(l => `
                <tr data-code="${l.shortCode}">
                    <td class="cell-code">${l.shortCode}</td>
                    <td><span class="copy-cell"><code>${shortUrlOf(l.shortCode)}</code>
                        <button class="act-btn accent" data-copy="${shortUrlOf(l.shortCode)}" title="复制">${iconCopy()}</button></span></td>
                    <td class="cell-url" title="${l.originalUrl}">${truncate(l.originalUrl, 50)}</td>
                    <td class="cell-pv">${fmt(l.pv)}</td>
                    <td>${statusTag(l.status)}</td>
                    <td class="cell-time">${l.createTime}</td>
                    <td><div class="row-actions">
                        <button class="act-btn" data-edit="${l.shortCode}" title="编辑">${iconEdit()}</button>
                        <button class="act-btn danger" data-del="${l.shortCode}" title="删除">${iconTrash()}</button>
                    </div></td>
                </tr>`).join('');
        }
        $('#pagerInfo').textContent = `搜索结果 ${filtered.length} 条`;
        $('#prevPage').disabled = true;
        $('#nextPage').disabled = true;
    });

    $('#prevPage').addEventListener('click', () => {
        if (state.page.current > 1) { state.page.current--; renderList(); }
    });
    $('#nextPage').addEventListener('click', () => {
        state.page.current++; renderList();
    });
}

/* 编辑弹窗 */
function openEdit(code) {
    const link = state.links.find(l => l.shortCode === code);
    if (!link) return;
    $('#eCode').value = code;
    $('#eDesc').value = link.describe || '';
    $('#eExpire').value = link.expireTime || '';
    $('#editModal').hidden = false;
}
function closeEdit() { $('#editModal').hidden = true; }

function bindEditModal() {
    $('#editClose').addEventListener('click', closeEdit);
    $('#editCancel').addEventListener('click', closeEdit);
    $('#editModal').addEventListener('click', (e) => { if (e.target.id === 'editModal') closeEdit(); });
    $('#editForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        await api.update({
            shortCode: $('#eCode').value,
            describe: $('#eDesc').value,
            expireTime: $('#eExpire').value
        });
        closeEdit();
        toast('已保存');
        renderList();
    });
}

/* ============================================================
   统计分析
   ============================================================ */
function fillStatsSelect() {
    const sel = $('#statsSelect');
    sel.innerHTML = state.links.map(l =>
        `<option value="${l.shortCode}">${l.shortCode} — ${truncate(l.originalUrl, 40)}</option>`
    ).join('');
    sel.value = state.statsCode;
}

async function renderStats() {
    fillStatsSelect();
    await renderStatsOverview();
    await renderStatsTrend();
    await renderDist();
    await renderHour();
}

async function renderStatsOverview() {
    const ov = await api.overview(state.statsCode);
    animateNum($('#ovPv'), ov.pv);
    animateNum($('#ovUv'), ov.uv);
    animateNum($('#ovIp'), ov.ipCount);
}

async function renderStatsTrend() {
    const data = await api.trend(state.statsTrendDays);
    const th = chartTheme();
    const ctx = $('#statsTrendChart').getContext('2d');
    if (state.charts.statsTrend) state.charts.statsTrend.destroy();
    state.charts.statsTrend = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => d.date.slice(5)),
            datasets: [
                {
                    label: 'PV', data: data.map(d => d.pv),
                    borderColor: th.accent, backgroundColor: 'rgba(34,197,94,0.10)',
                    borderWidth: 2, tension: 0.35, fill: true,
                    pointRadius: 0, pointHoverRadius: 5
                },
                {
                    label: 'UV', data: data.map(d => d.uv),
                    borderColor: '#60A5FA', backgroundColor: 'rgba(96,165,250,0.06)',
                    borderWidth: 2, tension: 0.35, fill: false,
                    pointRadius: 0, pointHoverRadius: 5
                }
            ]
        },
        options: {
            ...lineChartOpts(th),
            plugins: {
                ...lineChartOpts(th).plugins,
                legend: {
                    display: true, position: 'top', align: 'end',
                    labels: { color: th.muted, font: { family: 'DM Sans', size: 12 }, boxWidth: 12, boxHeight: 12, usePointStyle: true, pointStyle: 'rectRounded' }
                }
            }
        }
    });
}

async function renderDist() {
    const data = await api.distribution(state.statsCode, state.dim, state.statsTrendDays);
    const th = chartTheme();
    const ctx = $('#distChart').getContext('2d');
    if (state.charts.dist) state.charts.dist.destroy();
    state.charts.dist = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: data.map(d => d.name),
            datasets: [{
                data: data.map(d => d.value),
                backgroundColor: PALETTE,
                borderColor: th.fg === '#F8FAFC' ? '#1E293B' : '#FFFFFF',
                borderWidth: 2,
                hoverOffset: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '62%',
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        color: th.muted, font: { family: 'DM Sans', size: 12 },
                        boxWidth: 12, boxHeight: 12, usePointStyle: true, pointStyle: 'rectRounded', padding: 12
                    }
                },
                tooltip: {
                    backgroundColor: '#1E293B', borderColor: th.border, borderWidth: 1,
                    titleColor: th.fg, bodyColor: th.muted, padding: 12, cornerRadius: 8
                }
            }
        }
    });
}

async function renderHour() {
    const data = await api.hour(state.statsCode);
    const th = chartTheme();
    const ctx = $('#hourChart').getContext('2d');
    if (state.charts.hour) state.charts.hour.destroy();
    state.charts.hour = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => String(d.hour).padStart(2, '0')),
            datasets: [{
                label: 'PV',
                data: data.map(d => d.pv),
                backgroundColor: th.accent,
                hoverBackgroundColor: th.accent,
                borderRadius: 6,
                barPercentage: 0.7,
                categoryPercentage: 0.8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#1E293B', borderColor: th.border, borderWidth: 1,
                    titleColor: th.fg, bodyColor: th.muted, padding: 12, cornerRadius: 8,
                    callbacks: { title: (items) => `${items[0].label}:00 — ${items[0].label}:59` }
                }
            },
            scales: {
                x: { grid: { display: false }, ticks: { color: th.muted, font: { family: 'DM Sans', size: 11 } } },
                y: { grid: { color: th.grid, drawBorder: false }, ticks: { color: th.muted, font: { family: 'DM Sans', size: 11 } }, beginAtZero: true }
            }
        }
    });
}

/* ============================================================
   批量生成
   ============================================================ */
function bindBatch() {
    const input = $('#batchInput');
    const updateCount = () => {
        const n = input.value.split('\n').map(s => s.trim()).filter(Boolean).length;
        $('#batchCount').textContent = `${n} 条`;
    };
    input.addEventListener('input', updateCount);

    $('#batchClear').addEventListener('click', () => {
        input.value = '';
        updateCount();
        $('#batchResultPanel').hidden = true;
    });

    $('#batchBtn').addEventListener('click', async () => {
        const urls = input.value.split('\n').map(s => s.trim()).filter(Boolean);
        if (!urls.length) { toast('请输入至少一个 URL', true); return; }
        const btn = $('#batchBtn');
        btn.disabled = true; btn.textContent = '生成中…';
        try {
            const results = await api.batchCreate(urls);
            $('#batchBody').innerHTML = results.map(r => `
                <tr>
                    <td class="cell-url" title="${r.originalUrl}">${truncate(r.originalUrl, 60)}</td>
                    <td><span class="copy-cell"><code>${r.shortUrl}</code></span></td>
                    <td><button class="act-btn accent" data-bcopy="${r.shortUrl}" title="复制">${iconCopy()}</button></td>
                </tr>
            `).join('');
            $('#batchResultPanel').hidden = false;
            toast(`已生成 ${results.length} 条短链`);
        } catch {
            toast('批量生成失败', true);
        } finally {
            btn.disabled = false;
            btn.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" width="16" height="16"><path d="M5 12h14"/><path d="M12 5v14"/></svg> 批量生成`;
        }
    });

    $('#batchBody').addEventListener('click', (e) => {
        const b = e.target.closest('[data-bcopy]');
        if (b) copyText(b.dataset.bcopy);
    });

    $('#copyAllBtn').addEventListener('click', async () => {
        const codes = $$('#batchBody code').map(c => c.textContent).join('\n');
        copyText(codes);
    });
}

/* ============================================================
   主题 / 导航 绑定
   ============================================================ */
function bindNav() {
    $('#navMenu').addEventListener('click', (e) => {
        const btn = e.target.closest('.nav-item');
        if (btn) {
            switchView(btn.dataset.view);
            // 同步到 URL hash，便于分享与截图定位
            history.replaceState(null, '', `#${btn.dataset.view}`);
        }
    });
    // 内部“查看全部”链接
    document.addEventListener('click', (e) => {
        const link = e.target.closest('.link-text[data-view]');
        if (link) {
            switchView(link.dataset.view);
            history.replaceState(null, '', `#${link.dataset.view}`);
        }
    });
    // 支持 URL hash 直接定位视图（如 index.html#stats），用于深链与截图
    const valid = ['dashboard', 'manage', 'stats', 'batch'];
    const hash = location.hash.replace('#', '');
    if (valid.includes(hash)) switchView(hash);
    window.addEventListener('hashchange', () => {
        const h = location.hash.replace('#', '');
        if (valid.includes(h)) switchView(h);
    });
}

function bindTheme() {
    const saved = localStorage.getItem('garlic-theme');
    if (saved) document.documentElement.setAttribute('data-theme', saved);

    $('#themeBtn').addEventListener('click', () => {
        const cur = document.documentElement.getAttribute('data-theme');
        const next = cur === 'dark' ? 'light' : 'dark';
        document.documentElement.setAttribute('data-theme', next);
        localStorage.setItem('garlic-theme', next);
        // 重绘当前视图的图表以适配主题
        if (state.view === 'dashboard') renderDashTrend();
        if (state.view === 'stats') { renderStatsTrend(); renderDist(); renderHour(); }
    });
}

function bindApiMode() {
    const btn = $('#apiModeBtn');
    const badge = $('#apiBadge');
    // 读取持久化，同步到运行时 useMock
    const realMode = localStorage.getItem('garlic-api-real') === '1';
    if (realMode) { btn.dataset.real = '1'; badge.textContent = 'REAL'; useMock = false; }

    btn.addEventListener('click', () => {
        const isReal = btn.dataset.real === '1';
        if (isReal) {
            btn.dataset.real = ''; badge.textContent = 'MOCK';
            localStorage.setItem('garlic-api-real', '0');
            useMock = true;
            toast('已切换至 Mock 数据模式');
            // 回到 Mock 时重置数据
            state.links = [...MOCK_LINKS];
            if (state.view === 'manage') renderList();
            if (state.view === 'dashboard') renderDashboard();
        } else {
            btn.dataset.real = '1'; badge.textContent = 'REAL';
            localStorage.setItem('garlic-api-real', '1');
            useMock = false;
            toast('已切换至真实 API 模式');
            // 切到 Real 时刷新当前视图以拉取后端数据
            switchView(state.view);
        }
    });
}

function bindDashTabs() {
    $$('[data-trend]').forEach(b => {
        b.addEventListener('click', () => {
            $$('[data-trend]').forEach(x => x.classList.remove('active'));
            b.classList.add('active');
            state.trendDays = Number(b.dataset.trend);
            renderDashTrend();
        });
    });
}

function bindStatsTabs() {
    $('#statsSelect').addEventListener('change', (e) => {
        state.statsCode = e.target.value;
        renderStatsOverview();
        renderStatsTrend();
        renderDist();
        renderHour();
    });

    $$('[data-stats-trend]').forEach(b => {
        b.addEventListener('click', () => {
            $$('[data-stats-trend]').forEach(x => x.classList.remove('active'));
            b.classList.add('active');
            state.statsTrendDays = Number(b.dataset.statsTrend);
            renderStatsTrend();
            renderDist();
        });
    });

    $$('#distTabs [data-dim]').forEach(b => {
        b.addEventListener('click', () => {
            $$('#distTabs [data-dim]').forEach(x => x.classList.remove('active'));
            b.classList.add('active');
            state.dim = b.dataset.dim;
            renderDist();
        });
    });
}

/* ---------- 初始化 ---------- */
document.addEventListener('DOMContentLoaded', () => {
    bindNav();
    bindTheme();
    bindApiMode();
    bindListEvents();
    bindEditModal();
    bindDashTabs();
    bindStatsTabs();
    bindBatch();
    $('#createForm').addEventListener('submit', handleCreate);

    // 默认渲染仪表盘
    renderDashboard();
});

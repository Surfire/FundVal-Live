import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  createStrategyPortfolio,
  createStrategyVersion,
  generateStrategyRebalance,
  getAccountPositions,
  getStrategyPerformance,
  getStrategyPortfolio,
  listRebalanceOrders,
  listStrategyPortfolios,
  updateRebalanceOrderStatus,
} from '../services/api';
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

const ALL_WEATHER_SAMPLE = [
  { code: '513330', weight: 25 },
  { code: '513070', weight: 5 },
  { code: '513120', weight: 5 },
  { code: '164824', weight: 15 },
  { code: '518880', weight: 20 },
  { code: '516650', weight: 10 },
  { code: '159870', weight: 20 },
];

const RANGE_OPTIONS = [
  { key: 'since', label: '成立以来' },
  { key: '1w', label: '近1周' },
  { key: '1m', label: '近1月' },
  { key: '3m', label: '近3月' },
  { key: '1y', label: '近1年' },
  { key: 'ytd', label: '年初至今' },
];

function toPercent(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return '--';
  return `${(v * 100).toFixed(2)}%`;
}

function toNumber(v, n = 2) {
  if (v === null || v === undefined || Number.isNaN(v)) return '--';
  return Number(v).toFixed(n);
}

function parseHoldingsText(text) {
  const lines = text
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);

  const out = [];
  for (const line of lines) {
    const m = line.match(/^([0-9A-Za-z]{5,10})\s+([0-9.]+)%?$/);
    if (!m) {
      throw new Error(`格式错误：${line}，请用“代码 空格 权重%”`);
    }
    out.push({ code: m[1], weight: parseFloat(m[2]) });
  }
  if (!out.length) throw new Error('请输入至少一条持仓');
  return out;
}

function mergeSeries(strategySeries = [], benchmarkSeries = []) {
  const m = new Map();
  strategySeries.forEach((p) => {
    m.set(p.date, { date: p.date, strategy: p.return, benchmark: null });
  });
  benchmarkSeries.forEach((p) => {
    if (!m.has(p.date)) {
      m.set(p.date, { date: p.date, strategy: null, benchmark: p.return });
    } else {
      m.get(p.date).benchmark = p.return;
    }
  });

  return Array.from(m.values())
    .sort((a, b) => a.date.localeCompare(b.date))
    .map((d) => ({
      ...d,
      excess:
        d.strategy === null || d.benchmark === null ? null : d.strategy - d.benchmark,
    }));
}

function filterRange(data, rangeKey) {
  if (!data.length || rangeKey === 'since') return data;
  const endDate = new Date(data[data.length - 1].date);
  const start = new Date(endDate);

  if (rangeKey === '1w') start.setDate(start.getDate() - 7);
  if (rangeKey === '1m') start.setMonth(start.getMonth() - 1);
  if (rangeKey === '3m') start.setMonth(start.getMonth() - 3);
  if (rangeKey === '1y') start.setFullYear(start.getFullYear() - 1);
  if (rangeKey === 'ytd') {
    start.setMonth(0);
    start.setDate(1);
  }

  return data.filter((d) => new Date(d.date) >= start);
}

function toTextFromHoldings(holdings = []) {
  return holdings.map((h) => `${h.code} ${(h.weight * 100).toFixed(2)}%`).join('\n');
}

function Modal({ title, children, onClose }) {
  return (
    <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-2xl max-h-[90vh] overflow-auto">
        <div className="px-5 py-4 border-b flex items-center justify-between">
          <h3 className="text-lg font-semibold">{title}</h3>
          <button onClick={onClose} className="text-slate-500 hover:text-slate-700">关闭</button>
        </div>
        <div className="p-5">{children}</div>
      </div>
    </div>
  );
}

export default function Strategy({ currentAccount = 1, isActive = false }) {
  const [portfolios, setPortfolios] = useState([]);
  const [selectedId, setSelectedId] = useState(null);
  const [detail, setDetail] = useState(null);
  const [performance, setPerformance] = useState(null);
  const [orders, setOrders] = useState([]);

  const [baseLoading, setBaseLoading] = useState(false);
  const [perfLoading, setPerfLoading] = useState(false);

  const [range, setRange] = useState('since');

  const [createOpen, setCreateOpen] = useState(false);
  const [updateOpen, setUpdateOpen] = useState(false);

  const [accountPositions, setAccountPositions] = useState([]);

  const [createForm, setCreateForm] = useState({
    name: '全天候策略',
    benchmark: '000300',
    feeRate: '0.0015',
    holdingsText: '',
    selectedCodes: [],
  });

  const [updateForm, setUpdateForm] = useState({
    note: '',
    holdingsText: '',
    selectedCodes: [],
  });

  const selectedPortfolio = useMemo(
    () => portfolios.find((p) => p.id === selectedId),
    [portfolios, selectedId]
  );

  const loadPortfolios = useCallback(async () => {
    const data = await listStrategyPortfolios(currentAccount);
    setPortfolios(data);
    setSelectedId((prev) => {
      if (!prev && data.length) return data[0].id;
      if (prev && !data.some((p) => p.id === prev) && data.length) return data[0].id;
      return prev;
    });
  }, [currentAccount]);

  const loadAccountPositions = useCallback(async () => {
    const data = await getAccountPositions(currentAccount);
    setAccountPositions(data.positions || []);
  }, [currentAccount]);

  const loadBase = useCallback(async (portfolioId) => {
    if (!portfolioId) return;
    const [d, o] = await Promise.all([
      getStrategyPortfolio(portfolioId),
      listRebalanceOrders(portfolioId, currentAccount),
    ]);
    setDetail(d);
    setOrders(o);
  }, [currentAccount]);

  const loadPerformance = useCallback(async (portfolioId) => {
    if (!portfolioId) return;
    const p = await getStrategyPerformance(portfolioId, currentAccount);
    setPerformance(p);
  }, [currentAccount]);

  useEffect(() => {
    loadPortfolios().catch(() => {});
    loadAccountPositions().catch(() => {});
  }, [loadPortfolios, loadAccountPositions]);

  useEffect(() => {
    if (!selectedId) return;
    setBaseLoading(true);
    loadBase(selectedId)
      .catch((e) => alert(e?.response?.data?.detail || '加载策略失败'))
      .finally(() => setBaseLoading(false));

    setPerfLoading(true);
    loadPerformance(selectedId)
      .catch((e) => alert(e?.response?.data?.detail || '加载绩效失败'))
      .finally(() => setPerfLoading(false));
  }, [selectedId, loadBase, loadPerformance]);

  useEffect(() => {
    if (!isActive || !selectedId) return;
    const t = setInterval(() => {
      loadBase(selectedId).catch(() => {});
      loadPerformance(selectedId).catch(() => {});
    }, 30000);
    return () => clearInterval(t);
  }, [isActive, selectedId, loadBase, loadPerformance]);

  const applySample = () => {
    const text = ALL_WEATHER_SAMPLE.map((x) => `${x.code} ${x.weight}%`).join('\n');
    setCreateForm((prev) => ({ ...prev, holdingsText: text }));
  };

  const importFromSelectedPositions = (selectedCodes, setter) => {
    const selected = accountPositions.filter((p) => selectedCodes.includes(p.code));
    if (!selected.length) {
      alert('请先勾选持仓');
      return;
    }

    const total = selected.reduce((acc, p) => acc + Number(p.est_market_value || p.nav_market_value || 0), 0);
    if (total <= 0) {
      alert('选中持仓当前市值为 0，无法自动导入权重');
      return;
    }

    const text = selected
      .map((p) => {
        const w = (Number(p.est_market_value || p.nav_market_value || 0) / total) * 100;
        return `${p.code} ${w.toFixed(2)}%`;
      })
      .join('\n');

    setter((prev) => ({ ...prev, holdingsText: text }));
  };

  const handleCreate = async (e) => {
    e.preventDefault();
    try {
      const holdings = parseHoldingsText(createForm.holdingsText);
      const scopeCodes = createForm.selectedCodes.length
        ? createForm.selectedCodes
        : holdings.map((h) => h.code);

      await createStrategyPortfolio({
        name: createForm.name,
        account_id: currentAccount,
        benchmark: createForm.benchmark,
        fee_rate: parseFloat(createForm.feeRate || '0'),
        holdings,
        note: '初始版本',
        scope_codes: scopeCodes,
      });

      await loadPortfolios();
      setCreateOpen(false);
      setCreateForm({
        name: '全天候策略',
        benchmark: '000300',
        feeRate: '0.0015',
        holdingsText: '',
        selectedCodes: [],
      });
      alert('策略创建成功');
    } catch (err) {
      alert(err?.response?.data?.detail || err.message || '创建失败');
    }
  };

  const openUpdateModal = () => {
    if (!detail) return;
    setUpdateForm({
      note: '',
      holdingsText: toTextFromHoldings(detail.active_holdings || []),
      selectedCodes: detail?.portfolio?.scope_codes || [],
    });
    setUpdateOpen(true);
  };

  const handleUpdateStrategy = async (e) => {
    e.preventDefault();
    if (!selectedId) return;
    try {
      const holdings = parseHoldingsText(updateForm.holdingsText);
      const scopeCodes = updateForm.selectedCodes.length
        ? updateForm.selectedCodes
        : holdings.map((h) => h.code);

      await createStrategyVersion(selectedId, {
        holdings,
        note: updateForm.note || '更新策略组合',
        activate: true,
        scope_codes: scopeCodes,
      });

      await generateStrategyRebalance(selectedId, {
        account_id: currentAccount,
        min_deviation: 0.005,
        persist: true,
      });

      await Promise.all([loadBase(selectedId), loadPerformance(selectedId)]);
      setUpdateOpen(false);
      alert('策略已更新，并已自动生成新调仓指令');
    } catch (err) {
      alert(err?.response?.data?.detail || err.message || '更新失败');
    }
  };

  const handleGenerateRebalance = async () => {
    if (!selectedId) return;
    try {
      await generateStrategyRebalance(selectedId, {
        account_id: currentAccount,
        min_deviation: 0.005,
        persist: true,
      });
      await loadBase(selectedId);
      alert('调仓指令已生成');
    } catch (err) {
      alert(err?.response?.data?.detail || '生成失败');
    }
  };

  const handleOrderStatus = async (orderId, status) => {
    try {
      await updateRebalanceOrderStatus(orderId, status);
      if (selectedId) {
        const data = await listRebalanceOrders(selectedId, currentAccount);
        setOrders(data);
      }
    } catch (err) {
      alert(err?.response?.data?.detail || '更新状态失败');
    }
  };

  const mergedSeries = useMemo(() => {
    const strategy = performance?.series?.strategy || [];
    const benchmark = performance?.series?.benchmark || [];
    return mergeSeries(strategy, benchmark);
  }, [performance]);

  const chartData = useMemo(
    () => filterRange(mergedSeries, range),
    [mergedSeries, range]
  );

  const toggleCode = (codes, code) => {
    if (codes.includes(code)) return codes.filter((c) => c !== code);
    return [...codes, code];
  };

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-xl border border-slate-200 p-4">
        <div className="flex items-center justify-between gap-2 mb-3">
          <h2 className="text-lg font-bold">策略组合（账户 {currentAccount}）</h2>
          <button
            onClick={() => setCreateOpen(true)}
            className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700"
          >
            新建策略
          </button>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {portfolios.map((p) => (
            <button
              key={p.id}
              onClick={() => setSelectedId(p.id)}
              className={`px-3 py-1.5 rounded-lg border text-sm ${
                selectedId === p.id
                  ? 'bg-blue-100 text-blue-700 border-blue-200'
                  : 'bg-white text-slate-600 border-slate-200 hover:bg-slate-50'
              }`}
            >
              {p.name}
            </button>
          ))}
        </div>

        {selectedPortfolio && (
          <div className="text-sm text-slate-600 mt-3">
            基准：{selectedPortfolio.benchmark} · 费率：{toPercent(selectedPortfolio.fee_rate)} · 关联标的数：{(selectedPortfolio.scope_codes || []).length}
          </div>
        )}
      </div>

      {selectedId && (
        <div className="bg-white rounded-xl border border-slate-200 p-4 space-y-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <h3 className="font-semibold">绩效与调仓</h3>
            <div className="flex gap-2">
              <button
                onClick={openUpdateModal}
                className="px-3 py-2 rounded-lg border border-slate-300 text-sm hover:bg-slate-50"
              >
                更新策略
              </button>
              <button
                onClick={handleGenerateRebalance}
                className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700"
              >
                生成调仓指令
              </button>
            </div>
          </div>

          {perfLoading ? (
            <div className="text-sm text-slate-500">绩效计算中...</div>
          ) : (
            <>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                <div className="rounded-lg bg-slate-50 p-3">策略本金<br /><strong>{toNumber(performance?.capital?.principal, 2)}</strong></div>
                <div className="rounded-lg bg-slate-50 p-3">当前市值<br /><strong>{toNumber(performance?.capital?.market_value, 2)}</strong></div>
                <div className="rounded-lg bg-slate-50 p-3">策略盈利<br /><strong>{toNumber(performance?.capital?.profit, 2)}</strong></div>
                <div className="rounded-lg bg-slate-50 p-3">收益率<br /><strong>{toPercent(performance?.capital?.profit_rate)}</strong></div>
              </div>

              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                <div className="rounded-lg border p-3">本周回报<br /><strong>{toPercent(performance?.period_returns?.strategy?.week)}</strong></div>
                <div className="rounded-lg border p-3">本月回报<br /><strong>{toPercent(performance?.period_returns?.strategy?.month)}</strong></div>
                <div className="rounded-lg border p-3">本季回报<br /><strong>{toPercent(performance?.period_returns?.strategy?.quarter)}</strong></div>
                <div className="rounded-lg border p-3">本年回报<br /><strong>{toPercent(performance?.period_returns?.strategy?.ytd)}</strong></div>
              </div>

              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                <div className="rounded-lg border p-3">年化收益<br /><strong>{toPercent(performance?.metrics?.strategy?.annual_return)}</strong></div>
                <div className="rounded-lg border p-3">年化波动<br /><strong>{toPercent(performance?.metrics?.strategy?.annual_volatility)}</strong></div>
                <div className="rounded-lg border p-3">Sharpe<br /><strong>{toNumber(performance?.metrics?.strategy?.sharpe, 3)}</strong></div>
                <div className="rounded-lg border p-3">Alpha<br /><strong>{toPercent(performance?.metrics?.strategy?.alpha)}</strong></div>
                <div className="rounded-lg border p-3">Beta<br /><strong>{toNumber(performance?.metrics?.strategy?.beta, 3)}</strong></div>
                <div className="rounded-lg border p-3">Calmar<br /><strong>{toNumber(performance?.metrics?.strategy?.calmar, 3)}</strong></div>
                <div className="rounded-lg border p-3">信息比率<br /><strong>{toNumber(performance?.metrics?.strategy?.information_ratio, 3)}</strong></div>
                <div className="rounded-lg border p-3">最大回撤<br /><strong>{toPercent(performance?.metrics?.strategy?.max_drawdown)}</strong></div>
              </div>

              <div className="text-sm text-slate-600">
                盘中预估回报：策略 {toPercent(performance?.intraday_est_return?.strategy)} · 实际持仓 {toPercent(performance?.intraday_est_return?.actual)}
              </div>

              <div className="bg-slate-50 border rounded-xl p-3">
                <div className="flex flex-wrap gap-2 mb-2">
                  {RANGE_OPTIONS.map((r) => (
                    <button
                      key={r.key}
                      onClick={() => setRange(r.key)}
                      className={`px-3 py-1.5 rounded-lg text-xs border ${
                        range === r.key
                          ? 'bg-blue-600 text-white border-blue-600'
                          : 'bg-white border-slate-300 text-slate-600 hover:bg-slate-100'
                      }`}
                    >
                      {r.label}
                    </button>
                  ))}
                </div>
                <div className="h-72">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="date" minTickGap={28} />
                      <YAxis tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} />
                      <Tooltip formatter={(v) => toPercent(v)} />
                      <Legend />
                      <Line type="monotone" dataKey="strategy" stroke="#2563eb" name="组合" dot={false} strokeWidth={2} />
                      <Line type="monotone" dataKey="benchmark" stroke="#16a34a" name="基准" dot={false} strokeWidth={2} />
                      <Line type="monotone" dataKey="excess" stroke="#dc2626" name="超额" dot={false} strokeWidth={2} />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </>
          )}

          {baseLoading ? (
            <div className="text-sm text-slate-500">调仓数据加载中...</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-slate-50 text-slate-600">
                  <tr>
                    <th className="px-2 py-2 text-left">标的</th>
                    <th className="px-2 py-2 text-right">操作</th>
                    <th className="px-2 py-2 text-right">当前权重</th>
                    <th className="px-2 py-2 text-right">目标权重</th>
                    <th className="px-2 py-2 text-right">调整份额</th>
                    <th className="px-2 py-2 text-right">交易金额</th>
                    <th className="px-2 py-2 text-right">手续费</th>
                    <th className="px-2 py-2 text-right">状态</th>
                  </tr>
                </thead>
                <tbody>
                  {orders.slice(0, 80).map((o) => (
                    <tr key={o.id} className="border-t">
                      <td className="px-2 py-2">{o.fund_name || o.fund_code} <span className="text-slate-400">({o.fund_code})</span></td>
                      <td className="px-2 py-2 text-right">{o.action === 'buy' ? '买入' : o.action === 'sell' ? '卖出' : '保持'}</td>
                      <td className="px-2 py-2 text-right">{toPercent(o.current_weight)}</td>
                      <td className="px-2 py-2 text-right">{toPercent(o.target_weight)}</td>
                      <td className="px-2 py-2 text-right">{toNumber(o.delta_shares, 4)}</td>
                      <td className="px-2 py-2 text-right">{toNumber(o.trade_amount, 2)}</td>
                      <td className="px-2 py-2 text-right">{toNumber(o.fee, 2)}</td>
                      <td className="px-2 py-2 text-right">
                        <select
                          value={o.status}
                          onChange={(e) => handleOrderStatus(o.id, e.target.value)}
                          className="border rounded px-2 py-1"
                        >
                          <option value="suggested">待执行</option>
                          <option value="executed">已执行</option>
                          <option value="skipped">跳过</option>
                        </select>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {createOpen && (
        <Modal title="新建策略" onClose={() => setCreateOpen(false)}>
          <form onSubmit={handleCreate} className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
              <input
                value={createForm.name}
                onChange={(e) => setCreateForm((prev) => ({ ...prev, name: e.target.value }))}
                className="border rounded-lg px-3 py-2 text-sm"
                placeholder="策略名称"
              />
              <input
                value={createForm.benchmark}
                onChange={(e) => setCreateForm((prev) => ({ ...prev, benchmark: e.target.value }))}
                className="border rounded-lg px-3 py-2 text-sm"
                placeholder="基准代码"
              />
              <input
                value={createForm.feeRate}
                onChange={(e) => setCreateForm((prev) => ({ ...prev, feeRate: e.target.value }))}
                className="border rounded-lg px-3 py-2 text-sm"
                placeholder="费率，如0.0015"
              />
            </div>

            <div className="border rounded-lg p-3">
              <div className="font-medium text-sm mb-2">1) 关联策略持仓范围（从当前账户勾选）</div>
              <div className="max-h-40 overflow-auto grid grid-cols-2 md:grid-cols-3 gap-2 text-sm">
                {accountPositions.map((p) => (
                  <label key={p.code} className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={createForm.selectedCodes.includes(p.code)}
                      onChange={() => setCreateForm((prev) => ({
                        ...prev,
                        selectedCodes: toggleCode(prev.selectedCodes, p.code),
                      }))}
                    />
                    <span>{p.code}</span>
                  </label>
                ))}
              </div>
            </div>

            <div className="border rounded-lg p-3">
              <div className="font-medium text-sm mb-2">2) 填写目标权重</div>
              <textarea
                value={createForm.holdingsText}
                onChange={(e) => setCreateForm((prev) => ({ ...prev, holdingsText: e.target.value }))}
                className="w-full border rounded-lg px-3 py-2 text-sm h-36"
                placeholder={'每行一个标的，例如\n513330 25%\n518880 20%'}
              />
              <div className="flex gap-2 mt-2">
                <button type="button" onClick={applySample} className="px-3 py-2 text-sm rounded-lg border border-slate-300 hover:bg-slate-50">
                  填充全天候示例
                </button>
                <button
                  type="button"
                  onClick={() => importFromSelectedPositions(createForm.selectedCodes, setCreateForm)}
                  className="px-3 py-2 text-sm rounded-lg border border-slate-300 hover:bg-slate-50"
                >
                  按已选持仓当前权重导入
                </button>
              </div>
            </div>

            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setCreateOpen(false)} className="px-3 py-2 border rounded-lg text-sm">取消</button>
              <button type="submit" className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700">创建策略</button>
            </div>
          </form>
        </Modal>
      )}

      {updateOpen && (
        <Modal title="更新策略（发布新一期组合）" onClose={() => setUpdateOpen(false)}>
          <form onSubmit={handleUpdateStrategy} className="space-y-4">
            <input
              value={updateForm.note}
              onChange={(e) => setUpdateForm((prev) => ({ ...prev, note: e.target.value }))}
              className="w-full border rounded-lg px-3 py-2 text-sm"
              placeholder="版本备注，如：2026Q1再平衡"
            />

            <div className="border rounded-lg p-3">
              <div className="font-medium text-sm mb-2">1) 调整策略关联持仓范围</div>
              <div className="max-h-40 overflow-auto grid grid-cols-2 md:grid-cols-3 gap-2 text-sm">
                {accountPositions.map((p) => (
                  <label key={p.code} className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={updateForm.selectedCodes.includes(p.code)}
                      onChange={() => setUpdateForm((prev) => ({
                        ...prev,
                        selectedCodes: toggleCode(prev.selectedCodes, p.code),
                      }))}
                    />
                    <span>{p.code}</span>
                  </label>
                ))}
              </div>
            </div>

            <div className="border rounded-lg p-3">
              <div className="font-medium text-sm mb-2">2) 输入新一期目标权重</div>
              <textarea
                value={updateForm.holdingsText}
                onChange={(e) => setUpdateForm((prev) => ({ ...prev, holdingsText: e.target.value }))}
                className="w-full border rounded-lg px-3 py-2 text-sm h-36"
                placeholder={'每行一个标的，例如\n513330 25%\n518880 20%'}
              />
              <div className="flex gap-2 mt-2">
                <button
                  type="button"
                  onClick={() => importFromSelectedPositions(updateForm.selectedCodes, setUpdateForm)}
                  className="px-3 py-2 text-sm rounded-lg border border-slate-300 hover:bg-slate-50"
                >
                  按已选持仓当前权重导入
                </button>
              </div>
            </div>

            <div className="text-xs text-slate-500">提交后会自动生成新的调仓指令。</div>

            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setUpdateOpen(false)} className="px-3 py-2 border rounded-lg text-sm">取消</button>
              <button type="submit" className="px-3 py-2 rounded-lg bg-slate-800 text-white text-sm hover:bg-black">更新策略并生成调仓</button>
            </div>
          </form>
        </Modal>
      )}
    </div>
  );
}

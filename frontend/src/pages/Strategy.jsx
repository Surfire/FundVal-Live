import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  addPositionTrade,
  createStrategyPortfolio,
  createStrategyVersion,
  deleteStrategyPortfolio,
  executeRebalanceOrder,
  generateStrategyRebalance,
  getAccountPositions,
  getStrategyPerformance,
  getStrategyPortfolio,
  getStrategyPositionsView,
  listRebalanceOrders,
  listStrategyPortfolios,
  reducePositionTrade,
  searchFunds,
  updateRebalanceOrderStatus,
} from '../services/api';

const RANGE_OPTIONS = [
  { key: 'since', label: '成立以来' },
  { key: '1w', label: '近1周' },
  { key: '1m', label: '近1月' },
  { key: '3m', label: '近3月' },
  { key: '1y', label: '近1年' },
  { key: 'ytd', label: '年初至今' },
];

const TAB_OPTIONS = [
  { key: 'performance', label: '业绩' },
  { key: 'holdings', label: '持仓' },
  { key: 'announcements', label: '公告' },
  { key: 'news', label: '资讯' },
];

function toPercent(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return '--';
  return `${(v * 100).toFixed(2)}%`;
}

function toNumber(v, n = 2) {
  if (v === null || v === undefined || Number.isNaN(v)) return '--';
  return Number(v).toFixed(n);
}

function getRateColor(value) {
  if (value > 0) return 'text-red-500';
  if (value < 0) return 'text-green-500';
  return 'text-slate-500';
}

function mergeSeries(strategySeries = [], benchmarkSeries = []) {
  const safeStrategy = Array.isArray(strategySeries) ? strategySeries : [];
  const safeBenchmark = Array.isArray(benchmarkSeries) ? benchmarkSeries : [];
  const strategyStartDate = safeStrategy.length ? safeStrategy[0].date : null;

  const m = new Map();
  safeStrategy.forEach((p) => {
    m.set(p.date, { date: p.date, strategy: p.return, benchmark: null });
  });
  safeBenchmark.forEach((p) => {
    if (!m.has(p.date)) {
      m.set(p.date, { date: p.date, strategy: null, benchmark: p.return });
    } else {
      m.get(p.date).benchmark = p.return;
    }
  });

  return Array.from(m.values())
    .sort((a, b) => a.date.localeCompare(b.date))
    .filter((d) => !strategyStartDate || d.date >= strategyStartDate)
    .map((d) => ({
      ...d,
      excess: d.strategy === null || d.benchmark === null ? null : d.strategy - d.benchmark,
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

function Modal({ title, children, onClose }) {
  return (
    <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-4xl max-h-[90vh] overflow-auto">
        <div className="px-5 py-4 border-b flex items-center justify-between">
          <h3 className="text-lg font-semibold">{title}</h3>
          <button onClick={onClose} className="text-slate-500 hover:text-slate-700">关闭</button>
        </div>
        <div className="p-5">{children}</div>
      </div>
    </div>
  );
}

function StrategyBuilder({
  mode,
  onClose,
  onSubmit,
  accountPositions,
  initialName = '新策略',
  initialBenchmark = '000300',
  initialFeeRate = '0.0015',
  initialRows = [],
  initialScope = [],
}) {
  const makeRow = (init = {}) => ({
    id: `${Date.now()}-${Math.random().toString(16).slice(2, 10)}`,
    code: '',
    name: '',
    weight: '',
    ...init,
  });

  const isCreate = mode === 'create';
  const [step, setStep] = useState(1);
  const [name, setName] = useState(initialName);
  const [benchmark, setBenchmark] = useState(initialBenchmark);
  const [feeRate, setFeeRate] = useState(initialFeeRate);
  const [note, setNote] = useState('');
  const [rows, setRows] = useState(initialRows.length ? initialRows.map((r) => makeRow(r)) : [makeRow()]);
  const [selectedCodes, setSelectedCodes] = useState(initialScope);

  const toggleScope = (code) => {
    setSelectedCodes((prev) => {
      if (prev.includes(code)) return prev.filter((c) => c !== code);
      return [...prev, code];
    });
  };

  const addRow = () => setRows((prev) => [...prev, makeRow()]);
  const removeRow = (id) => setRows((prev) => prev.filter((r) => r.id !== id));

  const resolveName = async (idx, code) => {
    const q = code.trim();
    if (!q || q.length < 5) return;
    try {
      const results = await searchFunds(q);
      const exact = results.find((x) => x.id === q) || results[0];
      if (!exact) return;
      setRows((prev) => prev.map((r, i) => (i === idx ? { ...r, code: exact.id || r.code, name: exact.name || r.name } : r)));
    } catch {
      // ignore
    }
  };

  const canGoStep2 = useMemo(() => {
    const validRows = rows.filter((r) => r.code.trim().length >= 5 && Number(r.weight) > 0);
    return validRows.length > 0;
  }, [rows]);

  const goStep2 = () => {
    const targetCodes = rows
      .map((r) => r.code.trim())
      .filter((code) => code.length >= 5);
    setSelectedCodes((prev) => Array.from(new Set([...(prev || []), ...targetCodes])));
    setStep(2);
  };

  const submit = async (e) => {
    e.preventDefault();
    const holdings = rows
      .filter((r) => r.code.trim().length >= 5 && Number(r.weight) > 0)
      .map((r) => ({ code: r.code.trim(), weight: Number(r.weight) }));

    if (!holdings.length) {
      alert('请至少填写一个目标标的');
      return;
    }

    const payload = {
      name,
      benchmark,
      fee_rate: parseFloat(feeRate || '0'),
      note,
      holdings,
      scope_codes: selectedCodes,
    };

    await onSubmit(payload);
  };

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="flex gap-2 text-sm">
        <div className={`px-3 py-1.5 rounded-full ${step === 1 ? 'bg-blue-600 text-white' : 'bg-slate-100 text-slate-600'}`}>1. 目标组合</div>
        <div className={`px-3 py-1.5 rounded-full ${step === 2 ? 'bg-blue-600 text-white' : 'bg-slate-100 text-slate-600'}`}>2. 关联持仓范围</div>
      </div>

      {step === 1 && (
        <div className="space-y-3">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
            {isCreate ? (
              <input value={name} onChange={(e) => setName(e.target.value)} placeholder="策略名称" className="border rounded-lg px-3 py-2 text-sm" />
            ) : (
              <input value={note} onChange={(e) => setNote(e.target.value)} placeholder="版本备注，如 2026Q1 再平衡" className="border rounded-lg px-3 py-2 text-sm md:col-span-2" />
            )}
            <input value={benchmark} onChange={(e) => setBenchmark(e.target.value)} placeholder="基准代码" className="border rounded-lg px-3 py-2 text-sm" />
            <input value={feeRate} onChange={(e) => setFeeRate(e.target.value)} placeholder="费率，如 0.0015" className="border rounded-lg px-3 py-2 text-sm" />
          </div>

          <div className="border rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-slate-50 text-slate-600">
                <tr>
                  <th className="px-2 py-2 text-left">基金代码</th>
                  <th className="px-2 py-2 text-left">基金名称（自动识别）</th>
                  <th className="px-2 py-2 text-right">目标权重(%)</th>
                  <th className="px-2 py-2 text-right">操作</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row, idx) => (
                  <tr key={row.id} className="border-t">
                    <td className="px-2 py-2">
                      <input
                        value={row.code}
                        onChange={(e) => {
                          const code = e.target.value;
                          setRows((prev) => prev.map((r, i) => (i === idx ? { ...r, code } : r)));
                        }}
                        onBlur={() => resolveName(idx, row.code)}
                        placeholder="如 513330"
                        className="w-full border rounded px-2 py-1"
                      />
                    </td>
                    <td className="px-2 py-2">
                      <input
                        value={row.name}
                        onChange={(e) => setRows((prev) => prev.map((r, i) => (i === idx ? { ...r, name: e.target.value } : r)))}
                        placeholder="自动识别或手工填写"
                        className="w-full border rounded px-2 py-1"
                      />
                    </td>
                    <td className="px-2 py-2 text-right">
                      <input
                        type="number"
                        min="0"
                        step="0.01"
                        value={row.weight}
                        onChange={(e) => setRows((prev) => prev.map((r, i) => (i === idx ? { ...r, weight: e.target.value } : r)))}
                        className="w-24 border rounded px-2 py-1 text-right"
                      />
                    </td>
                    <td className="px-2 py-2 text-right">
                      <button type="button" onClick={() => removeRow(row.id)} className="px-2 py-1 text-red-600 hover:bg-red-50 rounded">删除</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <button type="button" onClick={addRow} className="px-3 py-2 border rounded-lg text-sm hover:bg-slate-50">新增标的</button>

          <div className="flex justify-end gap-2">
            <button type="button" onClick={onClose} className="px-3 py-2 border rounded-lg text-sm">取消</button>
            <button type="button" onClick={goStep2} disabled={!canGoStep2} className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm disabled:opacity-50">下一步</button>
          </div>
        </div>
      )}

      {step === 2 && (
        <div className="space-y-3">
          <div className="text-sm text-slate-600">从当前账户持仓里勾选本策略要跟踪的持仓范围（已自动勾选目标组合中的标的）。</div>
          <div className="max-h-72 overflow-auto border rounded-lg p-3 grid grid-cols-1 md:grid-cols-2 gap-2 text-sm">
            {accountPositions.map((p) => (
              <label key={p.code} className="flex items-center gap-2">
                <input type="checkbox" checked={selectedCodes.includes(p.code)} onChange={() => toggleScope(p.code)} />
                <span>{p.name || '--'} ({p.code})</span>
              </label>
            ))}
          </div>

          <div className="flex justify-between">
            <button type="button" onClick={() => setStep(1)} className="px-3 py-2 border rounded-lg text-sm">上一步</button>
            <div className="flex gap-2">
              <button type="button" onClick={onClose} className="px-3 py-2 border rounded-lg text-sm">取消</button>
              <button type="submit" className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm">{isCreate ? '创建策略' : '发布新一期并生成调仓'}</button>
            </div>
          </div>
        </div>
      )}
    </form>
  );
}

export default function Strategy({ currentAccount = 1, isActive = false, onSelectFund = null }) {
  const [portfolios, setPortfolios] = useState([]);
  const [selectedId, setSelectedId] = useState(null);
  const [detail, setDetail] = useState(null);

  const [tab, setTab] = useState('performance');
  const [range, setRange] = useState('since');

  const [performance, setPerformance] = useState(null);
  const [positionsView, setPositionsView] = useState({ rows: [], summary: {} });
  const [orders, setOrders] = useState([]);

  const [accountPositions, setAccountPositions] = useState([]);

  const [loadingBase, setLoadingBase] = useState(false);
  const [loadingPerf, setLoadingPerf] = useState(false);
  const [loadingHoldings, setLoadingHoldings] = useState(false);

  const [createOpen, setCreateOpen] = useState(false);
  const [updateOpen, setUpdateOpen] = useState(false);
  const [addModal, setAddModal] = useState(null);
  const [reduceModal, setReduceModal] = useState(null);
  const [tradeAmount, setTradeAmount] = useState('');
  const [tradeShares, setTradeShares] = useState('');
  const [tradeSubmitting, setTradeSubmitting] = useState(false);
  const [execModal, setExecModal] = useState(null);
  const [execShares, setExecShares] = useState('');
  const [execPrice, setExecPrice] = useState('');
  const [execNote, setExecNote] = useState('');

  const selectedPortfolio = useMemo(
    () => portfolios.find((p) => p.id === selectedId),
    [portfolios, selectedId]
  );

  const loadPortfolios = useCallback(async () => {
    const data = await listStrategyPortfolios(currentAccount);
    setPortfolios(data);
    setSelectedId((prev) => {
      if (!data.length) return null;
      if (!prev && data.length) return data[0].id;
      if (prev && !data.some((x) => x.id === prev) && data.length) return data[0].id;
      return prev;
    });
  }, [currentAccount]);

  const loadAccountPositions = useCallback(async () => {
    const res = await getAccountPositions(currentAccount);
    setAccountPositions(res.positions || []);
  }, [currentAccount]);

  const loadBase = useCallback(async (portfolioId) => {
    if (!portfolioId) return;
    const [d, o] = await Promise.all([
      getStrategyPortfolio(portfolioId),
      listRebalanceOrders(portfolioId, currentAccount),
    ]);
    setDetail(d);
    setOrders(Array.isArray(o) ? o : []);
  }, [currentAccount]);

  const loadPerformance = useCallback(async (portfolioId) => {
    if (!portfolioId) return;
    const data = await getStrategyPerformance(portfolioId, currentAccount);
    setPerformance(data || null);
  }, [currentAccount]);

  const loadHoldings = useCallback(async (portfolioId) => {
    if (!portfolioId) return;
    const data = await getStrategyPositionsView(portfolioId, currentAccount);
    setPositionsView({
      rows: Array.isArray(data?.rows) ? data.rows : [],
      summary: data?.summary || {},
    });
  }, [currentAccount]);

  useEffect(() => {
    loadPortfolios().catch(() => {});
    loadAccountPositions().catch(() => {});
  }, [loadPortfolios, loadAccountPositions]);

  useEffect(() => {
    if (!selectedId) return;
    setLoadingBase(true);
    loadBase(selectedId)
      .catch(() => setOrders([]))
      .finally(() => setLoadingBase(false));

    setLoadingPerf(true);
    loadPerformance(selectedId)
      .catch(() => setPerformance(null))
      .finally(() => setLoadingPerf(false));

    setLoadingHoldings(true);
    loadHoldings(selectedId)
      .catch(() => setPositionsView({ rows: [], summary: {} }))
      .finally(() => setLoadingHoldings(false));
  }, [selectedId, loadBase, loadPerformance, loadHoldings]);

  useEffect(() => {
    if (!isActive || !selectedId) return;
    const t = setInterval(() => {
      loadBase(selectedId).catch(() => {});
      loadPerformance(selectedId).catch(() => {});
      loadHoldings(selectedId).catch(() => {});
    }, 30000);
    return () => clearInterval(t);
  }, [isActive, selectedId, loadBase, loadPerformance, loadHoldings]);

  const handleCreateStrategy = async (payload) => {
    await createStrategyPortfolio({ ...payload, account_id: currentAccount });
    await Promise.all([loadPortfolios(), loadAccountPositions()]);
    setCreateOpen(false);
  };

  const handleUpdateStrategy = async (payload) => {
    if (!selectedId) return;
    await createStrategyVersion(selectedId, {
      holdings: payload.holdings,
      benchmark: payload.benchmark,
      note: payload.note || '更新策略组合',
      activate: true,
      scope_codes: payload.scope_codes,
    });
    await generateStrategyRebalance(selectedId, {
      account_id: currentAccount,
      min_deviation: 0.005,
      persist: true,
    });

    await Promise.all([
      loadBase(selectedId),
      loadPerformance(selectedId),
      loadHoldings(selectedId),
      loadPortfolios(),
    ]);
    setUpdateOpen(false);
  };

  const handleDeleteStrategy = async () => {
    if (!selectedId) return;
    if (!confirm('删除后该策略的版本和调仓记录会一并删除，确认继续？')) return;
    try {
      await deleteStrategyPortfolio(selectedId);
      setDetail(null);
      setPerformance(null);
      setPositionsView({ rows: [], summary: {} });
      setOrders([]);
      await loadPortfolios();
    } catch (e) {
      alert(e?.response?.data?.detail || '删除策略失败');
    }
  };

  const handleGenerateRebalance = async () => {
    if (!selectedId) return;
    await generateStrategyRebalance(selectedId, {
      account_id: currentAccount,
      min_deviation: 0.005,
      persist: true,
    });
    const o = await listRebalanceOrders(selectedId, currentAccount);
    setOrders(o);
  };

  const openAddModal = (row) => {
    setAddModal(row);
    setTradeAmount('');
  };

  const openReduceModal = (row) => {
    setReduceModal(row);
    setTradeShares('');
  };

  const submitAddTrade = async (e) => {
    e.preventDefault();
    if (!addModal || !tradeAmount || Number(tradeAmount) <= 0 || tradeSubmitting) return;
    setTradeSubmitting(true);
    try {
      await addPositionTrade(addModal.code, { amount: Number(tradeAmount) }, currentAccount);
      setAddModal(null);
      await Promise.all([selectedId ? loadHoldings(selectedId) : Promise.resolve(), loadAccountPositions()]);
    } catch (err) {
      alert(err?.response?.data?.detail || '加仓失败');
    } finally {
      setTradeSubmitting(false);
    }
  };

  const submitReduceTrade = async (e) => {
    e.preventDefault();
    if (!reduceModal || !tradeShares || Number(tradeShares) <= 0 || tradeSubmitting) return;
    if (Number(tradeShares) > Number(reduceModal.shares || 0)) {
      alert(`减仓份额不能大于当前份额 ${reduceModal.shares}`);
      return;
    }
    setTradeSubmitting(true);
    try {
      await reducePositionTrade(reduceModal.code, { shares: Number(tradeShares) }, currentAccount);
      setReduceModal(null);
      await Promise.all([selectedId ? loadHoldings(selectedId) : Promise.resolve(), loadAccountPositions()]);
    } catch (err) {
      alert(err?.response?.data?.detail || '减仓失败');
    } finally {
      setTradeSubmitting(false);
    }
  };

  const handleOrderStatus = async (orderId, status) => {
    await updateRebalanceOrderStatus(orderId, status);
    if (!selectedId) return;
    const o = await listRebalanceOrders(selectedId, currentAccount);
    setOrders(o);
  };

  const openExecuteModal = (order) => {
    setExecModal(order);
    setExecShares(String(Math.abs(Number(order.delta_shares || 0)) || ''));
    setExecPrice(String(Number(order.price || 0) || ''));
    setExecNote('');
  };

  const submitExecuteOrder = async (e) => {
    e.preventDefault();
    if (!execModal || tradeSubmitting) return;
    const shares = Number(execShares);
    const price = Number(execPrice);
    if (!(shares > 0) || !(price > 0)) {
      alert('请填写有效的成交份额和成交价格');
      return;
    }
    setTradeSubmitting(true);
    try {
      await executeRebalanceOrder(execModal.id, {
        executed_shares: shares,
        executed_price: price,
        note: execNote,
      });
      setExecModal(null);
      if (selectedId) {
        await Promise.all([
          loadBase(selectedId),
          loadHoldings(selectedId),
          loadPerformance(selectedId),
          loadAccountPositions(),
        ]);
      }
    } catch (err) {
      alert(err?.response?.data?.detail || '执行调仓失败');
    } finally {
      setTradeSubmitting(false);
    }
  };

  const mergedSeries = useMemo(() => {
    const strategy = performance?.series?.strategy || [];
    const benchmark = performance?.series?.benchmark || [];
    return mergeSeries(strategy, benchmark);
  }, [performance]);

  const chartData = useMemo(() => filterRange(mergedSeries, range), [mergedSeries, range]);

  const holdingRows = Array.isArray(positionsView?.rows) ? positionsView.rows : [];
  const orderRows = Array.isArray(orders) ? orders : [];

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-xl border border-slate-200 p-4">
        <div className="flex items-center justify-between gap-2 mb-3">
          <h2 className="text-lg font-bold">策略组合（账户 {currentAccount}）</h2>
          <div className="flex gap-2">
            <button onClick={() => setCreateOpen(true)} className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700">新建策略</button>
            <button onClick={handleDeleteStrategy} disabled={!selectedId} className="px-3 py-2 rounded-lg border border-red-300 text-red-600 text-sm hover:bg-red-50 disabled:opacity-50">删除策略</button>
          </div>
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
            基准：{selectedPortfolio.benchmark} · 费率：{toPercent(selectedPortfolio.fee_rate)} · 关联范围：{(selectedPortfolio.scope_codes || []).length} 个标的
          </div>
        )}
      </div>

      {selectedId && (
        <div className="bg-white rounded-xl border border-slate-200 p-4 space-y-4">
          <div className="flex items-center justify-between">
            <div className="flex gap-2">
              {TAB_OPTIONS.map((t) => (
                <button
                  key={t.key}
                  onClick={() => setTab(t.key)}
                  className={`px-3 py-1.5 rounded-lg text-sm border ${
                    tab === t.key ? 'bg-blue-100 border-blue-200 text-blue-700' : 'bg-white border-slate-200 text-slate-600 hover:bg-slate-50'
                  }`}
                >
                  {t.label}
                </button>
              ))}
            </div>
            {(tab === 'performance' || tab === 'holdings') && (
              <button onClick={() => setUpdateOpen(true)} className="px-3 py-2 rounded-lg border border-slate-300 text-sm hover:bg-slate-50">更新策略</button>
            )}
          </div>

          {tab === 'performance' && (
            <>
              {loadingPerf ? (
                <div className="text-sm text-slate-500">业绩数据加载中...</div>
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
                      <LineChart width={900} height={280} data={chartData} style={{ maxWidth: '100%' }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="date" minTickGap={28} />
                        <YAxis tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} />
                        <Tooltip formatter={(v) => toPercent(v)} />
                        <Legend />
                        <Line type="monotone" dataKey="strategy" stroke="#2563eb" name="组合" dot={false} strokeWidth={2} />
                        <Line type="monotone" dataKey="benchmark" stroke="#16a34a" name="基准" dot={false} strokeWidth={2} />
                        <Line type="monotone" dataKey="excess" stroke="#dc2626" name="超额" dot={false} strokeWidth={2} />
                      </LineChart>
                    </div>
                  </div>
                </>
              )}
            </>
          )}

          {tab === 'holdings' && (
            <>
              <div className="flex items-center justify-between">
                <div className="text-sm text-slate-600">当前策略关联持仓视图</div>
                <button onClick={handleGenerateRebalance} className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700">生成调仓指令</button>
              </div>

              {loadingHoldings ? (
                <div className="text-sm text-slate-500">持仓数据加载中...</div>
              ) : (
                <div className="bg-white rounded-xl shadow-sm border border-slate-200">
                  <table className="w-full text-base text-left border-collapse">
                    <thead className="bg-slate-50 text-slate-500 font-medium text-xs uppercase tracking-wider">
                      <tr>
                        <th className="px-4 py-3 text-left border-b border-slate-100 bg-slate-50 rounded-tl-xl">基金</th>
                        <th className="px-4 py-3 text-right border-b border-slate-100 bg-slate-50">现价 | 成本</th>
                        <th className="px-4 py-3 text-right border-b border-slate-100 bg-slate-50">份额 | 市值</th>
                        <th className="px-4 py-3 text-right border-b border-slate-100 bg-slate-50">持有收益</th>
                        <th className="px-4 py-3 text-right border-b border-slate-100 bg-slate-50">当前 vs 目标</th>
                        <th className="px-4 py-3 text-center border-b border-slate-100 bg-slate-50 rounded-tr-xl">操作</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 text-base">
                      {holdingRows.length === 0 ? (
                        <tr>
                          <td colSpan="6" className="px-4 py-8 text-center text-slate-400">
                            当前策略范围暂无持仓
                          </td>
                        </tr>
                      ) : holdingRows.map((r) => (
                        <tr key={r.code} className="hover:bg-slate-50 transition-colors">
                          <td
                            className={`px-4 py-3 max-w-[180px] ${onSelectFund ? 'cursor-pointer group' : ''}`}
                            onClick={() => onSelectFund && onSelectFund(r.code)}
                          >
                            <div className={`font-medium truncate ${onSelectFund ? 'text-slate-800 group-hover:text-blue-600' : 'text-slate-800'}`} title={r.name}>{r.name}</div>
                            <div className="text-xs text-slate-400 font-mono">{r.code}</div>
                          </td>
                          <td className="px-4 py-3 text-right font-mono">
                            <div className="text-slate-700">{toNumber(r.price, 4)}</div>
                            <div className="text-xs text-slate-400">{toNumber(r.cost, 4)}</div>
                          </td>
                          <td className="px-4 py-3 text-right font-mono text-slate-600">
                            <div>{toNumber(r.shares, 4)}</div>
                            <div className="text-xs text-slate-400">{toNumber(r.market_value, 2)}</div>
                          </td>
                          <td className="px-4 py-3 text-right font-mono">
                            <div className={`font-medium ${getRateColor(r.profit)}`}>
                              {r.profit > 0 ? '+' : ''}{toNumber(r.profit, 2)}
                            </div>
                            <div className={`text-xs ${getRateColor(r.profit_rate)}`}>
                              {r.profit_rate > 0 ? '+' : ''}{toPercent(r.profit_rate)}
                            </div>
                          </td>
                          <td className="px-4 py-3 text-right font-mono">
                            <div className="text-slate-700">{toPercent(r.current_weight)}</div>
                            <div className={`text-xs ${getRateColor(-r.deviation)}`}>
                              目标 {toPercent(r.target_weight)}
                            </div>
                          </td>
                          <td className="px-4 py-3">
                            <div className="flex justify-center gap-2">
                              <button
                                onClick={() => openAddModal(r)}
                                className="p-1.5 text-slate-400 hover:text-emerald-600 hover:bg-emerald-50 rounded-md transition-colors"
                                title="加仓"
                              >
                                +
                              </button>
                              <button
                                onClick={() => openReduceModal(r)}
                                className="p-1.5 text-slate-400 hover:text-amber-600 hover:bg-amber-50 rounded-md transition-colors"
                                title="减仓"
                              >
                                -
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {loadingBase ? (
                <div className="text-sm text-slate-500">调仓指令加载中...</div>
              ) : (
                <div className="overflow-x-auto border rounded-lg">
                  <table className="w-full text-sm">
                    <thead className="bg-slate-50 text-slate-600">
                      <tr>
                        <th className="px-2 py-2 text-left">标的</th>
                        <th className="px-2 py-2 text-right">操作</th>
                        <th className="px-2 py-2 text-right">调整份额</th>
                        <th className="px-2 py-2 text-right">交易金额</th>
                        <th className="px-2 py-2 text-right">手续费</th>
                        <th className="px-2 py-2 text-right">执行信息</th>
                        <th className="px-2 py-2 text-right">状态</th>
                        <th className="px-2 py-2 text-right">操作</th>
                      </tr>
                    </thead>
                    <tbody>
                      {orderRows.slice(0, 80).map((o) => (
                        <tr key={o.id} className="border-t">
                          <td className="px-2 py-2">{o.fund_name || o.fund_code} <span className="text-slate-400">({o.fund_code})</span></td>
                          <td className="px-2 py-2 text-right">{o.action === 'buy' ? '买入' : o.action === 'sell' ? '卖出' : '保持'}</td>
                          <td className="px-2 py-2 text-right">{toNumber(o.delta_shares, 4)}</td>
                          <td className="px-2 py-2 text-right">{toNumber(o.trade_amount, 2)}</td>
                          <td className="px-2 py-2 text-right">{toNumber(o.fee, 2)}</td>
                          <td className="px-2 py-2 text-right">
                            {o.executed_shares ? (
                              <div>
                                <div>{toNumber(o.executed_shares, 4)} @ {toNumber(o.executed_price, 4)}</div>
                                <div className="text-xs text-slate-400">{o.executed_at ? String(o.executed_at).slice(0, 16) : '--'}</div>
                              </div>
                            ) : (
                              <span className="text-slate-400">--</span>
                            )}
                          </td>
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
                          <td className="px-2 py-2 text-right">
                            {o.status === 'suggested' && (o.action === 'buy' || o.action === 'sell') ? (
                              <button
                                onClick={() => openExecuteModal(o)}
                                className="px-2 py-1 text-xs rounded border hover:bg-slate-50"
                              >
                                确认调仓
                              </button>
                            ) : (
                              <span className="text-slate-400 text-xs">--</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}

          {tab === 'announcements' && (
            <div className="rounded-lg border border-dashed p-6 text-sm text-slate-500">公告模块待接入。后续会按本策略持仓范围汇总相关基金/ETF公告。</div>
          )}

          {tab === 'news' && (
            <div className="rounded-lg border border-dashed p-6 text-sm text-slate-500">资讯模块待接入。后续会按本策略持仓范围汇总ETF与行业资讯。</div>
          )}
        </div>
      )}

      {createOpen && (
        <Modal title="新建策略" onClose={() => setCreateOpen(false)}>
          <StrategyBuilder
            mode="create"
            onClose={() => setCreateOpen(false)}
            onSubmit={handleCreateStrategy}
            accountPositions={accountPositions}
          />
        </Modal>
      )}

      {updateOpen && detail && (
        <Modal title="发布新一期组合" onClose={() => setUpdateOpen(false)}>
          <StrategyBuilder
            mode="update"
            onClose={() => setUpdateOpen(false)}
            onSubmit={handleUpdateStrategy}
            accountPositions={accountPositions}
            initialRows={(detail.active_holdings || []).map((h) => ({ code: h.code, name: '', weight: (h.weight * 100).toFixed(2) }))}
            initialScope={detail.portfolio?.scope_codes || []}
          />
        </Modal>
      )}

      {addModal && (
        <Modal title={`加仓 ${addModal.name || addModal.code}`} onClose={() => setAddModal(null)}>
          <form onSubmit={submitAddTrade} className="space-y-3">
            <div className="text-sm text-slate-600">输入加仓金额（元）</div>
            <input
              type="number"
              min="0"
              step="0.01"
              value={tradeAmount}
              onChange={(e) => setTradeAmount(e.target.value)}
              className="w-full border rounded-lg px-3 py-2"
              placeholder="例如 5000"
            />
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setAddModal(null)} className="px-3 py-2 border rounded-lg text-sm">取消</button>
              <button type="submit" disabled={tradeSubmitting} className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm disabled:opacity-50">确认加仓</button>
            </div>
          </form>
        </Modal>
      )}

      {reduceModal && (
        <Modal title={`减仓 ${reduceModal.name || reduceModal.code}`} onClose={() => setReduceModal(null)}>
          <form onSubmit={submitReduceTrade} className="space-y-3">
            <div className="text-sm text-slate-600">输入减仓份额（当前可用：{toNumber(reduceModal.shares, 4)}）</div>
            <input
              type="number"
              min="0"
              step="0.0001"
              value={tradeShares}
              onChange={(e) => setTradeShares(e.target.value)}
              className="w-full border rounded-lg px-3 py-2"
              placeholder="例如 1000"
            />
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setReduceModal(null)} className="px-3 py-2 border rounded-lg text-sm">取消</button>
              <button type="submit" disabled={tradeSubmitting} className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm disabled:opacity-50">确认减仓</button>
            </div>
          </form>
        </Modal>
      )}

      {execModal && (
        <Modal title={`确认调仓 · ${execModal.fund_name || execModal.fund_code}`} onClose={() => setExecModal(null)}>
          <form onSubmit={submitExecuteOrder} className="space-y-3">
            <div className="text-sm text-slate-600">
              操作方向：{execModal.action === 'buy' ? '买入' : '卖出'}，建议份额：{toNumber(Math.abs(execModal.delta_shares || 0), 4)}
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-1">实际成交份额</label>
              <input
                type="number"
                min="0.0001"
                step="0.0001"
                value={execShares}
                onChange={(e) => setExecShares(e.target.value)}
                className="w-full border rounded-lg px-3 py-2"
                required
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-1">实际成交价格</label>
              <input
                type="number"
                min="0.0001"
                step="0.0001"
                value={execPrice}
                onChange={(e) => setExecPrice(e.target.value)}
                className="w-full border rounded-lg px-3 py-2"
                required
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-1">备注（可选）</label>
              <input
                type="text"
                value={execNote}
                onChange={(e) => setExecNote(e.target.value)}
                className="w-full border rounded-lg px-3 py-2"
                placeholder="如：分两笔成交"
              />
            </div>
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setExecModal(null)} className="px-3 py-2 border rounded-lg text-sm">取消</button>
              <button type="submit" disabled={tradeSubmitting} className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm disabled:opacity-50">确认并同步持仓</button>
            </div>
          </form>
        </Modal>
      )}
    </div>
  );
}

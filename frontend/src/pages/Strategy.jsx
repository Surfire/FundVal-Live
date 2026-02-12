import React, { useCallback, useEffect, useMemo, useState } from 'react';
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
import {
  addPositionTrade,
  completeRebalanceBatch,
  createStrategyPortfolio,
  createStrategyVersion,
  deleteStrategyPortfolio,
  executeRebalanceOrder,
  generateStrategyRebalance,
  getAccountPositions,
  getFundDetail,
  getStrategyPerformance,
  getStrategyPortfolio,
  getStrategyPositionsView,
  getStrategyScopeCandidates,
  listRebalanceBatches,
  listRebalanceOrders,
  listStrategyPortfolios,
  refreshRebalanceBatch,
  reducePositionTrade,
  recognizeStrategyHoldingsFromImage,
  searchFunds,
  updateStrategyScope,
  updateRebalanceOrderStatus,
} from '../services/api';
import StrategyBacktest from './StrategyBacktest';

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

function toShares(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return '--';
  const num = Number(v);
  if (Math.abs(num - Math.round(num)) < 1e-8) return String(Math.round(num));
  return num.toFixed(2);
}

function getRateColor(value) {
  if (value > 0) return 'text-red-500';
  if (value < 0) return 'text-green-500';
  return 'text-slate-500';
}

function getBatchTypeLabel(source = '') {
  if (source === 'smart_rebalance') return '智能再平衡';
  if (source === 'auto') return '智能再平衡';
  if (source === 'manual') return '手动调仓';
  if (source === 'add') return '加仓';
  if (source === 'reduce') return '减仓';
  return '调仓';
}

function formatBeijingTime(value) {
  if (!value) return '--';
  const text = String(value).trim();
  if (!text) return '--';
  const iso = text.includes('T') ? text : text.replace(' ', 'T');
  const utcCandidate = iso.endsWith('Z') ? iso : `${iso}Z`;
  const d = new Date(utcCandidate);
  if (Number.isNaN(d.getTime())) return text.slice(0, 16);
  return d.toLocaleString('zh-CN', {
    hour12: false,
    timeZone: 'Asia/Shanghai',
  }).replace(/\//g, '-').slice(0, 16);
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
  submitting = false,
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
  const [ocrLoading, setOcrLoading] = useState(false);
  const [ocrMessage, setOcrMessage] = useState('');

  const toggleScope = (code) => {
    setSelectedCodes((prev) => {
      if (prev.includes(code)) return prev.filter((c) => c !== code);
      return [...prev, code];
    });
  };

  const addRow = () => setRows((prev) => [...prev, makeRow()]);
  const removeRow = (id) => setRows((prev) => prev.filter((r) => r.id !== id));

  const applyRecognizedHoldings = (items = []) => {
    if (!Array.isArray(items) || items.length === 0) return;
    const recognized = new Map();
    items.forEach((it) => {
      const code = String(it?.code || '').trim();
      if (!code) return;
      const weightPct = Number(it?.weight_pct);
      recognized.set(code, {
        ...makeRow(),
        code,
        name: it?.name || '',
        weight: Number.isFinite(weightPct) && weightPct > 0 ? String(weightPct.toFixed(2)) : '',
      });
    });

    let nextRows = Array.from(recognized.values());
    if (isCreate) {
      const merged = new Map();
      rows.forEach((r) => {
        const code = String(r.code || '').trim();
        if (!code) return;
        merged.set(code, { ...r });
      });
      nextRows.forEach((r) => {
        const prev = merged.get(r.code);
        merged.set(r.code, {
          ...(prev || makeRow()),
          ...r,
          name: r.name || prev?.name || '',
          weight: r.weight || prev?.weight || '',
        });
      });
      nextRows = Array.from(merged.values());
    }

    if (nextRows.length === 0) return;
    setRows(nextRows);
    setSelectedCodes((prev) => Array.from(new Set([...(prev || []), ...nextRows.map((x) => x.code).filter(Boolean)])));
  };

  const recognizeFromDataUrl = async (dataUrl) => {
    setOcrLoading(true);
    setOcrMessage('');
    try {
      const res = await recognizeStrategyHoldingsFromImage(dataUrl);
      const items = res?.holdings || [];
      applyRecognizedHoldings(items);
      setOcrMessage(`识别完成：${items.length} 个标的已填充`);
    } catch (err) {
      setOcrMessage(err?.response?.data?.detail || '图片识别失败');
    } finally {
      setOcrLoading(false);
    }
  };

  const handleFileSelect = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    if (!file.type.startsWith('image/')) {
      setOcrMessage('请上传图片文件');
      return;
    }
    if (file.size > 8 * 1024 * 1024) {
      setOcrMessage('图片过大，请控制在 8MB 以内');
      return;
    }
    const reader = new FileReader();
    reader.onload = async () => {
      const dataUrl = String(reader.result || '');
      if (dataUrl) await recognizeFromDataUrl(dataUrl);
    };
    reader.readAsDataURL(file);
  };

  const handlePasteImage = async (e) => {
    const items = e.clipboardData?.items || [];
    for (const item of items) {
      if (!item.type || !item.type.startsWith('image/')) continue;
      const file = item.getAsFile();
      if (!file) continue;
      const reader = new FileReader();
      reader.onload = async () => {
        const dataUrl = String(reader.result || '');
        if (dataUrl) await recognizeFromDataUrl(dataUrl);
      };
      reader.readAsDataURL(file);
      e.preventDefault();
      return;
    }
  };

  const resolveName = async (idx, code) => {
    const q = code.trim();
    if (!q || q.length < 5) return;
    try {
      const results = await searchFunds(q);
      const exact = results.find((x) => x.id === q) || results[0];
      if (exact) {
        setRows((prev) => prev.map((r, i) => (i === idx ? { ...r, code: exact.id || r.code, name: exact.name || r.name } : r)));
        return;
      }
      const detail = await getFundDetail(q);
      if (detail?.name) {
        setRows((prev) => prev.map((r, i) => (i === idx ? { ...r, code: q, name: detail.name } : r)));
      }
    } catch {
      try {
        const detail = await getFundDetail(q);
        if (detail?.name) {
          setRows((prev) => prev.map((r, i) => (i === idx ? { ...r, code: q, name: detail.name } : r)));
        }
      } catch {
        // ignore
      }
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
    if (submitting) return;
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
          <div className="border rounded-lg p-3 bg-slate-50 space-y-2">
            <div className="text-sm font-medium text-slate-700">智能识别持仓（图片）</div>
            <div className="flex flex-wrap items-center gap-2">
              <label className="px-3 py-2 rounded border bg-white text-sm hover:bg-slate-100 cursor-pointer">
                上传图片
                <input type="file" accept="image/*" className="hidden" onChange={handleFileSelect} />
              </label>
              <div
                onPaste={handlePasteImage}
                className="px-3 py-2 rounded border bg-white text-sm text-slate-600 min-w-[280px]"
              >
                在此区域粘贴截图（Ctrl/Cmd + V）
              </div>
              {ocrLoading && <span className="text-xs text-blue-600">识别中...</span>}
            </div>
            {ocrMessage && <div className="text-xs text-slate-600">{ocrMessage}</div>}
          </div>

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
            <button type="button" onClick={() => setStep(1)} className="px-3 py-2 border rounded-lg text-sm" disabled={submitting}>上一步</button>
            <div className="flex gap-2">
              <button type="button" onClick={onClose} className="px-3 py-2 border rounded-lg text-sm" disabled={submitting}>取消</button>
              <button type="submit" disabled={submitting} className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm disabled:opacity-50 disabled:cursor-not-allowed">
                {submitting ? '处理中...' : (isCreate ? '创建策略' : '发布新一期并生成调仓')}
              </button>
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
  const [backtestPageOpen, setBacktestPageOpen] = useState(false);

  const [performance, setPerformance] = useState(null);
  const [positionsView, setPositionsView] = useState({ rows: [], summary: {} });
  const [batches, setBatches] = useState([]);
  const [selectedBatchId, setSelectedBatchId] = useState(null);
  const [batchDetailOpen, setBatchDetailOpen] = useState(false);
  const [batchOrders, setBatchOrders] = useState([]);
  const [loadingBatchOrders, setLoadingBatchOrders] = useState(false);

  const [accountPositions, setAccountPositions] = useState([]);

  const [loadingBase, setLoadingBase] = useState(false);
  const [loadingPerf, setLoadingPerf] = useState(false);
  const [loadingHoldings, setLoadingHoldings] = useState(false);
  const [submittingCreate, setSubmittingCreate] = useState(false);
  const [submittingUpdate, setSubmittingUpdate] = useState(false);
  const [submittingSmart, setSubmittingSmart] = useState(false);

  const [createOpen, setCreateOpen] = useState(false);
  const [updateOpen, setUpdateOpen] = useState(false);
  const [smartOpen, setSmartOpen] = useState(false);
  const [scopeEditOpen, setScopeEditOpen] = useState(false);
  const [scopeSelectedCodes, setScopeSelectedCodes] = useState([]);
  const [scopeOptions, setScopeOptions] = useState([]);
  const [hideZeroShares, setHideZeroShares] = useState(true);
  const [addModal, setAddModal] = useState(null);
  const [reduceModal, setReduceModal] = useState(null);
  const [tradeAmount, setTradeAmount] = useState('');
  const [tradeShares, setTradeShares] = useState('');
  const [tradeSubmitting, setTradeSubmitting] = useState(false);
  const [execSubmitting, setExecSubmitting] = useState(false);
  const [execModal, setExecModal] = useState(null);
  const [execShares, setExecShares] = useState('');
  const [execPrice, setExecPrice] = useState('');
  const [execNote, setExecNote] = useState('');
  const [rebalanceMinDeviation, setRebalanceMinDeviation] = useState('0.5');
  const [rebalanceLotSize, setRebalanceLotSize] = useState('100');
  const [rebalanceCapitalAdjust, setRebalanceCapitalAdjust] = useState('0');
  const [rebalanceTitle, setRebalanceTitle] = useState('');
  const [refreshingBatchId, setRefreshingBatchId] = useState(null);

  const selectedPortfolio = useMemo(
    () => portfolios.find((p) => p.id === selectedId),
    [portfolios, selectedId]
  );

  const scopeCandidates = useMemo(
    () => (Array.isArray(scopeOptions) ? scopeOptions : []),
    [scopeOptions]
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
    const [d, b, s] = await Promise.all([
      getStrategyPortfolio(portfolioId),
      listRebalanceBatches(portfolioId, currentAccount),
      getStrategyScopeCandidates(portfolioId, currentAccount),
    ]);
    setDetail(d);
    setBatches(Array.isArray(b) ? b : []);
    setScopeOptions(Array.isArray(s) ? s : []);
    setSelectedBatchId((prev) => {
      if (!Array.isArray(b) || b.length === 0) return null;
      if (prev && b.some((item) => item.id === prev)) return prev;
      return b[0].id;
    });
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
    setBacktestPageOpen(false);
    setLoadingBase(true);
    loadBase(selectedId)
      .catch(() => setBatches([]))
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
    if (submittingCreate) return;
    setSubmittingCreate(true);
    try {
      await createStrategyPortfolio({ ...payload, account_id: currentAccount });
      await Promise.all([loadPortfolios(), loadAccountPositions()]);
      setCreateOpen(false);
    } finally {
      setSubmittingCreate(false);
    }
  };

  const handleUpdateStrategy = async (payload) => {
    if (!selectedId) return;
    if (submittingUpdate) return;
    setSubmittingUpdate(true);
    try {
      await createStrategyVersion(selectedId, {
        holdings: payload.holdings,
        benchmark: payload.benchmark,
        fee_rate: payload.fee_rate,
        note: payload.note || '更新策略组合',
        activate: true,
        scope_codes: payload.scope_codes,
      });
      await generateStrategyRebalance(selectedId, {
        account_id: currentAccount,
        min_deviation: 0.005,
        lot_size: 100,
        persist: true,
      });

      await Promise.all([
        loadBase(selectedId),
        loadPerformance(selectedId),
        loadHoldings(selectedId),
        loadPortfolios(),
      ]);
      setUpdateOpen(false);
    } finally {
      setSubmittingUpdate(false);
    }
  };

  const handleDeleteStrategy = async () => {
    if (!selectedId) return;
    if (!confirm('删除后该策略的版本和调仓记录会一并删除，确认继续？')) return;
    try {
      await deleteStrategyPortfolio(selectedId);
      setDetail(null);
      setPerformance(null);
      setPositionsView({ rows: [], summary: {} });
      setBatches([]);
      setBatchOrders([]);
      setScopeOptions([]);
      setBatchDetailOpen(false);
      await loadPortfolios();
    } catch (e) {
      alert(e?.response?.data?.detail || '删除策略失败');
    }
  };

  const handleGenerateRebalance = async () => {
    if (!selectedId) return;
    const minDeviation = Math.max(0, Number(rebalanceMinDeviation || 0) / 100);
    const lotSize = Math.max(1, Math.round(Number(rebalanceLotSize || 1)));
    const capitalAdjustment = Number(rebalanceCapitalAdjust || 0);
    const result = await generateStrategyRebalance(selectedId, {
      account_id: currentAccount,
      min_deviation: minDeviation,
      lot_size: lotSize,
      capital_adjustment: capitalAdjustment,
      title: rebalanceTitle,
      persist: true,
    });
    const b = await listRebalanceBatches(selectedId, currentAccount);
    setBatches(b);
    if (result?.batch_id) {
      setSelectedBatchId(result.batch_id);
      await openBatchDetailById(result.batch_id);
    } else if (b.length) {
      setSelectedBatchId(b[0].id);
    }
  };

  const handleRefreshRebalance = async (batchId) => {
    if (!selectedId || !batchId || refreshingBatchId) return;
    setRefreshingBatchId(batchId);
    try {
      await refreshRebalanceBatch(batchId);
      const b = await listRebalanceBatches(selectedId, currentAccount);
      setBatches(Array.isArray(b) ? b : []);
      await openBatchDetailById(batchId);
    } catch (err) {
      alert(err?.response?.data?.detail || '刷新调仓失败');
    } finally {
      setRefreshingBatchId(null);
    }
  };

  const openBatchDetailById = async (batchId) => {
    if (!selectedId || !batchId) return;
    setLoadingBatchOrders(true);
    try {
      setSelectedBatchId(batchId);
      setExecModal(null);
      const rows = await listRebalanceOrders(selectedId, currentAccount, null, batchId);
      setBatchOrders(Array.isArray(rows) ? rows : []);
      setBatchDetailOpen(true);
    } finally {
      setLoadingBatchOrders(false);
    }
  };

  const openScopeEditor = () => {
    const selected = detail?.portfolio?.scope_codes || [];
    setScopeSelectedCodes(selected);
    setScopeEditOpen(true);
  };

  const toggleScopeCode = (code) => {
    setScopeSelectedCodes((prev) => {
      if (prev.includes(code)) return prev.filter((c) => c !== code);
      return [...prev, code];
    });
  };

  const saveScopeCodes = async () => {
    if (!selectedId) return;
    await updateStrategyScope(selectedId, scopeSelectedCodes);
    setScopeEditOpen(false);
    await Promise.all([
      loadBase(selectedId),
      loadHoldings(selectedId),
      loadPerformance(selectedId),
    ]);
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

  const reloadBatchData = async (batchId = selectedBatchId) => {
    if (!selectedId) return;
    const b = await listRebalanceBatches(selectedId, currentAccount);
    setBatches(Array.isArray(b) ? b : []);
    if (batchId) {
      const rows = await listRebalanceOrders(selectedId, currentAccount, null, batchId);
      setBatchOrders(Array.isArray(rows) ? rows : []);
    }
    await Promise.all([loadHoldings(selectedId), loadPerformance(selectedId)]);
  };

  const handleOrderStatus = async (orderId, status) => {
    try {
      await updateRebalanceOrderStatus(orderId, status);
      await reloadBatchData();
    } catch (err) {
      alert(err?.response?.data?.detail || '更新指令状态失败');
    }
  };

  const handleCompleteBatch = async (batchId) => {
    if (!batchId) return;
    try {
      await completeRebalanceBatch(batchId);
      await reloadBatchData(batchId);
    } catch (err) {
      alert(err?.response?.data?.detail || '归档批次失败');
    }
  };

  const openExecuteModal = (order) => {
    setExecModal(order);
    const suggested = Math.abs(Number(order.delta_shares || 0));
    const val = Math.abs(suggested - Math.round(suggested)) < 1e-8 ? String(Math.round(suggested)) : String(suggested.toFixed(2));
    setExecShares(val || '');
    setExecPrice(String(Number(order.price || 0) || ''));
    setExecNote('');
    setExecSubmitting(false);
  };

  const submitExecuteOrder = async (e) => {
    e.preventDefault();
    if (!execModal || execSubmitting) return;
    const shares = Number(execShares);
    const price = Number(execPrice);
    if (!(shares > 0) || !(price > 0)) {
      alert('请填写有效的成交份额和成交价格');
      return;
    }
    setExecSubmitting(true);
    try {
      await executeRebalanceOrder(execModal.id, {
        executed_shares: shares,
        executed_price: price,
        note: execNote,
      });
      setExecModal(null);
      await Promise.all([reloadBatchData(), loadAccountPositions()]);
    } catch (err) {
      alert(err?.response?.data?.detail || '执行调仓失败');
    } finally {
      setExecSubmitting(false);
    }
  };

  const mergedSeries = useMemo(() => {
    const strategy = performance?.series?.strategy || [];
    const benchmark = performance?.series?.benchmark || [];
    return mergeSeries(strategy, benchmark);
  }, [performance]);

  const chartData = useMemo(() => filterRange(mergedSeries, range), [mergedSeries, range]);

  const allHoldingRows = Array.isArray(positionsView?.rows) ? positionsView.rows : [];
  const holdingRows = hideZeroShares
    ? allHoldingRows.filter((r) => Number(r.shares || 0) > 0 || Number(r.target_weight || 0) > 0)
    : allHoldingRows;
  const currentBatch = useMemo(
    () => (selectedBatchId ? (batches || []).find((b) => b.id === selectedBatchId) : null),
    [batches, selectedBatchId]
  );
  const actionableBatchOrders = useMemo(
    () => (batchOrders || []).filter((o) => o.action === 'buy' || o.action === 'sell'),
    [batchOrders]
  );
  const passiveOrderCount = Math.max(0, (batchOrders || []).length - actionableBatchOrders.length);

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
              <div className="flex items-center gap-2">
                {tab === 'performance' && (
                  <button onClick={() => setBacktestPageOpen(true)} className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700">回测</button>
                )}
                {tab === 'holdings' && (
                  <button onClick={() => setSmartOpen(true)} className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700">智能调仓</button>
                )}
                <button onClick={() => setUpdateOpen(true)} className="px-3 py-2 rounded-lg border border-slate-300 text-sm hover:bg-slate-50">更新策略</button>
              </div>
            )}
          </div>

          {tab === 'performance' && (
            <>
              {backtestPageOpen ? (
                <StrategyBacktest
                  portfolio={selectedPortfolio}
                  currentAccount={currentAccount}
                  defaultPrincipal={performance?.capital?.principal}
                  onBack={() => setBacktestPageOpen(false)}
                />
              ) : (
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
                    <div className="h-72 min-h-[220px] min-w-[1px]">
                      <ResponsiveContainer width="100%" height="100%" minWidth={1} minHeight={220}>
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
              </>
              )}
            </>
          )}

          {tab === 'holdings' && (
            <>
              <div className="flex items-center justify-between">
                <div className="text-sm text-slate-600">当前策略关联持仓视图</div>
                <div className="flex items-center gap-2">
                  <button onClick={openScopeEditor} className="px-3 py-2 rounded-lg border border-slate-300 text-sm hover:bg-slate-50">编辑关联标的</button>
                  <button onClick={() => setHideZeroShares((v) => !v)} className="px-3 py-2 rounded-lg border border-slate-300 text-sm hover:bg-slate-50">
                    {hideZeroShares ? '显示全部标的' : '隐藏零份额'}
                  </button>
                </div>
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
                        <th className="px-4 py-3 text-right border-b border-slate-100 bg-slate-50">市值 | 份额</th>
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
                            <div>{toNumber(r.market_value, 2)}</div>
                            <div className="text-xs text-slate-400">{toNumber(r.shares, 4)}</div>
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

              <div className="space-y-3">
                <div className="border rounded-lg p-3 bg-slate-50">
                  <div className="text-sm font-medium text-slate-700 mb-2">调仓批次</div>
                  {loadingBase ? (
                    <div className="text-sm text-slate-500">批次加载中...</div>
                  ) : (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead className="text-slate-600 bg-white">
                          <tr>
                            <th className="px-2 py-2 text-left">批次</th>
                            <th className="px-2 py-2 text-left">日期</th>
                            <th className="px-2 py-2 text-left">类型</th>
                            <th className="px-2 py-2 text-right">买入</th>
                            <th className="px-2 py-2 text-right">卖出</th>
                            <th className="px-2 py-2 text-right">净调仓</th>
                            <th className="px-2 py-2 text-right">待执行</th>
                            <th className="px-2 py-2 text-right">状态</th>
                            <th className="px-2 py-2 text-right">操作</th>
                          </tr>
                        </thead>
                        <tbody>
                          {batches.length === 0 && (
                            <tr>
                              <td colSpan="9" className="px-2 py-6 text-center text-slate-500">暂无调仓批次，点击“智能调仓”生成。</td>
                            </tr>
                          )}
                          {batches.map((b) => (
                            <tr key={b.id} className={`border-t ${selectedBatchId === b.id ? 'bg-blue-50/60' : ''}`}>
                              <td className="px-2 py-2">{b.title || `批次 #${b.id}`}</td>
                              <td className="px-2 py-2">{formatBeijingTime(b.created_at)}</td>
                              <td className="px-2 py-2">{getBatchTypeLabel(b.source)}</td>
                              <td className="px-2 py-2 text-right">{toNumber(b.buy_amount, 2)}</td>
                              <td className="px-2 py-2 text-right">{toNumber(b.sell_amount, 2)}</td>
                              <td className={`px-2 py-2 text-right ${getRateColor(-b.net_amount)}`}>{toNumber(b.net_amount, 2)}</td>
                              <td className="px-2 py-2 text-right">{b.pending_orders}</td>
                              <td className="px-2 py-2 text-right">
                                <span className={`px-2 py-1 rounded text-xs ${
                                  b.status === 'completed'
                                    ? 'bg-emerald-100 text-emerald-700'
                                    : Number(b.pending_orders || 0) > 0
                                      ? 'bg-amber-100 text-amber-700'
                                      : 'bg-blue-100 text-blue-700'
                                }`}>
                                  {b.status === 'completed' ? '已归档' : Number(b.pending_orders || 0) > 0 ? '待执行' : '可归档'}
                                </span>
                              </td>
                              <td className="px-2 py-2 text-right">
                                <button
                                  onClick={() => openBatchDetailById(b.id)}
                                  className="px-2 py-1 text-xs rounded border hover:bg-white"
                                >
                                  查看详情
                                </button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              </div>
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

      {scopeEditOpen && (
        <Modal title="编辑关联标的" onClose={() => setScopeEditOpen(false)}>
          <div className="space-y-3">
            <div className="text-sm text-slate-600">勾选本策略需要纳入持仓视图和调仓计算的标的。</div>
            <div className="max-h-96 overflow-auto border rounded-lg p-3 grid grid-cols-1 md:grid-cols-2 gap-2 text-sm">
              {scopeCandidates.map((p) => (
                <label key={p.code} className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    checked={scopeSelectedCodes.includes(p.code)}
                    onChange={() => toggleScopeCode(p.code)}
                  />
                  <span>{p.name || p.code} ({p.code})</span>
                </label>
              ))}
            </div>
            <div className="flex justify-end gap-2">
              <button onClick={() => setScopeEditOpen(false)} className="px-3 py-2 border rounded-lg text-sm">取消</button>
              <button onClick={saveScopeCodes} className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm">保存</button>
            </div>
          </div>
        </Modal>
      )}

      {createOpen && (
        <Modal title="新建策略" onClose={() => setCreateOpen(false)}>
          <StrategyBuilder
            mode="create"
            onClose={() => setCreateOpen(false)}
            onSubmit={handleCreateStrategy}
            submitting={submittingCreate}
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
            submitting={submittingUpdate}
            accountPositions={accountPositions}
            initialBenchmark={detail.portfolio?.benchmark || selectedPortfolio?.benchmark || '000300'}
            initialFeeRate={String(detail.portfolio?.fee_rate ?? selectedPortfolio?.fee_rate ?? '0.001')}
            initialRows={(detail.active_holdings || []).map((h) => ({ code: h.code, name: h.name || '', weight: (h.weight * 100).toFixed(2) }))}
            initialScope={detail.portfolio?.scope_codes || []}
          />
        </Modal>
      )}

      {smartOpen && (
        <Modal title="智能调仓参数" onClose={() => setSmartOpen(false)}>
          <form
            className="space-y-4"
            onSubmit={async (e) => {
              e.preventDefault();
              if (submittingSmart) return;
              setSubmittingSmart(true);
              try {
                await handleGenerateRebalance();
                setRebalanceTitle('');
                setSmartOpen(false);
              } catch (err) {
                alert(err?.response?.data?.detail || '生成调仓失败');
              } finally {
                setSubmittingSmart(false);
              }
            }}
          >
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              <div className="md:col-span-3">
                <label className="block text-sm font-medium text-slate-700 mb-1">批次名称（可选）</label>
                <input
                  type="text"
                  value={rebalanceTitle}
                  onChange={(e) => setRebalanceTitle(e.target.value)}
                  className="w-full border rounded-lg px-3 py-2"
                  placeholder="不填则自动命名为“智能调仓批次”"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">偏离阈值(%)</label>
                <input
                  type="number"
                  min="0"
                  step="0.1"
                  value={rebalanceMinDeviation}
                  onChange={(e) => setRebalanceMinDeviation(e.target.value)}
                  className="w-full border rounded-lg px-3 py-2"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">最小交易份额</label>
                <input
                  type="number"
                  min="1"
                  step="1"
                  value={rebalanceLotSize}
                  onChange={(e) => setRebalanceLotSize(e.target.value)}
                  className="w-full border rounded-lg px-3 py-2"
                />
                <div className="text-xs text-slate-500 mt-1">ETF 通常设为 100 份</div>
              </div>
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">本金增减(元)</label>
                <input
                  type="number"
                  step="100"
                  value={rebalanceCapitalAdjust}
                  onChange={(e) => setRebalanceCapitalAdjust(e.target.value)}
                  className="w-full border rounded-lg px-3 py-2"
                  placeholder="+120000 或 -50000"
                />
                <div className="text-xs text-slate-500 mt-1">正数加仓，负数减仓</div>
              </div>
            </div>
            <div className="flex justify-end gap-2">
              <button type="button" onClick={() => setSmartOpen(false)} className="px-3 py-2 border rounded-lg text-sm" disabled={submittingSmart}>取消</button>
              <button type="submit" disabled={submittingSmart} className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm disabled:opacity-50 disabled:cursor-not-allowed">
                {submittingSmart ? '生成中...' : '生成智能调仓批次'}
              </button>
            </div>
          </form>
        </Modal>
      )}

      {batchDetailOpen && (
        <Modal
          title={`调仓批次详情${currentBatch ? ` · ${currentBatch.title || `#${currentBatch.id}`}` : ''}`}
          onClose={() => {
            setBatchDetailOpen(false);
            setExecModal(null);
          }}
        >
          <div className="space-y-3">
            {currentBatch && (
              <div className="grid grid-cols-2 md:grid-cols-5 gap-2 text-sm">
                <div className="rounded-lg bg-slate-50 p-2">类型<br /><strong>{getBatchTypeLabel(currentBatch.source)}</strong></div>
                <div className="rounded-lg bg-slate-50 p-2">买入<br /><strong>{toNumber(currentBatch.buy_amount, 2)}</strong></div>
                <div className="rounded-lg bg-slate-50 p-2">卖出<br /><strong>{toNumber(currentBatch.sell_amount, 2)}</strong></div>
                <div className="rounded-lg bg-slate-50 p-2">待执行<br /><strong>{currentBatch.pending_orders}</strong></div>
                <div className="rounded-lg bg-slate-50 p-2">状态<br /><strong>{currentBatch.status === 'completed' ? '已归档' : '待执行'}</strong></div>
              </div>
            )}

            <div className="text-xs text-slate-500">
              无需处理的“保持”标的已自动跳过，当前共 {passiveOrderCount} 条。
            </div>

            {loadingBatchOrders ? (
              <div className="text-sm text-slate-500">批次指令加载中...</div>
            ) : (
              <div className="overflow-x-auto border rounded-lg">
                <table className="w-full min-w-[860px] text-sm">
                  <thead className="bg-slate-50 text-slate-600">
                    <tr>
                      <th className="px-2 py-2 text-left">标的</th>
                      <th className="px-2 py-2 text-right">方向</th>
                      <th className="px-2 py-2 text-right">建议份额</th>
                      <th className="px-2 py-2 text-right">金额</th>
                      <th className="px-2 py-2 text-right">执行信息</th>
                      <th className="px-2 py-2 text-right">状态</th>
                      <th className="px-2 py-2 text-right">操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {actionableBatchOrders.map((o) => (
                      <tr key={o.id} className="border-t">
                        <td className="px-2 py-2 max-w-[220px]">
                          <div className="truncate" title={`${o.fund_name || o.fund_code} (${o.fund_code})`}>
                            {o.fund_name || o.fund_code} <span className="text-slate-400">({o.fund_code})</span>
                          </div>
                        </td>
                        <td className="px-2 py-2 text-right whitespace-nowrap">{o.action === 'buy' ? '买入' : '卖出'}</td>
                        <td className="px-2 py-2 text-right whitespace-nowrap">{toShares(o.delta_shares)}</td>
                        <td className="px-2 py-2 text-right whitespace-nowrap">{toNumber(o.trade_amount, 2)}</td>
                        <td className="px-2 py-2 text-right whitespace-nowrap">
                          {o.executed_shares ? (
                            <div>
                              <div>{toShares(o.executed_shares)} @ {toNumber(o.executed_price, 4)}</div>
                              <div className="text-xs text-slate-400">{o.executed_at ? formatBeijingTime(o.executed_at) : '--'}</div>
                            </div>
                          ) : (
                            <span className="text-slate-400">--</span>
                          )}
                        </td>
                        <td className="px-2 py-2 text-right whitespace-nowrap">
                          <span className={`inline-flex px-2 py-0.5 rounded text-xs ${
                            o.status === 'executed'
                              ? 'bg-emerald-100 text-emerald-700'
                              : o.status === 'skipped'
                                ? 'bg-slate-100 text-slate-600'
                                : 'bg-amber-100 text-amber-700'
                          }`}>
                            {o.status === 'suggested' ? '待执行' : o.status === 'executed' ? '已执行' : '已跳过'}
                          </span>
                        </td>
                        <td className="px-2 py-2 text-right whitespace-nowrap">
                          <div className="flex justify-end gap-1 whitespace-nowrap">
                            {currentBatch?.status !== 'completed' && o.status === 'suggested' && (
                              <>
                                <button
                                  onClick={() => openExecuteModal(o)}
                                  className="px-1.5 py-1 text-xs rounded border hover:bg-slate-50"
                                >
                                  执行
                                </button>
                                <button
                                  onClick={() => handleOrderStatus(o.id, 'skipped')}
                                  className="px-1.5 py-1 text-xs rounded border hover:bg-slate-50"
                                >
                                  跳过
                                </button>
                              </>
                            )}
                            {(currentBatch?.status === 'completed' || o.status !== 'suggested') && (
                              <span className="text-slate-400 text-xs">--</span>
                            )}
                          </div>
                        </td>
                      </tr>
                    ))}
                    {actionableBatchOrders.length === 0 && (
                      <tr>
                        <td colSpan="7" className="px-2 py-6 text-center text-slate-500">该批次没有需要执行的买卖指令。</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            )}

            <div className="flex justify-end gap-2">
              <button
                type="button"
                onClick={() => handleRefreshRebalance(currentBatch?.id)}
                disabled={!currentBatch || currentBatch.status === 'completed' || refreshingBatchId !== null}
                className="px-3 py-2 rounded-lg border text-sm disabled:opacity-50"
                title="按当前最新价格重算当前批次，不新增记录"
              >
                {refreshingBatchId === currentBatch?.id ? '刷新中...' : '刷新计算'}
              </button>
              <button type="button" onClick={() => setBatchDetailOpen(false)} className="px-3 py-2 border rounded-lg text-sm">关闭</button>
              <button
                type="button"
                onClick={() => handleCompleteBatch(currentBatch?.id)}
                disabled={!currentBatch || currentBatch.status === 'completed' || Number(currentBatch.pending_orders || 0) > 0}
                className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm disabled:opacity-50"
                title={Number(currentBatch?.pending_orders || 0) > 0 ? '仍有待执行指令，无法归档' : ''}
              >
                完成并归档批次
              </button>
            </div>
          </div>
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
              操作方向：{execModal.action === 'buy' ? '买入' : '卖出'}，建议份额：{toShares(Math.abs(execModal.delta_shares || 0))}
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-1">实际成交份额</label>
              <input
                type="number"
                min="0.01"
                step="0.01"
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
              <button type="submit" disabled={execSubmitting} className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm disabled:opacity-50">确认并同步持仓</button>
            </div>
          </form>
        </Modal>
      )}
    </div>
  );
}

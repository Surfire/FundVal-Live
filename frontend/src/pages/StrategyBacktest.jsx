import React, { useMemo, useState } from 'react';
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
import { analyzeBacktestWithAI, getBacktestPromptPresets, runStrategyBacktest } from '../services/api';

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

function todayStr() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function oneYearAgoStr() {
  const d = new Date();
  d.setFullYear(d.getFullYear() - 1);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function safePositiveNumber(value, fallback) {
  const n = Number(value);
  if (Number.isFinite(n) && n > 0) return n;
  return fallback;
}

export default function StrategyBacktest({
  portfolio,
  currentAccount,
  defaultPrincipal,
  onBack,
}) {
  const [startDate, setStartDate] = useState(oneYearAgoStr());
  const [endDate, setEndDate] = useState(todayStr());
  const [initialCapital, setInitialCapital] = useState(String(Math.round(safePositiveNumber(defaultPrincipal, 100000))));
  const [rebalanceMode, setRebalanceMode] = useState('threshold');
  const [thresholdPct, setThresholdPct] = useState('0.5');
  const [periodicDays, setPeriodicDays] = useState('20');
  const [feeRate, setFeeRate] = useState(String(portfolio?.fee_rate ?? 0.001));
  const [cacheOnly, setCacheOnly] = useState(false);
  const [range, setRange] = useState('since');
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState(null);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiResult, setAiResult] = useState(null);
  const [aiError, setAiError] = useState('');
  const [aiPrompts, setAiPrompts] = useState([]);
  const [aiStyle, setAiStyle] = useState('hardcore_audit');

  const chartData = useMemo(() => {
    const merged = mergeSeries(result?.series?.strategy || [], result?.series?.benchmark || []);
    return filterRange(merged, range);
  }, [result, range]);

  React.useEffect(() => {
    let mounted = true;
    getBacktestPromptPresets()
      .then((list) => {
        if (!mounted) return;
        setAiPrompts(Array.isArray(list) ? list : []);
        if (Array.isArray(list) && list.length > 0) {
          setAiStyle(list[0].key || 'hardcore_audit');
        }
      })
      .catch(() => {});
    return () => { mounted = false; };
  }, []);

  const run = async (e) => {
    e.preventDefault();
    if (!portfolio?.id) return;
    const initialCapitalNum = Number(initialCapital);
    if (!Number.isFinite(initialCapitalNum) || initialCapitalNum <= 0) {
      alert('初始本金请输入有效的正数');
      return;
    }
    setRunning(true);
    try {
      const res = await runStrategyBacktest(portfolio.id, {
        account_id: currentAccount,
        start_date: startDate,
        end_date: endDate,
        initial_capital: initialCapitalNum,
        rebalance_mode: rebalanceMode,
        threshold: Math.max(0, Number(thresholdPct || 0) / 100),
        periodic_days: Math.max(1, Number(periodicDays || 1)),
        fee_rate: Number(feeRate || 0),
        cache_only: cacheOnly,
      });
      setResult(res || null);
      setAiResult(null);
      setAiError('');
      setRange('since');
    } catch (err) {
      alert(err?.response?.data?.detail || '回测失败');
    } finally {
      setRunning(false);
    }
  };

  const runAi = async () => {
    if (!result) return;
    setAiLoading(true);
    setAiError('');
    try {
      const data = await analyzeBacktestWithAI(result, aiStyle);
      setAiResult(data || null);
    } catch (err) {
      setAiError(err?.response?.data?.detail || 'AI 分析失败');
    } finally {
      setAiLoading(false);
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-slate-900">策略回测</h2>
          <div className="text-sm text-slate-500">{portfolio?.name || '--'} · 基准默认沪深300</div>
        </div>
        <button onClick={onBack} className="px-3 py-2 border rounded-lg text-sm hover:bg-slate-50">返回业绩</button>
      </div>

      <form onSubmit={run} className="bg-white border rounded-xl p-4 space-y-3">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
          <div>
            <label className="block text-sm text-slate-600 mb-1">开始日期</label>
            <input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} className="w-full border rounded-lg px-3 py-2 text-sm" />
          </div>
          <div>
            <label className="block text-sm text-slate-600 mb-1">结束日期</label>
            <input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} className="w-full border rounded-lg px-3 py-2 text-sm" />
          </div>
          <div>
            <label className="block text-sm text-slate-600 mb-1">初始本金</label>
            <input type="number" min="1" step="1" value={initialCapital} onChange={(e) => setInitialCapital(e.target.value)} className="w-full border rounded-lg px-3 py-2 text-sm" />
          </div>
          <div>
            <label className="block text-sm text-slate-600 mb-1">费率</label>
            <input type="number" min="0" step="0.0001" value={feeRate} onChange={(e) => setFeeRate(e.target.value)} className="w-full border rounded-lg px-3 py-2 text-sm" />
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
          <div>
            <label className="block text-sm text-slate-600 mb-1">再平衡模式</label>
            <select value={rebalanceMode} onChange={(e) => setRebalanceMode(e.target.value)} className="w-full border rounded-lg px-3 py-2 text-sm">
              <option value="none">不再平衡</option>
              <option value="threshold">偏离阈值</option>
              <option value="periodic">固定周期</option>
              <option value="hybrid">阈值 + 周期</option>
            </select>
          </div>
          <div>
            <label className="block text-sm text-slate-600 mb-1">偏离阈值(%)</label>
            <input type="number" min="0" step="0.1" value={thresholdPct} onChange={(e) => setThresholdPct(e.target.value)} className="w-full border rounded-lg px-3 py-2 text-sm" disabled={rebalanceMode === 'none' || rebalanceMode === 'periodic'} />
          </div>
          <div>
            <label className="block text-sm text-slate-600 mb-1">固定周期(交易日)</label>
            <input type="number" min="1" step="1" value={periodicDays} onChange={(e) => setPeriodicDays(e.target.value)} className="w-full border rounded-lg px-3 py-2 text-sm" disabled={rebalanceMode === 'none' || rebalanceMode === 'threshold'} />
          </div>
          <div className="flex items-end">
            <label className="inline-flex items-center gap-2 text-sm text-slate-700">
              <input type="checkbox" checked={cacheOnly} onChange={(e) => setCacheOnly(e.target.checked)} />
              仅用本地缓存数据
            </label>
          </div>
        </div>

        <div className="flex justify-end">
          <button type="submit" disabled={running} className="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm disabled:opacity-50">
            {running ? '回测中...' : '开始回测'}
          </button>
        </div>
      </form>

      {result && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
            <div className="rounded-lg bg-slate-50 p-3">初始本金<br /><strong>{toNumber(result?.capital?.principal, 2)}</strong></div>
            <div className="rounded-lg bg-slate-50 p-3">期末市值<br /><strong>{toNumber(result?.capital?.market_value, 2)}</strong></div>
            <div className="rounded-lg bg-slate-50 p-3">回测盈利<br /><strong>{toNumber(result?.capital?.profit, 2)}</strong></div>
            <div className="rounded-lg bg-slate-50 p-3">收益率<br /><strong>{toPercent(result?.capital?.profit_rate)}</strong></div>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
            <div className="rounded-lg border p-3">本周回报<br /><strong>{toPercent(result?.period_returns?.strategy?.week)}</strong></div>
            <div className="rounded-lg border p-3">本月回报<br /><strong>{toPercent(result?.period_returns?.strategy?.month)}</strong></div>
            <div className="rounded-lg border p-3">本季回报<br /><strong>{toPercent(result?.period_returns?.strategy?.quarter)}</strong></div>
            <div className="rounded-lg border p-3">年初至今<br /><strong>{toPercent(result?.period_returns?.strategy?.ytd)}</strong></div>
            <div className="rounded-lg border p-3">年化收益<br /><strong>{toPercent(result?.metrics?.strategy?.annual_return)}</strong></div>
            <div className="rounded-lg border p-3">年化波动<br /><strong>{toPercent(result?.metrics?.strategy?.annual_volatility)}</strong></div>
            <div className="rounded-lg border p-3">Sharpe<br /><strong>{toNumber(result?.metrics?.strategy?.sharpe, 3)}</strong></div>
            <div className="rounded-lg border p-3">Alpha<br /><strong>{toPercent(result?.metrics?.strategy?.alpha)}</strong></div>
            <div className="rounded-lg border p-3">Beta<br /><strong>{toNumber(result?.metrics?.strategy?.beta, 3)}</strong></div>
            <div className="rounded-lg border p-3">Calmar<br /><strong>{toNumber(result?.metrics?.strategy?.calmar, 3)}</strong></div>
            <div className="rounded-lg border p-3">信息比率<br /><strong>{toNumber(result?.metrics?.strategy?.information_ratio, 3)}</strong></div>
            <div className="rounded-lg border p-3">最大回撤<br /><strong>{toPercent(result?.metrics?.strategy?.max_drawdown)}</strong></div>
          </div>

          <div className="rounded-lg border p-3 text-sm text-slate-600 grid grid-cols-2 md:grid-cols-4 gap-2">
            <div>调仓次数：<strong>{result?.rebalance_summary?.rebalance_count ?? 0}</strong></div>
            <div>成交笔数：<strong>{result?.rebalance_summary?.trade_count ?? 0}</strong></div>
            <div>换手金额：<strong>{toNumber(result?.rebalance_summary?.turnover, 2)}</strong></div>
            <div>手续费：<strong>{toNumber(result?.rebalance_summary?.fee_total, 2)}</strong></div>
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
            <div className="h-72 min-h-[220px]">
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

          <div className="bg-white border rounded-xl p-4 space-y-3">
            <div className="flex items-center justify-between gap-2">
              <div>
                <h3 className="text-base font-semibold text-slate-800">AI 深度回测分析</h3>
                <div className="text-xs text-slate-500">基于回测指标与调仓行为的策略审计</div>
              </div>
              <div className="flex items-center gap-2">
                <select
                  value={aiStyle}
                  onChange={(e) => setAiStyle(e.target.value)}
                  className="border rounded-lg px-2 py-1 text-sm"
                >
                  {(aiPrompts.length ? aiPrompts : [
                    { key: 'hardcore_audit', name: '硬核审计风格' },
                    { key: 'steady_advisor', name: '稳健顾问风格' },
                  ]).map((p) => (
                    <option key={p.key} value={p.key}>{p.name}</option>
                  ))}
                </select>
                <button
                  onClick={runAi}
                  disabled={aiLoading}
                  className="px-3 py-2 rounded-lg bg-indigo-600 text-white text-sm disabled:opacity-50"
                >
                  {aiLoading ? '分析中...' : (aiResult ? '重新分析' : '开始分析')}
                </button>
              </div>
            </div>

            {aiError && (
              <div className="text-sm text-red-600 bg-red-50 border border-red-200 rounded-lg p-3">{aiError}</div>
            )}

            {aiResult && (
              <div className="space-y-3">
                <div className="flex items-center gap-2 text-sm">
                  <span className="px-2 py-1 rounded bg-indigo-50 border border-indigo-100 text-indigo-700">{aiResult.style_name || aiStyle}</span>
                  <span className="px-2 py-1 rounded bg-slate-50 border border-slate-200 text-slate-700">风险: {aiResult.risk_level || '--'}</span>
                  <span className="text-slate-400 text-xs">生成时间 {aiResult.timestamp || '--:--'}</span>
                </div>
                <div className="rounded-lg border bg-slate-50 p-3">
                  <div className="text-xs text-slate-500 mb-1">核心结论</div>
                  <div className="text-sm font-medium text-slate-800">{aiResult.summary}</div>
                </div>
                <div className="text-sm text-slate-700 whitespace-pre-line">{aiResult.analysis_report}</div>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
                  <div className="rounded-lg border p-3">
                    <div className="font-medium mb-1">优势</div>
                    {(aiResult.strengths || []).length > 0 ? (
                      <ul className="list-disc list-inside text-slate-700">
                        {(aiResult.strengths || []).map((x, i) => <li key={`s-${i}`}>{x}</li>)}
                      </ul>
                    ) : <div className="text-slate-400">--</div>}
                  </div>
                  <div className="rounded-lg border p-3">
                    <div className="font-medium mb-1">风险点</div>
                    {(aiResult.risks || []).length > 0 ? (
                      <ul className="list-disc list-inside text-slate-700">
                        {(aiResult.risks || []).map((x, i) => <li key={`r-${i}`}>{x}</li>)}
                      </ul>
                    ) : <div className="text-slate-400">--</div>}
                  </div>
                  <div className="rounded-lg border p-3">
                    <div className="font-medium mb-1">动作建议</div>
                    {(aiResult.actions || []).length > 0 ? (
                      <ul className="list-disc list-inside text-slate-700">
                        {(aiResult.actions || []).map((x, i) => <li key={`a-${i}`}>{x}</li>)}
                      </ul>
                    ) : <div className="text-slate-400">--</div>}
                  </div>
                </div>
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}

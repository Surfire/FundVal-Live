import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  createStrategyPortfolio,
  createStrategyVersion,
  generateStrategyRebalance,
  getStrategyPerformance,
  getStrategyPortfolio,
  listRebalanceOrders,
  listStrategyPortfolios,
  updateRebalanceOrderStatus,
} from '../services/api';

const ALL_WEATHER_SAMPLE = [
  { code: '513330', weight: 25 },
  { code: '513070', weight: 5 },
  { code: '513120', weight: 5 },
  { code: '164824', weight: 15 },
  { code: '518880', weight: 20 },
  { code: '516650', weight: 10 },
  { code: '159870', weight: 20 },
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

export default function Strategy({ currentAccount = 1, isActive = false }) {
  const [portfolios, setPortfolios] = useState([]);
  const [selectedId, setSelectedId] = useState(null);
  const [detail, setDetail] = useState(null);
  const [performance, setPerformance] = useState(null);
  const [orders, setOrders] = useState([]);
  const [loading, setLoading] = useState(false);

  const [createForm, setCreateForm] = useState({
    name: '全天候策略',
    benchmark: '000300',
    feeRate: '0.0015',
    holdingsText: '',
  });

  const [versionForm, setVersionForm] = useState({
    note: '',
    holdingsText: '',
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

  const loadDetail = useCallback(async (portfolioId) => {
    if (!portfolioId) return;
    const [d, p, o] = await Promise.all([
      getStrategyPortfolio(portfolioId),
      getStrategyPerformance(portfolioId, currentAccount),
      listRebalanceOrders(portfolioId, currentAccount),
    ]);
    setDetail(d);
    setPerformance(p);
    setOrders(o);
  }, [currentAccount]);

  useEffect(() => {
    loadPortfolios();
  }, [loadPortfolios]);

  useEffect(() => {
    if (!selectedId) return;
    setLoading(true);
    loadDetail(selectedId)
      .catch((e) => alert(e?.response?.data?.detail || '加载策略失败'))
      .finally(() => setLoading(false));
  }, [selectedId, loadDetail]);

  useEffect(() => {
    if (!isActive || !selectedId) return;
    const t = setInterval(() => {
      loadDetail(selectedId).catch(() => {});
    }, 30000);
    return () => clearInterval(t);
  }, [isActive, selectedId, loadDetail]);

  const applySample = () => {
    const text = ALL_WEATHER_SAMPLE.map((x) => `${x.code} ${x.weight}%`).join('\n');
    setCreateForm((prev) => ({ ...prev, holdingsText: text }));
  };

  const handleCreate = async (e) => {
    e.preventDefault();
    try {
      const holdings = parseHoldingsText(createForm.holdingsText);
      await createStrategyPortfolio({
        name: createForm.name,
        account_id: currentAccount,
        benchmark: createForm.benchmark,
        fee_rate: parseFloat(createForm.feeRate || '0'),
        holdings,
        note: '初始版本',
      });
      await loadPortfolios();
      alert('策略创建成功');
    } catch (err) {
      alert(err?.response?.data?.detail || err.message || '创建失败');
    }
  };

  const handleCreateVersion = async (e) => {
    e.preventDefault();
    if (!selectedId) return;
    try {
      const holdings = parseHoldingsText(versionForm.holdingsText);
      await createStrategyVersion(selectedId, {
        holdings,
        note: versionForm.note || '新一期组合',
        activate: true,
      });
      setVersionForm({ note: '', holdingsText: '' });
      await loadDetail(selectedId);
      alert('新版本已创建并激活');
    } catch (err) {
      alert(err?.response?.data?.detail || err.message || '创建版本失败');
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
      await loadDetail(selectedId);
      alert('调仓建议已生成');
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

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-xl border border-slate-200 p-4">
        <h2 className="text-lg font-bold mb-3">策略组合（账户 {currentAccount}）</h2>
        <div className="flex flex-wrap items-center gap-2 mb-3">
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
          <div className="text-sm text-slate-600">
            基准：{selectedPortfolio.benchmark} · 费率：{toPercent(selectedPortfolio.fee_rate)}
          </div>
        )}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <form onSubmit={handleCreate} className="bg-white rounded-xl border border-slate-200 p-4 space-y-3">
          <h3 className="font-semibold">新建策略</h3>
          <input
            value={createForm.name}
            onChange={(e) => setCreateForm((prev) => ({ ...prev, name: e.target.value }))}
            className="w-full border rounded-lg px-3 py-2 text-sm"
            placeholder="策略名称"
          />
          <div className="grid grid-cols-2 gap-2">
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
              placeholder="费率，如 0.0015"
            />
          </div>
          <textarea
            value={createForm.holdingsText}
            onChange={(e) => setCreateForm((prev) => ({ ...prev, holdingsText: e.target.value }))}
            className="w-full border rounded-lg px-3 py-2 text-sm h-36"
            placeholder={'每行一个标的，例如\n513330 25%\n518880 20%'}
          />
          <div className="flex gap-2">
            <button type="button" onClick={applySample} className="px-3 py-2 text-sm rounded-lg border border-slate-300 hover:bg-slate-50">
              填充全天候示例
            </button>
            <button type="submit" className="px-3 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700">
              创建策略
            </button>
          </div>
        </form>

        <form onSubmit={handleCreateVersion} className="bg-white rounded-xl border border-slate-200 p-4 space-y-3">
          <h3 className="font-semibold">发布新一期组合</h3>
          <input
            value={versionForm.note}
            onChange={(e) => setVersionForm((prev) => ({ ...prev, note: e.target.value }))}
            className="w-full border rounded-lg px-3 py-2 text-sm"
            placeholder="版本备注"
          />
          <textarea
            value={versionForm.holdingsText}
            onChange={(e) => setVersionForm((prev) => ({ ...prev, holdingsText: e.target.value }))}
            className="w-full border rounded-lg px-3 py-2 text-sm h-36"
            placeholder={'每行一个标的，例如\n513330 25%\n518880 20%'}
          />
          <button
            type="submit"
            disabled={!selectedId}
            className="px-3 py-2 text-sm rounded-lg bg-slate-800 text-white hover:bg-black disabled:opacity-50"
          >
            创建新版本并激活
          </button>
        </form>
      </div>

      {selectedId && (
        <div className="bg-white rounded-xl border border-slate-200 p-4 space-y-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <h3 className="font-semibold">绩效与调仓</h3>
            <button onClick={handleGenerateRebalance} className="px-3 py-2 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700">
              生成调仓建议
            </button>
          </div>

          {loading ? (
            <div className="text-sm text-slate-500">加载中...</div>
          ) : (
            <>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                <div className="rounded-lg bg-slate-50 p-3">本周回报<br /><strong>{toPercent(performance?.period_returns?.strategy?.week)}</strong></div>
                <div className="rounded-lg bg-slate-50 p-3">本月回报<br /><strong>{toPercent(performance?.period_returns?.strategy?.month)}</strong></div>
                <div className="rounded-lg bg-slate-50 p-3">本季回报<br /><strong>{toPercent(performance?.period_returns?.strategy?.quarter)}</strong></div>
                <div className="rounded-lg bg-slate-50 p-3">本年回报<br /><strong>{toPercent(performance?.period_returns?.strategy?.ytd)}</strong></div>
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

              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-slate-50 text-slate-600">
                    <tr>
                      <th className="px-2 py-2 text-left">标的</th>
                      <th className="px-2 py-2 text-right">操作</th>
                      <th className="px-2 py-2 text-right">当前权重</th>
                      <th className="px-2 py-2 text-right">目标权重</th>
                      <th className="px-2 py-2 text-right">调整份额</th>
                      <th className="px-2 py-2 text-right">估算手续费</th>
                      <th className="px-2 py-2 text-right">状态</th>
                    </tr>
                  </thead>
                  <tbody>
                    {orders.slice(0, 50).map((o) => (
                      <tr key={o.id} className="border-t">
                        <td className="px-2 py-2">{o.fund_name || o.fund_code} <span className="text-slate-400">({o.fund_code})</span></td>
                        <td className="px-2 py-2 text-right">{o.action === 'buy' ? '买入' : o.action === 'sell' ? '卖出' : '保持'}</td>
                        <td className="px-2 py-2 text-right">{toPercent(o.current_weight)}</td>
                        <td className="px-2 py-2 text-right">{toPercent(o.target_weight)}</td>
                        <td className="px-2 py-2 text-right">{toNumber(o.delta_shares, 4)}</td>
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

              {detail?.active_holdings?.length > 0 && (
                <div>
                  <h4 className="font-medium text-sm mb-2">当前激活组合</h4>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-sm">
                    {detail.active_holdings.map((h) => (
                      <div key={h.code} className="px-3 py-2 rounded-lg border bg-slate-50">
                        {h.code} · {toPercent(h.weight)}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}

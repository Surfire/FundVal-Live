import React, { useState, useEffect } from 'react';
import { Plus, X, Edit2, Trash2, RefreshCw } from 'lucide-react';
import { getAccountPositions, updatePosition, deletePosition } from '../services/api';
import { getRateColor } from '../components/StatCard';
import { PortfolioChart } from '../components/PortfolioChart';

const Account = ({ onSelectFund, onPositionChange, onSyncWatchlist, syncLoading }) => {
  const [data, setData] = useState({ summary: {}, positions: [] });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [editingPos, setEditingPos] = useState(null);
  const [submitting, setSubmitting] = useState(false);

  // Form State
  const [formData, setFormData] = useState({ code: '', cost: '', shares: '' });

  const fetchData = async (retryCount = 0) => {
    setLoading(true);
    setError(null);
    try {
      const res = await getAccountPositions();
      setData(res);
    } catch (e) {
      console.error(e);

      // Retry logic: retry up to 2 times with exponential backoff
      if (retryCount < 2) {
        const delay = Math.pow(2, retryCount) * 1000; // 1s, 2s
        console.log(`Retrying in ${delay}ms... (attempt ${retryCount + 1}/2)`);
        setTimeout(() => fetchData(retryCount + 1), delay);
      } else {
        setError('加载账户数据失败，请检查后端服务是否启动');
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    // Delay initial fetch to give backend time to start
    const timer = setTimeout(() => fetchData(), 500);
    return () => clearTimeout(timer);
  }, []);

  const handleOpenModal = (pos = null) => {
    if (pos) {
      setEditingPos(pos);
      setFormData({ code: pos.code, cost: pos.cost, shares: pos.shares });
    } else {
      setEditingPos(null);
      setFormData({ code: '', cost: '', shares: '' });
    }
    setModalOpen(true);
  };

  const handleSync = () => {
    const { positions } = data;
    if (!positions || positions.length === 0) return;
    if (confirm(`确定将 ${positions.length} 个持仓基金同步到关注列表吗？`)) {
        onSyncWatchlist && onSyncWatchlist(positions);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!formData.code || !formData.cost || !formData.shares) return;
    if (submitting) return; // Prevent duplicate submission

    setSubmitting(true);
    try {
      await updatePosition({
        code: formData.code,
        cost: parseFloat(formData.cost),
        shares: parseFloat(formData.shares)
      });
      setModalOpen(false);
      onPositionChange && onPositionChange(formData.code, 'add');
      fetchData();
    } catch (e) {
      alert('保存失败');
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (code) => {
    if (!confirm(`确定删除 ${code} 吗？`)) return;
    try {
      await deletePosition(code);
      onPositionChange && onPositionChange(code, 'remove');
      fetchData();
    } catch (e) {
      alert('删除失败');
    }
  };

  const { summary, positions } = data;

  return (
    <div className="space-y-6">
      {/* Error Banner */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="text-red-600">
              <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
              </svg>
            </div>
            <div>
              <p className="text-sm font-medium text-red-800">{error}</p>
            </div>
          </div>
          <button
            onClick={() => fetchData()}
            className="text-sm font-medium text-red-600 hover:text-red-700 underline"
          >
            重试
          </button>
        </div>
      )}

      {/* 1. Portfolio Overview with Summary */}
      <div className="w-full">
        <PortfolioChart positions={positions} summary={summary} loading={loading} onRefresh={fetchData} />
      </div>

      {/* 2. Actions */}
      <div className="flex justify-between items-center">
        <h2 className="text-xl font-bold text-slate-800">持仓明细</h2>
        <div className="flex gap-2">
            <button
              onClick={handleSync}
              disabled={syncLoading}
              className="flex items-center gap-2 bg-white border border-slate-200 text-slate-600 hover:text-blue-600 hover:border-blue-200 px-4 py-2 rounded-lg transition-colors text-sm font-medium disabled:opacity-50 disabled:cursor-not-allowed"
              title="将持仓基金添加到关注列表"
            >
              <RefreshCw className={`w-4 h-4 ${syncLoading ? 'animate-spin' : ''}`} />
              {syncLoading ? '同步中...' : '同步关注'}
            </button>
            <button 
              onClick={() => handleOpenModal()}
              className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors text-sm font-medium"
            >
              <Plus className="w-4 h-4" />
              记一笔
            </button>
        </div>
      </div>

      {/* 3. Table */}
      <div className="bg-white rounded-xl shadow-sm border border-slate-200">
        <div>
          <table className="w-full text-base text-left border-collapse">
            <thead className="bg-slate-50 text-slate-500 font-medium text-xs uppercase tracking-wider sticky top-[73px] z-30 shadow-sm">
              <tr>
                <th className="px-4 py-3 text-left border-b border-slate-100 bg-slate-50 rounded-tl-xl">基金</th>
                <th className="px-4 py-3 text-right border-b border-slate-100 bg-slate-50">净值 | 估值</th>
                <th className="px-4 py-3 text-right border-b border-slate-100 bg-slate-50">份额 | 成本</th>
                <th className="px-4 py-3 text-right border-b border-slate-100 bg-slate-50">持有收益</th>
                <th className="px-4 py-3 text-right border-b border-slate-100 bg-slate-50">当日预估</th>
                <th className="px-4 py-3 text-right border-b border-slate-100 bg-slate-50">预估总值</th>
                <th className="px-4 py-3 text-center border-b border-slate-100 bg-slate-50 rounded-tr-xl">操作</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 text-base">
              {positions.length === 0 ? (
                <tr>
                  <td colSpan="7" className="px-4 py-8 text-center text-slate-400">
                    暂无持仓，快去记一笔吧
                  </td>
                </tr>
              ) : positions.map((pos) => (
                <tr key={pos.code} className="hover:bg-slate-50 transition-colors">
                  <td 
                    className="px-4 py-3 cursor-pointer group max-w-[180px]"
                    onClick={() => onSelectFund && onSelectFund(pos.code)}
                  >
                    <div className="font-medium text-slate-800 group-hover:text-blue-600 transition-colors truncate" title={pos.name}>{pos.name}</div>
                    <div className="text-xs text-slate-400 font-mono">{pos.code}</div>
                  </td>
                  
                  {/* Price Column */}
                  <td className="px-4 py-3 text-right font-mono">
                    <div className="text-slate-500 text-xs" title="昨日净值">{pos.nav.toFixed(4)}</div>
                    <div className={`font-medium ${getRateColor(pos.est_rate)}`} title="实时估值">
                        {pos.estimate > 0 ? pos.estimate.toFixed(4) : '--'}
                    </div>
                  </td>

                  {/* Position Column */}
                  <td className="px-4 py-3 text-right font-mono text-slate-600">
                    <div>{pos.shares.toLocaleString()}</div>
                    <div className="text-xs text-slate-400">{pos.cost.toFixed(4)}</div>
                  </td>

                  {/* Accumulated Income (Historical) */}
                  <td className="px-4 py-3 text-right font-mono">
                    <div className={`font-medium ${getRateColor(pos.accumulated_income)}`}>
                        {pos.accumulated_income > 0 ? '+' : ''}{pos.accumulated_income}
                    </div>
                    <div className={`text-xs ${getRateColor(pos.accumulated_return_rate)}`}>
                        {pos.accumulated_return_rate > 0 ? '+' : ''}{pos.accumulated_return_rate}%
                    </div>
                  </td>

                  {/* Intraday Income (Real-time) */}
                  <td className="px-4 py-3 text-right font-mono">
                    <div className={`font-medium ${!pos.is_est_valid ? 'text-slate-300' : getRateColor(pos.day_income)}`}>
                        {pos.is_est_valid ? (pos.day_income > 0 ? '+' : '') + pos.day_income : '--'}
                    </div>
                    <div className={`text-xs ${!pos.is_est_valid ? 'text-slate-300' : getRateColor(pos.est_rate)}`}>
                        {pos.is_est_valid ? (pos.est_rate > 0 ? '+' : '') + pos.est_rate + '%' : '--'}
                    </div>
                  </td>

                  {/* Total Projected */}
                  <td className="px-4 py-3 text-right font-mono">
                     <div className="text-slate-800 font-medium">{pos.est_market_value.toLocaleString()}</div>
                     <div className={`text-xs ${getRateColor(pos.total_income)}`}>
                        {pos.total_income > 0 ? '+' : ''}{pos.total_income}
                     </div>
                  </td>

                  <td className="px-4 py-3">
                    <div className="flex justify-center gap-2">
                      <button 
                        onClick={() => handleOpenModal(pos)}
                        className="p-1.5 text-slate-400 hover:text-blue-600 hover:bg-blue-50 rounded-md transition-colors"
                      >
                        <Edit2 className="w-4 h-4" />
                      </button>
                      <button 
                        onClick={() => handleDelete(pos.code)}
                        className="p-1.5 text-slate-400 hover:text-red-600 hover:bg-red-50 rounded-md transition-colors"
                      >
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Modal */}
      {modalOpen && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
          <div className="bg-white rounded-xl shadow-xl w-full max-w-sm overflow-hidden animate-in fade-in zoom-in duration-200">
            <div className="px-6 py-4 border-b border-slate-100 flex justify-between items-center bg-slate-50">
              <h3 className="font-bold text-slate-800">
                {editingPos ? '修改持仓' : '新增持仓'}
              </h3>
              <button 
                onClick={() => setModalOpen(false)}
                className="text-slate-400 hover:text-slate-600"
              >
                <X className="w-5 h-5" />
              </button>
            </div>
            
            <form onSubmit={handleSubmit} className="p-6 space-y-4">
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">基金代码</label>
                <input 
                  type="text" 
                  value={formData.code}
                  onChange={(e) => setFormData({...formData, code: e.target.value})}
                  disabled={!!editingPos}
                  placeholder="如: 005827"
                  className="w-full px-3 py-2 bg-slate-50 border border-slate-200 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none font-mono disabled:opacity-60"
                  required
                />
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">持有份额</label>
                  <input 
                    type="number" 
                    step="0.01"
                    value={formData.shares}
                    onChange={(e) => setFormData({...formData, shares: e.target.value})}
                    className="w-full px-3 py-2 bg-white border border-slate-200 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none font-mono"
                    required
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">持仓成本(单价)</label>
                  <input 
                    type="number" 
                    step="0.0001"
                    value={formData.cost}
                    onChange={(e) => setFormData({...formData, cost: e.target.value})}
                    className="w-full px-3 py-2 bg-white border border-slate-200 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none font-mono"
                    required
                  />
                </div>
              </div>

              <div className="pt-2">
                <button
                  type="submit"
                  disabled={submitting}
                  className="w-full bg-blue-600 hover:bg-blue-700 text-white font-medium py-2.5 rounded-lg transition-colors shadow-sm active:scale-[0.98] disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {submitting ? '保存中...' : '保存'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};

const SummaryCard = ({ label, value, subValue, isPositive }) => (
  <div className="bg-white p-5 rounded-xl shadow-sm border border-slate-200">
    <div className="text-xs text-slate-500 mb-1">{label}</div>
    <div className={`text-2xl font-mono font-bold ${
      isPositive === undefined ? 'text-slate-900' : 
      isPositive ? 'text-red-500' : 'text-green-500'
    }`}>
      {value}
    </div>
    {subValue && (
      <div className={`text-xs mt-1 font-medium ${
        isPositive ? 'text-red-500' : 'text-green-500'
      }`}>
        {isPositive ? '+' : ''}{subValue}
      </div>
    )}
  </div>
);

export default Account;

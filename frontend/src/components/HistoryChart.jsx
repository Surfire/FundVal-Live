import React, { useEffect, useState } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { getFundHistory } from '../services/api';

const RANGES = [
  { label: '近1周', val: 5 },
  { label: '近1月', val: 22 },
  { label: '近3月', val: 66 },
  { label: '近半年', val: 130 },
  { label: '近1年', val: 250 },
  { label: '成立来', val: 9999 },
];

export const HistoryChart = ({ fundId, accountId = null }) => {
  const [data, setData] = useState([]);
  const [transactions, setTransactions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [range, setRange] = useState(22); // Default 1M

  useEffect(() => {
    if (!fundId) return;

    const fetchHistory = async () => {
      setLoading(true);
      try {
        const result = await getFundHistory(fundId, range, accountId);
        // Handle both old format (array) and new format (object)
        if (Array.isArray(result)) {
          setData(result);
          setTransactions([]);
        } else {
          setData(result.history || []);
          setTransactions(result.transactions || []);
        }
      } catch (e) {
        console.error("Failed to load history", e);
      } finally {
        setLoading(false);
      }
    };

    fetchHistory();
  }, [fundId, range, accountId]);

  if (loading) return <div className="h-64 flex items-center justify-center text-slate-400">加载走势中...</div>;
  if (!data || data.length === 0) return <div className="h-64 flex items-center justify-center text-slate-400">暂无历史数据</div>;

  // Ensure we have valid data before rendering chart
  const validData = data.filter(d => d && d.date && d.nav !== null && d.nav !== undefined);
  if (validData.length === 0) return <div className="h-64 flex items-center justify-center text-slate-400">暂无有效数据</div>;

  // Custom dot component for transaction markers
  const TransactionDot = (props) => {
    const { cx, cy, payload } = props;
    const transaction = transactions.find(t => t.date === payload.date);

    if (!transaction) return null;

    const isBuy = transaction.type === 'buy';
    const color = isBuy ? '#ef4444' : '#10b981'; // red for buy, green for sell
    const label = isBuy ? 'B' : 'S';

    return (
      <g>
        <circle cx={cx} cy={cy} r={12} fill={color} stroke="white" strokeWidth={2} />
        <text
          x={cx}
          y={cy}
          textAnchor="middle"
          dominantBaseline="middle"
          fill="white"
          fontSize={10}
          fontWeight="bold"
        >
          {label}
        </text>
      </g>
    );
  };

  // Custom tooltip to show transaction info
  const CustomTooltip = ({ active, payload }) => {
    if (!active || !payload || !payload.length) return null;

    const point = payload[0].payload;
    const transaction = transactions.find(t => t.date === point.date);

    return (
      <div className="bg-white p-3 rounded-lg shadow-lg border border-slate-200">
        <p className="text-xs text-slate-500 mb-1">{point.date}</p>
        <p className="text-sm font-bold text-slate-800">净值: {point.nav?.toFixed(4)}</p>
        {transaction && (
          <div className={`mt-2 pt-2 border-t ${transaction.type === 'buy' ? 'border-red-200' : 'border-green-200'}`}>
            <p className={`text-xs font-bold ${transaction.type === 'buy' ? 'text-red-600' : 'text-green-600'}`}>
              {transaction.type === 'buy' ? '买入' : '卖出'}
            </p>
            {transaction.amount && (
              <p className="text-xs text-slate-600">金额: ¥{transaction.amount.toFixed(2)}</p>
            )}
            {transaction.shares && (
              <p className="text-xs text-slate-600">份额: {transaction.shares.toFixed(2)}</p>
            )}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="w-full">
      <div className="flex gap-2 mb-4 overflow-x-auto pb-2">
        {RANGES.map((r) => (
          <button
            key={r.label}
            onClick={() => setRange(r.val)}
            className={`px-3 py-1 text-xs rounded-full whitespace-nowrap transition-colors ${
              range === r.val
                ? 'bg-blue-600 text-white font-medium'
                : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
            }`}
          >
            {r.label}
          </button>
        ))}
      </div>

      {transactions.length > 0 && (
        <div className="mb-2 flex items-center gap-4 text-xs text-slate-500">
          <div className="flex items-center gap-1">
            <div className="w-4 h-4 rounded-full bg-red-500 flex items-center justify-center text-white font-bold text-[8px]">B</div>
            <span>买入</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-4 h-4 rounded-full bg-green-500 flex items-center justify-center text-white font-bold text-[8px]">S</div>
            <span>卖出</span>
          </div>
        </div>
      )}

      <div className="h-64 w-full min-h-[256px] min-w-0">
        <ResponsiveContainer width="100%" height="100%" minHeight={256}>
          <AreaChart
            data={validData}
            margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
          >
            <defs>
              <linearGradient id="colorNav" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.2}/>
                <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
            <XAxis
              dataKey="date"
              tick={{fontSize: 10, fill: '#94a3b8'}}
              tickLine={false}
              axisLine={false}
              tickFormatter={(str) => str.slice(5)} // Show MM-DD
              minTickGap={30}
            />
            <YAxis
              domain={['auto', 'auto']}
              tick={{fontSize: 10, fill: '#94a3b8'}}
              tickLine={false}
              axisLine={false}
              width={40}
            />
            <Tooltip content={<CustomTooltip />} />
            <Area
              type="monotone"
              dataKey="nav"
              stroke="#3b82f6"
              strokeWidth={2}
              fillOpacity={1}
              fill="url(#colorNav)"
              animationDuration={500}
              dot={<TransactionDot />}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

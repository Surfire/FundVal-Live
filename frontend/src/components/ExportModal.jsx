import React, { useState } from 'react';
import { X, Download, CheckSquare, Square } from 'lucide-react';

export const ExportModal = ({ isOpen, onClose, onExport }) => {
  const [selectedModules, setSelectedModules] = useState([
    'accounts',
    'positions',
    'transactions',
    'ai_prompts',
    'subscriptions',
    'settings',
    'strategy'
  ]);
  const [exporting, setExporting] = useState(false);

  const modules = [
    { key: 'accounts', label: '账户', description: '账户列表和描述' },
    { key: 'positions', label: '持仓', description: '持仓数据（成本、份额）' },
    { key: 'transactions', label: '交易记录', description: '加仓减仓历史记录' },
    { key: 'ai_prompts', label: 'AI 提示词', description: '自定义提示词模板' },
    { key: 'subscriptions', label: '订阅设置', description: '邮件订阅配置' },
    { key: 'settings', label: '系统设置', description: '配置项（敏感信息已脱敏）' },
    { key: 'strategy', label: '策略组合', description: '策略、版本、调仓批次、回测缓存' }
  ];

  const toggleModule = (moduleKey) => {
    setSelectedModules(prev =>
      prev.includes(moduleKey)
        ? prev.filter(m => m !== moduleKey)
        : [...prev, moduleKey]
    );
  };

  const toggleAll = () => {
    if (selectedModules.length === modules.length) {
      setSelectedModules([]);
    } else {
      setSelectedModules(modules.map(m => m.key));
    }
  };

  const handleExport = async () => {
    if (selectedModules.length === 0) {
      alert('请至少选择一个模块');
      return;
    }

    setExporting(true);
    try {
      await onExport(selectedModules);
      onClose();
    } catch (error) {
      alert('导出失败：' + (error.response?.data?.detail || error.message));
    } finally {
      setExporting(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-2xl shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-slate-200">
          <h2 className="text-2xl font-bold text-slate-800">导出数据</h2>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-600 transition-colors"
          >
            <X className="w-6 h-6" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Select All */}
          <div className="flex items-center justify-between pb-4 border-b border-slate-200">
            <span className="text-sm font-medium text-slate-700">
              已选择 {selectedModules.length} / {modules.length} 个模块
            </span>
            <button
              onClick={toggleAll}
              className="text-sm text-blue-600 hover:text-blue-700 font-medium"
            >
              {selectedModules.length === modules.length ? '取消全选' : '全选'}
            </button>
          </div>

          {/* Module List */}
          <div className="space-y-3">
            {modules.map(module => (
              <div
                key={module.key}
                onClick={() => toggleModule(module.key)}
                className={`flex items-start gap-3 p-4 rounded-lg border-2 cursor-pointer transition-all ${
                  selectedModules.includes(module.key)
                    ? 'border-blue-500 bg-blue-50'
                    : 'border-slate-200 hover:border-slate-300'
                }`}
              >
                <div className="mt-0.5">
                  {selectedModules.includes(module.key) ? (
                    <CheckSquare className="w-5 h-5 text-blue-600" />
                  ) : (
                    <Square className="w-5 h-5 text-slate-400" />
                  )}
                </div>
                <div className="flex-1">
                  <h3 className="font-semibold text-slate-800">{module.label}</h3>
                  <p className="text-sm text-slate-500 mt-1">{module.description}</p>
                </div>
              </div>
            ))}
          </div>

          {/* Info */}
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
            <p className="text-sm text-blue-800">
              <strong>提示：</strong>导出的 JSON 文件包含所选模块的所有数据。敏感信息（如 API Key、密码）将被脱敏处理。
            </p>
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-6 border-t border-slate-200 bg-slate-50">
          <button
            onClick={onClose}
            className="px-6 py-2 text-slate-600 hover:text-slate-800 font-medium transition-colors"
            disabled={exporting}
          >
            取消
          </button>
          <button
            onClick={handleExport}
            disabled={exporting || selectedModules.length === 0}
            className="flex items-center gap-2 px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed font-medium transition-colors"
          >
            <Download className="w-4 h-4" />
            {exporting ? '导出中...' : '导出'}
          </button>
        </div>
      </div>
    </div>
  );
};

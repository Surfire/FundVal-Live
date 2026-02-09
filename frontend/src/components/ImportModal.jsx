import React, { useState } from 'react';
import { X, Upload, AlertTriangle, CheckSquare, Square, FileJson } from 'lucide-react';

export const ImportModal = ({ isOpen, onClose, onImport }) => {
  const [file, setFile] = useState(null);
  const [fileData, setFileData] = useState(null);
  const [selectedModules, setSelectedModules] = useState([]);
  const [mode, setMode] = useState('merge');
  const [confirmText, setConfirmText] = useState('');
  const [importing, setImporting] = useState(false);
  const [error, setError] = useState(null);
  const [result, setResult] = useState(null);

  const modules = [
    { key: 'accounts', label: '账户' },
    { key: 'positions', label: '持仓' },
    { key: 'transactions', label: '交易记录' },
    { key: 'ai_prompts', label: 'AI 提示词' },
    { key: 'subscriptions', label: '订阅设置' },
    { key: 'settings', label: '系统设置' },
    { key: 'strategy', label: '策略组合' }
  ];

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    if (!selectedFile) return;

    // Check file size (10MB limit)
    if (selectedFile.size > 10 * 1024 * 1024) {
      setError('文件大小超过 10MB 限制');
      return;
    }

    // Check file type
    if (!selectedFile.name.endsWith('.json')) {
      setError('只支持 JSON 文件');
      return;
    }

    setFile(selectedFile);
    setError(null);

    // Read and parse file
    const reader = new FileReader();
    reader.onload = (event) => {
      try {
        const data = JSON.parse(event.target.result);

        // Validate structure
        if (!data.version || !data.modules) {
          setError('JSON 格式错误：缺少 version 或 modules 字段');
          setFileData(null);
          return;
        }

        setFileData(data);

        // Auto-select available modules
        const availableModules = Object.keys(data.modules).filter(m =>
          modules.some(mod => mod.key === m) && data.modules[m] && (
            Array.isArray(data.modules[m]) ? data.modules[m].length > 0 : Object.keys(data.modules[m]).length > 0
          )
        );
        setSelectedModules(availableModules);

      } catch (err) {
        setError('JSON 解析失败：' + err.message);
        setFileData(null);
      }
    };

    reader.readAsText(selectedFile);
  };

  const toggleModule = (moduleKey) => {
    setSelectedModules(prev =>
      prev.includes(moduleKey)
        ? prev.filter(m => m !== moduleKey)
        : [...prev, moduleKey]
    );
  };

  const handleImport = async () => {
    if (!fileData) {
      setError('请先上传文件');
      return;
    }

    if (selectedModules.length === 0) {
      setError('请至少选择一个模块');
      return;
    }

    if (mode === 'replace' && confirmText !== '确认删除') {
      setError('请输入「确认删除」以继续');
      return;
    }

    setImporting(true);
    setError(null);

    try {
      const response = await onImport(fileData, selectedModules, mode);
      setResult(response);
    } catch (err) {
      setError('导入失败：' + (err.response?.data?.detail || err.message));
    } finally {
      setImporting(false);
    }
  };

  const handleClose = () => {
    setFile(null);
    setFileData(null);
    setSelectedModules([]);
    setMode('merge');
    setConfirmText('');
    setError(null);
    setResult(null);
    onClose();
  };

  if (!isOpen) return null;

  // Show result screen
  if (result) {
    return (
      <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
        <div className="bg-white rounded-2xl shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-hidden flex flex-col">
          <div className="flex items-center justify-between p-6 border-b border-slate-200">
            <h2 className="text-2xl font-bold text-slate-800">导入结果</h2>
            <button onClick={handleClose} className="text-slate-400 hover:text-slate-600">
              <X className="w-6 h-6" />
            </button>
          </div>

          <div className="flex-1 overflow-y-auto p-6 space-y-4">
            {/* Summary */}
            <div className={`p-4 rounded-lg border-2 ${
              result.success ? 'bg-green-50 border-green-200' : 'bg-red-50 border-red-200'
            }`}>
              <h3 className="font-semibold text-lg mb-2">
                {result.success ? '✅ 导入成功' : '❌ 导入失败'}
              </h3>
              <div className="grid grid-cols-2 gap-2 text-sm">
                <div>总记录数：{result.total_records}</div>
                <div>已导入：{result.imported}</div>
                <div>已跳过：{result.skipped}</div>
                <div>失败：{result.failed}</div>
                {result.deleted > 0 && <div>已删除：{result.deleted}</div>}
              </div>
            </div>

            {/* Details */}
            {result.details && Object.keys(result.details).length > 0 && (
              <div className="space-y-3">
                <h4 className="font-semibold text-slate-800">详细信息</h4>
                {Object.entries(result.details).map(([module, detail]) => (
                  <div key={module} className="border border-slate-200 rounded-lg p-4">
                    <h5 className="font-medium text-slate-700 mb-2">
                      {modules.find(m => m.key === module)?.label || module}
                    </h5>
                    <div className="text-sm text-slate-600 space-y-1">
                      <div>导入：{detail.imported} / 跳过：{detail.skipped} / 失败：{detail.failed}</div>
                      {detail.deleted > 0 && <div>删除：{detail.deleted}</div>}
                      {detail.errors && detail.errors.length > 0 && (
                        <div className="mt-2">
                          <div className="font-medium text-red-600">错误：</div>
                          <ul className="list-disc list-inside text-red-600">
                            {detail.errors.slice(0, 5).map((err, i) => (
                              <li key={i}>{err}</li>
                            ))}
                            {detail.errors.length > 5 && (
                              <li>...还有 {detail.errors.length - 5} 个错误</li>
                            )}
                          </ul>
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="p-6 border-t border-slate-200 bg-slate-50">
            <button
              onClick={handleClose}
              className="w-full px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-medium"
            >
              完成
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-2xl shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-slate-200">
          <h2 className="text-2xl font-bold text-slate-800">导入数据</h2>
          <button onClick={handleClose} className="text-slate-400 hover:text-slate-600">
            <X className="w-6 h-6" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* File Upload */}
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-2">
              选择文件
            </label>
            <div className="border-2 border-dashed border-slate-300 rounded-lg p-6 text-center hover:border-blue-400 transition-colors">
              <input
                type="file"
                accept=".json"
                onChange={handleFileChange}
                className="hidden"
                id="file-upload"
              />
              <label htmlFor="file-upload" className="cursor-pointer">
                {file ? (
                  <div className="flex items-center justify-center gap-2 text-blue-600">
                    <FileJson className="w-6 h-6" />
                    <span className="font-medium">{file.name}</span>
                  </div>
                ) : (
                  <div>
                    <Upload className="w-12 h-12 text-slate-400 mx-auto mb-2" />
                    <p className="text-slate-600">点击选择 JSON 文件</p>
                    <p className="text-xs text-slate-400 mt-1">最大 10MB</p>
                  </div>
                )}
              </label>
            </div>
          </div>

          {/* File Info */}
          {fileData && (
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
              <h4 className="font-semibold text-blue-900 mb-2">文件信息</h4>
              <div className="text-sm text-blue-800 space-y-1">
                <div>版本：{fileData.version}</div>
                <div>导出时间：{new Date(fileData.exported_at).toLocaleString('zh-CN')}</div>
                {fileData.metadata && (
                  <div className="mt-2">
                    {Object.entries(fileData.metadata).map(([key, value]) => (
                      <div key={key}>{key}: {value}</div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Module Selection */}
          {fileData && (
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">
                选择要导入的模块
              </label>
              <div className="space-y-2">
                {modules.filter(m => fileData.modules[m.key]).map(module => {
                  const moduleData = fileData.modules[module.key];
                  const count = Array.isArray(moduleData) ? moduleData.length : Object.keys(moduleData).length;

                  return (
                    <div
                      key={module.key}
                      onClick={() => toggleModule(module.key)}
                      className={`flex items-center gap-3 p-3 rounded-lg border cursor-pointer transition-all ${
                        selectedModules.includes(module.key)
                          ? 'border-blue-500 bg-blue-50'
                          : 'border-slate-200 hover:border-slate-300'
                      }`}
                    >
                      {selectedModules.includes(module.key) ? (
                        <CheckSquare className="w-5 h-5 text-blue-600" />
                      ) : (
                        <Square className="w-5 h-5 text-slate-400" />
                      )}
                      <span className="flex-1 font-medium text-slate-800">{module.label}</span>
                      <span className="text-sm text-slate-500">{count} 条</span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Import Mode */}
          {fileData && selectedModules.length > 0 && (
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-2">
                导入模式
              </label>
              <div className="space-y-2">
                <label className={`flex items-start gap-3 p-4 rounded-lg border-2 cursor-pointer ${
                  mode === 'merge' ? 'border-blue-500 bg-blue-50' : 'border-slate-200'
                }`}>
                  <input
                    type="radio"
                    name="mode"
                    value="merge"
                    checked={mode === 'merge'}
                    onChange={(e) => setMode(e.target.value)}
                    className="mt-1"
                  />
                  <div>
                    <div className="font-semibold text-slate-800">合并模式</div>
                    <div className="text-sm text-slate-600 mt-1">
                      保留现有数据，添加新数据。主键冲突时跳过。
                    </div>
                  </div>
                </label>

                <label className={`flex items-start gap-3 p-4 rounded-lg border-2 cursor-pointer ${
                  mode === 'replace' ? 'border-red-500 bg-red-50' : 'border-slate-200'
                }`}>
                  <input
                    type="radio"
                    name="mode"
                    value="replace"
                    checked={mode === 'replace'}
                    onChange={(e) => setMode(e.target.value)}
                    className="mt-1"
                  />
                  <div className="flex-1">
                    <div className="font-semibold text-slate-800 flex items-center gap-2">
                      替换模式
                      <AlertTriangle className="w-4 h-4 text-red-600" />
                    </div>
                    <div className="text-sm text-slate-600 mt-1">
                      <strong className="text-red-600">危险操作！</strong>
                      清空选中模块的现有数据，然后导入新数据。
                    </div>
                  </div>
                </label>
              </div>
            </div>
          )}

          {/* Confirmation for Replace Mode */}
          {mode === 'replace' && selectedModules.length > 0 && (
            <div className="bg-red-50 border-2 border-red-200 rounded-lg p-4">
              <div className="flex items-start gap-2 mb-3">
                <AlertTriangle className="w-5 h-5 text-red-600 mt-0.5" />
                <div>
                  <h4 className="font-semibold text-red-900">确认删除</h4>
                  <p className="text-sm text-red-800 mt-1">
                    替换模式将删除以下模块的所有现有数据：
                    <strong> {selectedModules.map(m => modules.find(mod => mod.key === m)?.label).join('、')}</strong>
                  </p>
                </div>
              </div>
              <input
                type="text"
                value={confirmText}
                onChange={(e) => setConfirmText(e.target.value)}
                placeholder="请输入「确认删除」以继续"
                className="w-full px-4 py-2 border-2 border-red-300 rounded-lg focus:ring-2 focus:ring-red-500 focus:border-transparent"
              />
            </div>
          )}

          {/* Error */}
          {error && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4">
              <p className="text-sm text-red-800 flex items-center gap-2">
                <AlertTriangle className="w-5 h-5" />
                {error}
              </p>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-6 border-t border-slate-200 bg-slate-50">
          <button
            onClick={handleClose}
            className="px-6 py-2 text-slate-600 hover:text-slate-800 font-medium"
            disabled={importing}
          >
            取消
          </button>
          <button
            onClick={handleImport}
            disabled={importing || !fileData || selectedModules.length === 0 || (mode === 'replace' && confirmText !== '确认删除')}
            className="flex items-center gap-2 px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed font-medium"
          >
            <Upload className="w-4 h-4" />
            {importing ? '导入中...' : '导入'}
          </button>
        </div>
      </div>
    </div>
  );
};

import React, { useState, useEffect } from 'react';
import { Save, AlertCircle, CheckCircle2, Plus, Edit2, Trash2, Star, Download, Upload } from 'lucide-react';
import { getPrompts, createPrompt, updatePrompt, deletePrompt, exportData, importData } from '../services/api';
import { PromptModal } from '../components/PromptModal';
import { ExportModal } from '../components/ExportModal';
import { ImportModal } from '../components/ImportModal';

export default function Settings() {
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState({ type: '', text: '' });

  const [settings, setSettings] = useState({
    OPENAI_API_KEY: '',
    OPENAI_API_BASE: '',
    AI_MODEL_NAME: '',
    OCR_MODEL_NAME: 'Qwen/Qwen3-VL-32B-Instruct',
    TUSHARE_PRO_TOKEN: '',
    SMTP_HOST: '',
    SMTP_PORT: '',
    SMTP_USER: '',
    SMTP_PASSWORD: '',
    EMAIL_FROM: '',
    INTRADAY_COLLECT_INTERVAL: '5'
  });

  const [errors, setErrors] = useState({});

  // AI Prompts state
  const [prompts, setPrompts] = useState([]);
  const [promptsLoading, setPromptsLoading] = useState(false);
  const [promptModalOpen, setPromptModalOpen] = useState(false);
  const [editingPrompt, setEditingPrompt] = useState(null);

  // Import/Export state
  const [exportModalOpen, setExportModalOpen] = useState(false);
  const [importModalOpen, setImportModalOpen] = useState(false);

  useEffect(() => {
    loadSettings();
    loadPrompts();
  }, []);

  const loadSettings = async () => {
    setLoading(true);
    try {
      const response = await fetch('/api/settings');
      if (!response.ok) throw new Error('Failed to load settings');
      const data = await response.json();

      setSettings((prev) => ({ ...prev, ...(data.settings || {}) }));
    } catch (error) {
      setMessage({ type: 'error', text: error.message });
    } finally {
      setLoading(false);
    }
  };

  const validateForm = () => {
    const newErrors = {};

    // Email validation
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (settings.SMTP_USER && !emailRegex.test(settings.SMTP_USER)) {
      newErrors.SMTP_USER = '邮箱格式不正确';
    }
    if (settings.EMAIL_FROM && !emailRegex.test(settings.EMAIL_FROM)) {
      newErrors.EMAIL_FROM = '邮箱格式不正确';
    }

    // Port validation
    const port = parseInt(settings.SMTP_PORT);
    if (settings.SMTP_PORT && (isNaN(port) || port < 1 || port > 65535)) {
      newErrors.SMTP_PORT = '端口必须在 1-65535 之间';
    }

    // Interval validation
    const interval = parseInt(settings.INTRADAY_COLLECT_INTERVAL);
    if (settings.INTRADAY_COLLECT_INTERVAL && (isNaN(interval) || interval < 1 || interval > 60)) {
      newErrors.INTRADAY_COLLECT_INTERVAL = '采集间隔必须在 1-60 分钟之间';
    }

    // URL validation
    if (settings.OPENAI_API_BASE) {
      try {
        new URL(settings.OPENAI_API_BASE);
      } catch {
        newErrors.OPENAI_API_BASE = 'URL 格式不正确';
      }
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSave = async () => {
    if (!validateForm()) {
      setMessage({ type: 'error', text: '请修正表单错误' });
      return;
    }

    setSaving(true);
    setMessage({ type: '', text: '' });

    try {
      // 过滤掉掩码字段
      const filteredSettings = Object.fromEntries(
        Object.entries(settings).filter(([key, value]) => value !== '***')
      );

      const response = await fetch('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ settings: filteredSettings })
      });

      if (!response.ok) {
        const errorData = await response.json();

        // 如果后端返回字段级错误
        if (errorData.detail && errorData.detail.errors) {
          setErrors(errorData.detail.errors);
          setMessage({ type: 'error', text: '请修正表单错误' });
        } else {
          setMessage({ type: 'error', text: '保存失败' });
        }
        return;
      }

      setMessage({ type: 'success', text: '设置已保存' });
    } catch (error) {
      setMessage({ type: 'error', text: '网络错误' });
    } finally {
      setSaving(false);
    }
  };

  const handleChange = (field, value) => {
    setSettings(prev => ({ ...prev, [field]: value }));
    if (errors[field]) {
      setErrors(prev => ({ ...prev, [field]: undefined }));
    }
  };

  // AI Prompts functions
  const loadPrompts = async () => {
    setPromptsLoading(true);
    try {
      const data = await getPrompts();
      setPrompts(data);
    } catch (error) {
      console.error('Load prompts failed', error);
    } finally {
      setPromptsLoading(false);
    }
  };

  const handleCreatePrompt = () => {
    setEditingPrompt(null);
    setPromptModalOpen(true);
  };

  const handleEditPrompt = (prompt) => {
    setEditingPrompt(prompt);
    setPromptModalOpen(true);
  };

  const handleSavePrompt = async (data) => {
    try {
      if (editingPrompt) {
        await updatePrompt(editingPrompt.id, data);
        setMessage({ type: 'success', text: '模板已更新' });
      } else {
        await createPrompt(data);
        setMessage({ type: 'success', text: '模板已创建' });
      }
      await loadPrompts();
    } catch (error) {
      throw error;
    }
  };

  const handleDeletePrompt = async (id) => {
    if (!confirm('确定要删除这个提示词模板吗？')) return;

    try {
      await deletePrompt(id);
      setMessage({ type: 'success', text: '模板已删除' });
      await loadPrompts();
    } catch (error) {
      const errorMsg = error.response?.data?.detail || '删除失败';
      setMessage({ type: 'error', text: errorMsg });
    }
  };

  const handleSetDefault = async (prompt) => {
    try {
      // Pass complete prompt data to satisfy backend validation
      await updatePrompt(prompt.id, {
        name: prompt.name,
        system_prompt: prompt.system_prompt,
        user_prompt: prompt.user_prompt,
        is_default: true
      });
      setMessage({ type: 'success', text: '已设为默认模板' });
      await loadPrompts();
    } catch (error) {
      setMessage({ type: 'error', text: '设置失败' });
    }
  };

  const handleImportSuccess = () => {
    setMessage({ type: 'success', text: '数据导入成功' });
    // 重新加载数据
    loadSettings();
    loadPrompts();
  };

  const handleExport = async (modules) => {
    try {
      await exportData(modules);
    } catch (error) {
      throw error;
    }
  };

  const handleImport = async (data, modules, mode) => {
    try {
      const response = await importData(data, modules, mode);
      handleImportSuccess();
      return response;
    } catch (error) {
      throw error;
    }
  };


  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-gray-500">加载设置中...</div>
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">设置</h1>
        <button
          onClick={handleSave}
          disabled={saving}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          <Save className="w-4 h-4" />
          {saving ? '保存中...' : '保存更改'}
        </button>
      </div>

      {message.text && (
        <div className={`flex items-center gap-2 p-4 rounded-lg ${
          message.type === 'success' ? 'bg-green-50 text-green-800' : 'bg-red-50 text-red-800'
        }`}>
          {message.type === 'success' ? (
            <CheckCircle2 className="w-5 h-5" />
          ) : (
            <AlertCircle className="w-5 h-5" />
          )}
          <span>{message.text}</span>
        </div>
      )}

      {/* AI Configuration */}
      <div className="bg-white rounded-lg shadow p-6 space-y-4">
        <h2 className="text-xl font-semibold text-gray-900">AI 配置</h2>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            OpenAI API Key
          </label>
          <input
            type="password"
            value={settings.OPENAI_API_KEY}
            onChange={(e) => handleChange('OPENAI_API_KEY', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            placeholder="sk-..."
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            OpenAI API Base URL
          </label>
          <input
            type="text"
            value={settings.OPENAI_API_BASE}
            onChange={(e) => handleChange('OPENAI_API_BASE', e.target.value)}
            className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
              errors.OPENAI_API_BASE ? 'border-red-500' : 'border-gray-300'
            }`}
            placeholder="https://api.openai.com/v1"
          />
          {errors.OPENAI_API_BASE && (
            <p className="mt-1 text-sm text-red-600">{errors.OPENAI_API_BASE}</p>
          )}
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            AI Model Name
          </label>
          <input
            type="text"
            value={settings.AI_MODEL_NAME}
            onChange={(e) => handleChange('AI_MODEL_NAME', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            placeholder="gpt-4"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            OCR Model Name
          </label>
          <input
            type="text"
            value={settings.OCR_MODEL_NAME}
            onChange={(e) => handleChange('OCR_MODEL_NAME', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            placeholder="Qwen/Qwen3-VL-32B-Instruct"
          />
          <p className="mt-1 text-xs text-gray-500">用于策略新建里的图片持仓识别（OCR/VLM）。</p>
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Tushare Pro Token（港股补源可选）
          </label>
          <input
            type="password"
            value={settings.TUSHARE_PRO_TOKEN}
            onChange={(e) => handleChange('TUSHARE_PRO_TOKEN', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            placeholder="填写后可用于港股日线/收盘价补源"
          />
        </div>
      </div>

      {/* Email Configuration */}
      <div className="bg-white rounded-lg shadow p-6 space-y-4">
        <h2 className="text-xl font-semibold text-gray-900">邮件配置</h2>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              SMTP Host
            </label>
            <input
              type="text"
              value={settings.SMTP_HOST}
              onChange={(e) => handleChange('SMTP_HOST', e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder="smtp.gmail.com"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              SMTP Port
            </label>
            <input
              type="number"
              value={settings.SMTP_PORT}
              onChange={(e) => handleChange('SMTP_PORT', e.target.value)}
              className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
                errors.SMTP_PORT ? 'border-red-500' : 'border-gray-300'
              }`}
              placeholder="587"
            />
            {errors.SMTP_PORT && (
              <p className="mt-1 text-sm text-red-600">{errors.SMTP_PORT}</p>
            )}
          </div>
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            SMTP User (Email)
          </label>
          <input
            type="email"
            value={settings.SMTP_USER}
            onChange={(e) => handleChange('SMTP_USER', e.target.value)}
            className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
              errors.SMTP_USER ? 'border-red-500' : 'border-gray-300'
            }`}
            placeholder="user@example.com"
          />
          {errors.SMTP_USER && (
            <p className="mt-1 text-sm text-red-600">{errors.SMTP_USER}</p>
          )}
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            SMTP Password
          </label>
          <input
            type="password"
            value={settings.SMTP_PASSWORD}
            onChange={(e) => handleChange('SMTP_PASSWORD', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            placeholder="••••••••"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            From Email Address
          </label>
          <input
            type="email"
            value={settings.EMAIL_FROM}
            onChange={(e) => handleChange('EMAIL_FROM', e.target.value)}
            className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
              errors.EMAIL_FROM ? 'border-red-500' : 'border-gray-300'
            }`}
            placeholder="noreply@example.com"
          />
          {errors.EMAIL_FROM && (
            <p className="mt-1 text-sm text-red-600">{errors.EMAIL_FROM}</p>
          )}
        </div>
      </div>

      {/* Data Collection Configuration */}
      <div className="bg-white rounded-lg shadow p-6 space-y-4">
        <h2 className="text-xl font-semibold text-gray-900">数据采集配置</h2>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            分时数据采集间隔（分钟）
          </label>
          <input
            type="number"
            value={settings.INTRADAY_COLLECT_INTERVAL}
            onChange={(e) => handleChange('INTRADAY_COLLECT_INTERVAL', e.target.value)}
            className={`w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent ${
              errors.INTRADAY_COLLECT_INTERVAL ? 'border-red-500' : 'border-gray-300'
            }`}
            placeholder="5"
            min="1"
            max="60"
          />
          {errors.INTRADAY_COLLECT_INTERVAL && (
            <p className="mt-1 text-sm text-red-600">{errors.INTRADAY_COLLECT_INTERVAL}</p>
          )}
          <p className="mt-2 text-sm text-gray-500">
             请注意：分时数据采集仅在系统开启时运行（交易日 09:35-15:05）
          </p>
        </div>
      </div>

      {/* AI Prompts Management */}
      <div className="bg-white rounded-lg shadow p-6 space-y-4">
        <div className="flex items-center justify-between">
          <h2 className="text-xl font-semibold text-gray-900">AI 提示词管理</h2>
          <button
            onClick={handleCreatePrompt}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            <Plus className="w-4 h-4" />
            新建模板
          </button>
        </div>

        {promptsLoading ? (
          <div className="text-center py-8 text-gray-500">加载中...</div>
        ) : prompts.length === 0 ? (
          <div className="text-center py-8 text-gray-500">暂无提示词模板</div>
        ) : (
          <div className="grid gap-4">
            {prompts.map(prompt => (
              <div
                key={prompt.id}
                className="border border-gray-200 rounded-lg p-4 hover:border-blue-300 transition-colors"
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center gap-2 mb-2">
                      <h3 className="font-semibold text-gray-900">{prompt.name}</h3>
                      {prompt.is_default && (
                        <span className="px-2 py-0.5 bg-blue-100 text-blue-700 text-xs rounded-full flex items-center gap-1">
                          <Star className="w-3 h-3 fill-current" />
                          默认
                        </span>
                      )}
                    </div>
                    <p className="text-sm text-gray-500">
                      创建于 {new Date(prompt.created_at).toLocaleString('zh-CN')}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    {!prompt.is_default && (
                      <button
                        onClick={() => handleSetDefault(prompt)}
                        className="p-2 text-gray-400 hover:text-blue-600 transition-colors"
                        title="设为默认"
                      >
                        <Star className="w-4 h-4" />
                      </button>
                    )}
                    <button
                      onClick={() => handleEditPrompt(prompt)}
                      className="p-2 text-gray-400 hover:text-blue-600 transition-colors"
                      title="编辑"
                    >
                      <Edit2 className="w-4 h-4" />
                    </button>
                    <button
                      onClick={() => handleDeletePrompt(prompt.id)}
                      className="p-2 text-gray-400 hover:text-red-600 transition-colors"
                      title="删除"
                      disabled={prompt.is_default}
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Data Import/Export */}
      <div className="bg-white rounded-lg shadow p-6 space-y-4">
        <h2 className="text-xl font-semibold text-gray-900">数据导入导出</h2>

        <div className="grid grid-cols-2 gap-4">
          <button
            onClick={() => setExportModalOpen(true)}
            className="flex items-center justify-center gap-2 px-6 py-4 border-2 border-blue-200 text-blue-700 rounded-lg hover:bg-blue-50 transition-colors"
          >
            <Download className="w-5 h-5" />
            <span className="font-medium">导出数据</span>
          </button>

          <button
            onClick={() => setImportModalOpen(true)}
            className="flex items-center justify-center gap-2 px-6 py-4 border-2 border-green-200 text-green-700 rounded-lg hover:bg-green-50 transition-colors"
          >
            <Upload className="w-5 h-5" />
            <span className="font-medium">导入数据</span>
          </button>
        </div>

        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
          <p className="text-sm text-yellow-800">
            <strong>提示：</strong>
          </p>
          <ul className="text-sm text-yellow-700 mt-2 space-y-1 list-disc list-inside">
            <li>导出时，敏感信息（API Key、密码）将被掩码处理</li>
            <li>导入时，可选择合并模式（保留现有数据）或替换模式（删除现有数据）</li>
            <li>替换模式需要二次确认，请谨慎操作</li>
          </ul>
        </div>
      </div>

      {/* Prompt Modal */}
      <PromptModal
        isOpen={promptModalOpen}
        onClose={() => setPromptModalOpen(false)}
        onSave={handleSavePrompt}
        prompt={editingPrompt}
      />

      {/* Export Modal */}
      <ExportModal
        isOpen={exportModalOpen}
        onClose={() => setExportModalOpen(false)}
        onExport={handleExport}
      />

      {/* Import Modal */}
      <ImportModal
        isOpen={importModalOpen}
        onClose={() => setImportModalOpen(false)}
        onImport={handleImport}
      />
    </div>
  );
}

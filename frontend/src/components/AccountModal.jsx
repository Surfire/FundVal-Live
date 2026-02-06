import React, { useState } from 'react';
import { X, Plus, Trash2, Edit2 } from 'lucide-react';

export const AccountModal = ({ accounts, currentAccount, onClose, onRefresh, onSwitch }) => {
  const [newName, setNewName] = useState('');
  const [newDescription, setNewDescription] = useState('');
  const [editingId, setEditingId] = useState(null);
  const [editName, setEditName] = useState('');
  const [editDescription, setEditDescription] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleCreate = async () => {
    if (!newName.trim()) {
      setError('账户名称不能为空');
      return;
    }

    setLoading(true);
    setError('');

    try {
      const response = await fetch('/api/accounts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newName.trim(), description: newDescription.trim() })
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || '创建失败');
      }

      setNewName('');
      setNewDescription('');
      onRefresh();
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async (id) => {
    if (!confirm('确定删除此账户？删除后无法恢复。')) return;

    setLoading(true);
    setError('');

    try {
      const response = await fetch(`/api/accounts/${id}`, { method: 'DELETE' });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || '删除失败');
      }

      if (currentAccount === id) {
        onSwitch(1); // 切换到默认账户
      }
      onRefresh();
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const handleUpdate = async (id) => {
    if (!editName.trim()) {
      setError('账户名称不能为空');
      return;
    }

    setLoading(true);
    setError('');

    try {
      const response = await fetch(`/api/accounts/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: editName.trim(), description: editDescription.trim() })
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || '更新失败');
      }

      setEditingId(null);
      onRefresh();
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const startEdit = (account) => {
    setEditingId(account.id);
    setEditName(account.name);
    setEditDescription(account.description || '');
    setError('');
  };

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-2xl max-h-[90vh] overflow-hidden animate-in fade-in zoom-in duration-200 flex flex-col">
        <div className="px-6 py-4 border-b border-slate-100 flex justify-between items-center bg-slate-50 shrink-0">
          <h2 className="text-xl font-bold text-slate-800">账户管理</h2>
          <button onClick={onClose} className="text-slate-400 hover:text-slate-600">
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-6 overflow-y-auto flex-1">
          {error && (
            <div className="mb-4 p-3 bg-red-50 text-red-600 rounded-lg text-sm">
              {error}
            </div>
          )}

          {/* Account List */}
          <div className="mb-6">
          <h3 className="text-sm font-semibold text-slate-700 mb-3">现有账户</h3>
          <div className="space-y-2">
            {accounts.map(acc => (
              <div key={acc.id} className="flex items-center justify-between p-3 bg-slate-50 rounded-lg">
                {editingId === acc.id ? (
                  <div className="flex-1 space-y-2">
                    <input
                      type="text"
                      value={editName}
                      onChange={(e) => setEditName(e.target.value)}
                      className="w-full px-3 py-2 border border-slate-300 rounded-lg"
                      placeholder="账户名称"
                    />
                    <input
                      type="text"
                      value={editDescription}
                      onChange={(e) => setEditDescription(e.target.value)}
                      className="w-full px-3 py-2 border border-slate-300 rounded-lg"
                      placeholder="账户描述（可选）"
                    />
                    <div className="flex gap-2">
                      <button
                        onClick={() => handleUpdate(acc.id)}
                        disabled={loading}
                        className="px-3 py-1 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700 disabled:opacity-50"
                      >
                        保存
                      </button>
                      <button
                        onClick={() => setEditingId(null)}
                        className="px-3 py-1 bg-slate-200 text-slate-700 rounded-lg text-sm hover:bg-slate-300"
                      >
                        取消
                      </button>
                    </div>
                  </div>
                ) : (
                  <>
                    <div className="flex-1">
                      <div className="font-medium text-slate-800">{acc.name}</div>
                      {acc.description && (
                        <div className="text-sm text-slate-500">{acc.description}</div>
                      )}
                    </div>
                    <div className="flex gap-2">
                      {acc.id !== 1 && (
                        <>
                          <button
                            onClick={() => startEdit(acc)}
                            className="p-2 text-slate-600 hover:text-blue-600 hover:bg-blue-50 rounded-lg"
                            title="编辑"
                          >
                            <Edit2 className="w-4 h-4" />
                          </button>
                          <button
                            onClick={() => handleDelete(acc.id)}
                            disabled={loading}
                            className="p-2 text-slate-600 hover:text-red-600 hover:bg-red-50 rounded-lg disabled:opacity-50"
                            title="删除"
                          >
                            <Trash2 className="w-4 h-4" />
                          </button>
                        </>
                      )}
                      {acc.id === 1 && (
                        <span className="text-xs text-slate-400 px-2 py-1">系统默认</span>
                      )}
                    </div>
                  </>
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Create New Account */}
        <div>
          <h3 className="text-sm font-semibold text-slate-700 mb-3">创建新账户</h3>
          <div className="space-y-3">
            <input
              type="text"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              className="w-full px-3 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder="账户名称"
            />
            <input
              type="text"
              value={newDescription}
              onChange={(e) => setNewDescription(e.target.value)}
              className="w-full px-3 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder="账户描述（可选）"
            />
            <button
              onClick={handleCreate}
              disabled={loading || !newName.trim()}
              className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Plus className="w-4 h-4" />
              创建账户
            </button>
          </div>
        </div>
        </div>
      </div>
    </div>
  );
};

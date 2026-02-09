import axios from 'axios';

const API_BASE_URL = '/api';

export const api = axios.create({
  baseURL: API_BASE_URL,
});

export const searchFunds = async (query) => {
  try {
    const response = await api.get('/search', { params: { q: query } });
    return response.data;
  } catch (error) {
    console.error("Search failed", error);
    return [];
  }
};

export const getFundDetail = async (fundId) => {
  try {
    const response = await api.get(`/fund/${fundId}`);
    return response.data;
  } catch (error) {
    console.error(`Get fund ${fundId} failed`, error);
    throw error;
  }
};

export const getFundHistory = async (fundId, limit = 30, accountId = null) => {
    try {
        const params = { limit };
        if (accountId) {
            params.account_id = accountId;
        }
        const response = await api.get(`/fund/${fundId}/history`, { params });
        return response.data;
    } catch (error) {
        console.error("Get history failed", error);
        return { history: [], transactions: [] };
    }
};

export const subscribeFund = async (fundId, data) => {
    return api.post(`/fund/${fundId}/subscribe`, data);
};

export const getFundCategories = async () => {
    try {
        const response = await api.get('/categories');
        return response.data.categories || [];
    } catch (error) {
        console.error("Get categories failed", error);
        return [];
    }
};

// Account management
export const getAccounts = async () => {
    try {
        const response = await api.get('/accounts');
        return response.data.accounts || [];
    } catch (error) {
        console.error("Get accounts failed", error);
        return [];
    }
};

export const createAccount = async (data) => {
    return api.post('/accounts', data);
};

export const updateAccount = async (accountId, data) => {
    return api.put(`/accounts/${accountId}`, data);
};

export const deleteAccount = async (accountId) => {
    return api.delete(`/accounts/${accountId}`);
};

// Position management (with account_id)
export const getAccountPositions = async (accountId = 1) => {
    try {
        const response = await api.get('/account/positions', { params: { account_id: accountId } });
        return response.data;
    } catch (error) {
        console.error("Get positions failed", error);
        throw error;
    }
};

export const updatePosition = async (data, accountId = 1) => {
    return api.post('/account/positions', data, { params: { account_id: accountId } });
};

export const deletePosition = async (code, accountId = 1) => {
    return api.delete(`/account/positions/${code}`, { params: { account_id: accountId } });
};

export const addPositionTrade = async (code, data, accountId = 1) => {
    const response = await api.post(`/account/positions/${code}/add`, data, { params: { account_id: accountId } });
    return response.data;
};

export const reducePositionTrade = async (code, data, accountId = 1) => {
    const response = await api.post(`/account/positions/${code}/reduce`, data, { params: { account_id: accountId } });
    return response.data;
};

export const getTransactions = async (accountId = 1, code = null, limit = 100) => {
    const params = { account_id: accountId, limit };
    if (code) params.code = code;
    const response = await api.get('/account/transactions', { params });
    return response.data.transactions || [];
};

export const updatePositionsNav = async (accountId = 1) => {
    return api.post('/account/positions/update-nav', null, { params: { account_id: accountId } });
};

// AI Prompts management
export const getPrompts = async () => {
    try {
        const response = await api.get('/ai/prompts');
        return response.data.prompts || [];
    } catch (error) {
        console.error("Get prompts failed", error);
        return [];
    }
};

export const createPrompt = async (data) => {
    return api.post('/ai/prompts', data);
};

export const updatePrompt = async (id, data) => {
    return api.put(`/ai/prompts/${id}`, data);
};

export const deletePrompt = async (id) => {
    return api.delete(`/ai/prompts/${id}`);
};

// Data import/export
export const exportData = async (modules) => {
    try {
        const modulesParam = modules.join(',');
        const response = await api.get(`/data/export?modules=${modulesParam}`, {
            responseType: 'blob'
        });

        // Create download link
        const url = window.URL.createObjectURL(new Blob([response.data]));
        const link = document.createElement('a');
        link.href = url;

        // Extract filename from Content-Disposition header or use default
        const contentDisposition = response.headers['content-disposition'];
        let filename = 'fundval_export.json';
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="?(.+)"?/);
            if (filenameMatch) {
                filename = filenameMatch[1];
            }
        }

        link.setAttribute('download', filename);
        document.body.appendChild(link);
        link.click();
        link.remove();
        window.URL.revokeObjectURL(url);

        return { success: true };
    } catch (error) {
        console.error('Export failed', error);
        throw error;
    }
};

export const importData = async (data, modules, mode) => {
    return api.post('/data/import', { data, modules, mode });
};

// User preferences (watchlist, current account, sort option)
export const getPreferences = async () => {
    try {
        const response = await api.get('/preferences');
        return response.data;
    } catch (error) {
        console.error('Get preferences failed', error);
        return { watchlist: '[]', currentAccount: 1, sortOption: null };
    }
};

export const updatePreferences = async (data) => {
    return api.post('/preferences', data);
};

// Strategy portfolio
export const listStrategyPortfolios = async (accountId = null) => {
    const params = {};
    if (accountId) params.account_id = accountId;
    const response = await api.get('/strategy/portfolios', { params });
    return response.data.portfolios || [];
};

export const createStrategyPortfolio = async (data) => {
    const response = await api.post('/strategy/portfolios', data);
    return response.data;
};

export const getStrategyPortfolio = async (portfolioId) => {
    const response = await api.get(`/strategy/portfolios/${portfolioId}`);
    return response.data;
};

export const deleteStrategyPortfolio = async (portfolioId) => {
    try {
        const response = await api.delete(`/strategy/portfolios/${portfolioId}`);
        return response.data;
    } catch (error) {
        // Backward compatible fallback when backend method table is stale.
        if (error?.response?.status === 405) {
            const response = await api.post(`/strategy/portfolios/${portfolioId}/delete`);
            return response.data;
        }
        throw error;
    }
};

export const createStrategyVersion = async (portfolioId, data) => {
    const response = await api.post(`/strategy/portfolios/${portfolioId}/versions`, data);
    return response.data;
};

export const updateStrategyScope = async (portfolioId, scopeCodes) => {
    const response = await api.patch(`/strategy/portfolios/${portfolioId}/scope`, {
        scope_codes: scopeCodes || []
    });
    return response.data;
};

export const getStrategyPerformance = async (portfolioId, accountId) => {
    const response = await api.get(`/strategy/portfolios/${portfolioId}/performance`, {
        params: { account_id: accountId }
    });
    return response.data;
};

export const runStrategyBacktest = async (portfolioId, data) => {
    const response = await api.post(`/strategy/portfolios/${portfolioId}/backtest`, data);
    return response.data;
};

export const getStrategyPositionsView = async (portfolioId, accountId) => {
    const response = await api.get(`/strategy/portfolios/${portfolioId}/positions`, {
        params: { account_id: accountId }
    });
    return response.data;
};

export const getStrategyScopeCandidates = async (portfolioId, accountId) => {
    const response = await api.get(`/strategy/portfolios/${portfolioId}/scope-candidates`, {
        params: { account_id: accountId }
    });
    return response.data.rows || [];
};

export const recognizeStrategyHoldingsFromImage = async (imageDataUrl) => {
    const response = await api.post('/strategy/holdings-ocr', {
        image_data_url: imageDataUrl
    });
    return response.data;
};

export const generateStrategyRebalance = async (portfolioId, data) => {
    const response = await api.post(`/strategy/portfolios/${portfolioId}/rebalance`, data);
    return response.data;
};

export const listRebalanceOrders = async (portfolioId, accountId, status = null, batchId = null) => {
    const params = { account_id: accountId };
    if (status) params.status = status;
    if (batchId) params.batch_id = batchId;
    const response = await api.get(`/strategy/portfolios/${portfolioId}/rebalance-orders`, { params });
    const rows = response.data.orders || [];
    if (batchId) return rows.filter((r) => Number(r.batch_id) === Number(batchId));
    return rows;
};

export const updateRebalanceOrderStatus = async (orderId, status) => {
    const response = await api.post(`/strategy/rebalance-orders/${orderId}/status`, { status });
    return response.data;
};

export const executeRebalanceOrder = async (orderId, data) => {
    try {
        const response = await api.post(`/strategy/rebalance-orders/${orderId}/execute`, data);
        return response.data;
    } catch (error) {
        if (error?.response?.status === 405) {
            const response = await api.post(`/strategy/rebalance-orders/${orderId}/apply`, data);
            return response.data;
        }
        throw error;
    }
};

export const listRebalanceBatches = async (portfolioId, accountId) => {
    const response = await api.get(`/strategy/portfolios/${portfolioId}/rebalance-batches`, {
        params: { account_id: accountId }
    });
    return response.data.batches || [];
};

export const completeRebalanceBatch = async (batchId) => {
    const response = await api.post(`/strategy/rebalance-batches/${batchId}/complete`);
    return response.data;
};

export const getBacktestPromptPresets = async () => {
    const response = await api.get('/ai/backtest-prompts');
    return response.data.prompts || [];
};

export const analyzeBacktestWithAI = async (backtestResult, style = 'hardcore_audit') => {
    const response = await api.post('/ai/analyze_backtest', {
        backtest_result: backtestResult,
        style
    });
    return response.data;
};

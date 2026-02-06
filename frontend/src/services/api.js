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

export const getFundHistory = async (fundId, limit = 30) => {
    try {
        const response = await api.get(`/fund/${fundId}/history`, { params: { limit } });
        return response.data;
    } catch (error) {
        console.error("Get history failed", error);
        return [];
    }
};

export const subscribeFund = async (fundId, data) => {
    return api.post(`/fund/${fundId}/subscribe`, data);
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

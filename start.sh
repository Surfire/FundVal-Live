#!/bin/bash

# FundVal Live - Development Mode Startup Script
# For desktop app users: Download from https://github.com/Surfire/FundVal-Live/releases

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

echo -e "${GREEN}>>> Starting FundVal Live (Development Mode)...${NC}"
echo -e "${YELLOW}>>> For desktop app, download from Releases page${NC}"
echo ""

# Dev isolation defaults:
# - Use a different port from desktop release app to avoid conflict.
# - Use an isolated development database under backend/data/dev.
DEV_PORT="${FUNDVAL_DEV_PORT:-21346}"
DEV_DB_PATH="${FUNDVAL_DB_PATH:-$(pwd)/backend/data/dev/fund-dev.db}"
mkdir -p "$(dirname "$DEV_DB_PATH")"

# Create logs directory
mkdir -p logs backend/data

# 1. Check Prerequisites
if ! command -v uv &> /dev/null; then
    echo -e "${RED}Error: 'uv' is not installed. Please install it first.${NC}"
    exit 1
fi

if ! command -v npm &> /dev/null; then
    echo -e "${RED}Error: 'npm' is not installed. Please install Node.js.${NC}"
    exit 1
fi

# 2. Build Frontend
echo -e "${BLUE}>>> [1/2] Building Frontend...${NC}"
cd frontend || exit
if [ ! -d "node_modules" ]; then
    npm install > /dev/null 2>&1
fi
npm run build > ../logs/frontend-build.log 2>&1
cd ..

if [ ! -d "frontend/dist" ]; then
    echo -e "${RED}✘ Frontend build failed (Check logs/frontend-build.log)${NC}"
    exit 1
fi
echo -e "${GREEN}✔ Frontend built${NC}"

# 3. Start Backend
echo -e "${BLUE}>>> [2/2] Starting Backend...${NC}"
cd backend || exit
uv sync > /dev/null 2>&1
# Start with nohup and redirect to root logs folder
FUNDVAL_DB_PATH="$DEV_DB_PATH" nohup uv run uvicorn app.main:app --port "$DEV_PORT" --host 0.0.0.0 > ../logs/backend.log 2>&1 &
BACKEND_PID=$!
echo $BACKEND_PID > ../backend.pid
cd ..

# 4. Final Validation
echo -e "${GREEN}>>> Waiting for initialization...${NC}"
sleep 4

echo -e "------------------------------------------------"
if ps -p $(cat backend.pid) > /dev/null; then
    echo -e "${GREEN}✔ Backend  : RUNNING (Port ${DEV_PORT})${NC}"
    echo -e "${GREEN}>>> Access : http://localhost:${DEV_PORT}${NC}"
    echo -e "${GREEN}>>> Dev DB : ${DEV_DB_PATH}${NC}"
else
    echo -e "${RED}✘ Backend  : FAILED (Check logs/backend.log)${NC}"
fi
echo -e "------------------------------------------------"
echo -e "${BLUE}View logs with: tail -f logs/backend.log${NC}"

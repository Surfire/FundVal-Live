#!/bin/bash

# FundVal Live - Stop Development Services
# Note: This script is for development mode only
# Desktop app users can simply quit the application

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}>>> Stopping FundVal Live (Development Mode)...${NC}"

# Kill Backend
if [ -f "backend.pid" ]; then
    PID=$(cat backend.pid)
    if ps -p $PID > /dev/null; then
        kill $PID
        echo -e "Backend (PID: $PID) stopped."
    else
        echo -e "${RED}Backend process $PID not found.${NC}"
    fi
    rm backend.pid
else
    echo "No backend PID file found."
fi

echo -e "${GREEN}>>> All services stopped.${NC}"

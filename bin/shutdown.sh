#!/bin/bash
if [ -f logs/app.pid ]; then
    PID=$(cat logs/app.pid)
    echo "Stopping short-link-service (PID: $PID)..."
    kill $PID
    rm -f logs/app.pid
    echo "Stopped"
else
    echo "PID file not found, app may not be running"
fi

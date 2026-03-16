#!/bin/zsh
cd "$(dirname "$0")" || exit 1
python3 app.py seo-serve --host 127.0.0.1 --port 8876 &
SERVER_PID=$!
sleep 1
open "http://127.0.0.1:8876/"
wait $SERVER_PID

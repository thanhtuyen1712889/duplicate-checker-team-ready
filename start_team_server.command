#!/bin/zsh
cd "$(dirname "$0")"
IP="$(ipconfig getifaddr en0 2>/dev/null)"
if [ -z "$IP" ]; then
  IP="$(ipconfig getifaddr en1 2>/dev/null)"
fi
echo "Starting team server..."
if [ -n "$IP" ]; then
  echo "Open from the host machine: http://127.0.0.1:8765/"
  echo "Open from the same network: http://$IP:8765/"
else
  echo "Open from the host machine: http://127.0.0.1:8765/"
fi
python3 app.py serve --host 0.0.0.0 --port 8765

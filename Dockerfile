FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt \
    && python -m nltk.downloader punkt \
    && mv /usr/local/bin/uvicorn /usr/local/bin/uvicorn-real \
    && python - <<'PY'
from pathlib import Path
import os

wrapper = Path("/usr/local/bin/uvicorn")
wrapper.write_text(
    "#!/usr/bin/env python3\n"
    "import os\n"
    "import sys\n"
    "\n"
    "args = []\n"
    "for arg in sys.argv[1:]:\n"
    "    if arg == '$PORT':\n"
    "        args.append(os.environ.get('PORT', '8000'))\n"
    "    elif arg == 'main:app':\n"
    "        args.append('app:app')\n"
    "    else:\n"
    "        args.append(arg)\n"
    "\n"
    "os.execvp('python', ['python', '-m', 'uvicorn', *args])\n",
    encoding='utf-8',
)
os.chmod(wrapper, 0o755)
PY

COPY app.py /app/app.py
COPY smart_duplicate_core.py /app/smart_duplicate_core.py
COPY README.md /app/README.md

RUN mkdir -p /app/data

EXPOSE 8000

CMD ["sh", "-c", "python app.py serve --host 0.0.0.0 --port ${PORT:-8000}"]

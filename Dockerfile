FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt \
    && python -m spacy download en_core_web_sm \
    && python - <<'PY'
import nltk
nltk.download("punkt")
PY

COPY app.py /app/app.py
COPY smart_duplicate_core.py /app/smart_duplicate_core.py

RUN mkdir -p /app/data

EXPOSE 8000

CMD ["sh", "-c", "python app.py serve --host 0.0.0.0 --port ${PORT:-8000}"]

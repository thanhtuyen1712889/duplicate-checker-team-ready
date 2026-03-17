# Bộ kiểm tra trùng lặp nội dung thông minh

Ứng dụng web kiểm tra trùng lặp nội dung cho trang sản phẩm, xây bằng `FastAPI + HTML/JS + SQLite`.

Luồng chính:

1. Tạo dự án.
2. Thêm bài mẫu để hệ thống tự phát hiện phần được phép trùng.
3. Thêm bài mới bằng link Google Docs hoặc dán nội dung.
4. Xem kết quả xung đột, lọc theo section, bài xung đột, mức độ.
5. Xuất Excel hoặc sao lưu ZIP.

## Tính năng chính

- Tạo nhiều dự án độc lập.
- Nhập bài mẫu từ Google Docs hoặc raw text.
- Tự phát hiện:
  - regex thông số
  - cụm từ thương hiệu / tên sản phẩm
  - câu boilerplate
  - section được bỏ qua hoàn toàn
- Tự strip allowed zone trước khi so sánh.
- So sánh 3 lớp:
  - n-gram overlap
  - semantic similarity
  - sentence-level LCS
- Guard giảm false positive:
  - bỏ qua section specs
  - bỏ qua câu quá ngắn sau stripping
  - nới ngưỡng cho biến thể cùng dòng sản phẩm
  - guard semantic cao nhưng n-gram thấp
  - penalty cho section quá ngắn
- Hỗ trợ:
  - re-check từng bài
  - xóa bài
  - xóa dự án
  - nhân bản dự án
  - backup ZIP
  - khôi phục lại từ ZIP

## Cấu trúc file

```text
/Users/bssgroup/Codex test
├── app.py
├── smart_duplicate_core.py
├── requirements.txt
├── Dockerfile
├── railway.json
├── README.md
├── data/
│   └── smart_duplicate.sqlite3
└── tests/
    └── test_smart_duplicate_core.py
```

## Chạy local

Tạo môi trường ảo và cài dependencies:

```bash
cd "/Users/bssgroup/Codex test"
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm
python -m nltk.downloader punkt
```

Chạy web app:

```bash
python3.11 app.py serve --host 127.0.0.1 --port 8000
```

Mở trình duyệt:

```text
http://127.0.0.1:8000
```

## Chạy self-test

```bash
python3 app.py self-test
```

Self-test sẽ:

- tạo project `Test`
- thêm 1 bài mẫu
- thêm 1 bài gần như giống
- thêm 1 bài paraphrase
- thêm 1 bài khác hoàn toàn
- kiểm tra rằng:
  - bài giống bị `🔴`
  - bài paraphrase bị `🟡` hoặc `🔴`
  - bài khác bị `✅`
  - section specs không gây flag sai

## API chính

- `POST /api/project/create`
- `GET /api/project/list`
- `POST /api/project/import`
- `POST /api/project/{id}/template`
- `POST /api/project/{id}/allowed-zone`
- `POST /api/project/{id}/add-doc`
- `GET /api/project/{id}/docs`
- `GET /api/project/{id}/results`
- `GET /api/project/{id}/export`
- `DELETE /api/project/{id}/delete`
- `POST /api/project/{id}/rename`
- `POST /api/project/{id}/duplicate`
- `GET /api/project/{id}/status`
- `DELETE /api/document/{id}`
- `POST /api/document/{id}/recheck`

## Deploy lên Railway

Repo này đã có sẵn `Dockerfile` và `railway.json`.

Các bước:

1. Push code lên GitHub.
2. Vào Railway, tạo project mới.
3. Chọn `Deploy from GitHub repo`.
4. Chọn đúng repo này.
5. Railway sẽ tự nhận `Dockerfile` ở thư mục gốc.
6. Đợi build xong service.
7. Vào phần `Networking` của service và bấm `Generate Domain`.
8. Không cần set biến môi trường bắt buộc nếu dùng SQLite local.
9. Truy cập domain vừa tạo để dùng app.

Lưu ý:

- SQLite sẽ nằm trong container, nên bản free chỉ phù hợp demo hoặc nhóm nhỏ.
- Nếu cần dữ liệu bền vững hơn trên Railway, nên mount volume hoặc đổi sang managed database.
- Railway sẽ gọi `/healthz` để health check.

## Ghi chú vận hành

- Link Google Docs phải được share ở chế độ xem được bằng link.
- Nếu export Google Docs bị chặn, UI sẽ báo để người dùng chuyển sang dán raw text.
- Nếu `sentence-transformers` chưa tải được model, hệ thống sẽ tự fallback sang `n-gram + LCS`.
- Dữ liệu lưu trong SQLite tại `data/smart_duplicate.sqlite3`.
- Stack pinned ở `requirements.txt` nên nên chạy bằng Python `3.11` để khớp môi trường Docker/Railway.

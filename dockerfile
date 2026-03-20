# Sử dụng image Python nhẹ để tối ưu dung lượng
FROM python:3.11-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# Copy file requirements và cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ mã nguồn vào container
COPY src/ ./src/

# QUAN TRỌNG: Thiết lập PYTHONPATH để import từ folder src
ENV PYTHONPATH=/app/src

# Lệnh khởi chạy module binance
CMD ["python", "-m", "binance.main"]
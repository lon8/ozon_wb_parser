FROM python:3.11-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файл requirements.txt в рабочую директорию
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем все остальные файлы в рабочую директорию
COPY . .

# Указываем команду для запуска сервера
CMD ["python", "main.py"]

# Открываем порт 8000
EXPOSE 8000
FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml poetry.lock* ./
RUN pip install uv \
    && uv pip install --system --requirements pyproject.toml

ENV PYTHONPATH=/app

COPY . .

EXPOSE 8050

# ENTRYPOINTを空にすることでCMDが素直に実行されるようにする
# ENTRYPOINT []
# CMD ["python", "dash_app/app.py"]
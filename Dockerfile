# ─────────────────────────────────────────────────────────────
# Frontend build — compila o React (Vite) sem VITE_API_URL.
# As URLs /api/* são relativas; o mesmo host serve API e UI.
# ─────────────────────────────────────────────────────────────
FROM node:22-slim AS web-builder
WORKDIR /web
COPY apps/dashboard/web/package*.json ./
RUN npm ci --ignore-scripts
COPY apps/dashboard/web/ ./
RUN npm run build

# ─────────────────────────────────────────────────────────────
# Build — instala o workspace uv com todas as dependências.
# Uma única imagem; o CMD é sobrescrito por serviço no Cloud Run.
# ─────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# uv para gerenciar o workspace
RUN pip install --no-cache-dir uv==0.5.0

# Manifests primeiro para aproveitar o cache de camadas
COPY pyproject.toml uv.lock ./

# Todo o código-fonte do workspace
COPY libs/ libs/
COPY apps/ apps/
COPY services/ services/
COPY contracts/ contracts/
COPY tools/ tools/
COPY governance/ governance/
COPY dbt/ dbt/

# Frontend compilado copiado para o workspace (pulso-serve serve como estático)
COPY --from=web-builder /web/dist apps/dashboard/web/dist/

# Instala todas as dependências sem pacotes de dev
RUN uv sync --frozen --no-dev

# ─────────────────────────────────────────────────────────────
# Runtime — imagem enxuta sem uv nem cache de build
# ─────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Usuário não-root
RUN useradd -m -u 1000 pulso

COPY --from=builder --chown=pulso:pulso /app /app

USER pulso

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

# Cloud Run injeta $PORT; o default 8080 mantém a imagem utilizável localmente.
# Cada serviço sobrescreve o CMD na definição do Cloud Run (ver infra/terraform/cloud_run.tf).
EXPOSE 8080

CMD ["python", "-m", "pulso_serve"]

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GuanHe Capital is a full-stack investment research platform that aggregates and analyzes financial content from multiple sources (Twitter, YouTube, news feeds, uploaded documents) with AI-powered summarization, tagging, and semantic search capabilities.

## Development Commands

### Backend (Python FastAPI)

```bash
# Install dependencies
pip install -r requirements.txt

# Run development server
cd backend && python main.py  # or: uvicorn main:app --reload --port 8000

# Run tests
pytest backend/tests/ -v

# Run single test file
pytest backend/tests/test_agent_v1.py -v
```

### Frontend (React TypeScript)

```bash
cd web-frontend

# Install dependencies
npm install

# Development server (port 3000)
npm start

# Production build
npm run build

# Run tests
npm test
```

### Service Management (Production)

```bash
# Start all services (requires root)
sudo ./start_services.sh

# Stop all services
sudo ./stop_services.sh

# Restart backend only
sudo ./restart_backend.sh

# View service logs
journalctl -u rc-webapp -f      # FastAPI backend
journalctl -u rc-celery-ai -f   # AI/file processing worker
journalctl -u rc-celery-data -f # Data fetching worker
journalctl -u rc-scheduler -f   # Scheduler service
```

## Architecture

### Backend Structure

- **`backend/main.py`**: FastAPI application entry point, registers all API routers
- **`backend/api/`**: HTTP route handlers (auth, files, tasks, markets, subscriptions, etc.)
- **`backend/services/`**: Business logic layer (~60 service modules)
- **`backend/tasks/`**: Celery async tasks organized by function (file_tasks, data_tasks, theme_tasks, etc.)
- **`backend/database/`**: PostgreSQL database manager and migrations
- **`backend/core/`**: LLM framework, config resolver, rate limiters, prompt renderer
- **`backend/schemas/`**: Pydantic request/response models
- **`backend/auth/`**: JWT authentication utilities

### Celery Task Queues

Task routing is defined in `backend/celery_config.py`:

| Queue | Purpose | Example Tasks |
|-------|---------|---------------|
| `file_processing` | File upload/analysis | PDF parsing, auto-tagging, auto-naming |
| `ai_processing` | LLM calls | Theme embeddings, content summarization, vectorization |
| `data_fetching` | External API fetch | EODHD fundamentals, earnings calendar, Twitter feeds |
| `youtube_fetching` | YouTube metadata | Subscription fetch, transcript pull (remote worker) |
| `youtube_transcription` | ASR transcription | Local Fun-ASR for videos without subtitles |

### Frontend Structure

- **`web-frontend/src/App.tsx`**: Main routing and layout
- **`web-frontend/src/pages/`**: Page components (FilesPage, FeedReaderPage, DailyPage, etc.)
- **`web-frontend/src/components/`**: Reusable UI components
- **`web-frontend/src/services/`**: API client functions (axios-based)
- **`web-frontend/src/hooks/`**: Custom React hooks

### Scheduler Service

Independent FastAPI service on port 8001 (`scheduler_main.py`). Managed via scripts in `scheduler/` directory. Communicates with main backend via `/api/scheduler/...` proxy routes.

### MCP Server

`tools/guanhe-mcp/` provides a Model Context Protocol server wrapping the readonly Agent HTTP API. Published as wheel files in `dist/`. Used by Cursor/Claude Desktop for semantic search and daily feed access.

## Key Design Patterns

### LLM Integration

The system uses a unified LLM framework (`backend/core/llm_framework.py`) with:

1. **ConfigResolver**: Per-user, per-module provider/model selection with fallback chains
2. **PromptRenderer**: Jinja2 templates from `llm_prompt_templates` DB table
3. **LLMTemplateResolver**: Priority chain: runtime > user_owned > system_default
4. **Rate Limiters**: Per-user LLM rate limiting and PAT (Personal Access Token) rate limiting

Template types:
- **SYSTEM_FIXED**: Admin-only edits, global consistency (e.g., `feed_tagging`, `file_naming`)
- **USER_OWNED**: Per-user customization (e.g., `file_analysis`, `feed_summary`)

See `docs/instructions/LLM_ARCHITECTURE_OVERVIEW.md` for full details.

### Timezone Contract

All datetime values stored as UTC in PostgreSQL. The system registers a psycopg2 UTC adapter at startup (`backend/utils/datetime_utils.py`). Naive datetime objects are automatically converted to UTC before DB writes.

### Vector RAG System

OSS Vector Bucket for semantic search over uploaded documents and long feed content. See `docs/instructions/VECTOR_RAG_SYSTEM.md` for architecture.

### Database Migrations

Located in `backend/database/migrations/`. Schema changes tracked via SQL files with naming convention `pr_x_description.sql`.

## Environment Configuration

All config via `.env` file (see `example.env`). Key variables:

| Variable | Description |
|----------|-------------|
| `DB_*` | PostgreSQL connection |
| `REDIS_URL` | Celery broker |
| `JWT_SECRET_KEY` | Auth signing |
| `DEFAULT_ADMIN_*` | Initial admin credentials |
| `PUBLIC_WEB_URL` | Frontend public URL for Agent deep links |

## Important Documentation

- `docs/instructions/LLM_ARCHITECTURE_OVERVIEW.md`: LLM calling patterns, template system
- `docs/instructions/DAILY_MODULE.md`: Daily feed aggregation architecture
- `docs/instructions/SCHEDULER_DEVELOPMENT_GUIDE.md`: Scheduler service internals
- `docs/instructions/FEEDS_MODULE.md`: Twitter/YouTube subscription system
- `docs/instructions/VECTOR_RAG_SYSTEM.md`: Semantic search implementation
- `deployment/DEPLOY_GUIDE.md`: Production deployment guide
import React, { useMemo, useState } from 'react';
import { message } from 'antd';
import { CopyOutlined, LinkOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

function suggestedApiOrigin(): string {
  const raw = (process.env.REACT_APP_API_BASE_URL || '').trim();
  if (typeof window === 'undefined') return '';
  if (!raw || raw.startsWith('/')) return window.location.origin;
  try {
    const u = new URL(raw);
    u.pathname = '';
    u.search = '';
    u.hash = '';
    return u.toString().replace(/\/$/, '');
  } catch {
    return window.location.origin;
  }
}

// ─── Small primitives ────────────────────────────────────────────────────────

const TEAL = '#0d9488';
const TEAL_LIGHT = '#f0fdf9';
const TEAL_DARK = '#0f766e';
const DARK_BG = '#0f172a';
const DARK_SURFACE = '#1e293b';
const DARK_BORDER = '#334155';

const CodeBlock: React.FC<{
  children: string;
  dark: boolean;
  label?: string;
}> = ({ children, dark, label }) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(children);
      setCopied(true);
      setTimeout(() => setCopied(false), 1800);
    } catch {
      message.error('Copy failed');
    }
  };

  return (
    <div
      style={{
        position: 'relative',
        borderRadius: 10,
        border: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
        background: dark ? '#0f172a' : '#f8fafc',
        overflow: 'hidden',
      }}
    >
      {label && (
        <div
          style={{
            padding: '6px 14px',
            borderBottom: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
            fontSize: 11,
            color: dark ? '#64748b' : '#94a3b8',
            fontFamily: 'ui-monospace, SFMono-Regular, monospace',
            letterSpacing: '0.06em',
            textTransform: 'uppercase',
          }}
        >
          {label}
        </div>
      )}
      <pre
        style={{
          margin: 0,
          padding: '14px 48px 14px 16px',
          fontSize: 13.5,
          lineHeight: 1.65,
          color: dark ? '#e2e8f0' : '#1e293b',
          fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
          overflowX: 'auto',
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-all',
        }}
      >
        {children}
      </pre>
      <button
        onClick={() => void handleCopy()}
        title={copied ? 'Copied!' : 'Copy'}
        style={{
          position: 'absolute',
          top: label ? 40 : 10,
          right: 10,
          background: copied
            ? TEAL
            : dark
              ? 'rgba(255,255,255,0.07)'
              : 'rgba(0,0,0,0.06)',
          border: 'none',
          borderRadius: 6,
          padding: '4px 8px',
          cursor: 'pointer',
          color: copied ? '#fff' : dark ? '#94a3b8' : '#64748b',
          fontSize: 12,
          display: 'flex',
          alignItems: 'center',
          gap: 4,
          transition: 'all .15s',
        }}
      >
        <CopyOutlined />
        {copied ? '已复制' : '复制'}
      </button>
    </div>
  );
};

const PromptItem: React.FC<{ text: string; dark: boolean }> = ({ text, dark }) => (
  <div
    style={{
      padding: '11px 16px',
      borderRadius: 8,
      border: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
      color: dark ? '#cbd5e1' : '#334155',
      fontSize: 14,
      lineHeight: 1.5,
    }}
  >
    "{text}"
  </div>
);

type IntentRow = { intent: string; endpoint: string };
const IntentTable: React.FC<{ rows: IntentRow[]; dark: boolean }> = ({ rows, dark }) => (
  <div
    style={{
      borderRadius: 10,
      border: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
      overflow: 'hidden',
    }}
  >
    <table style={{ width: '100%', borderCollapse: 'collapse' }}>
      <thead>
        <tr style={{ background: dark ? DARK_SURFACE : '#f8fafc' }}>
          <th
            style={{
              padding: '9px 14px',
              textAlign: 'left',
              fontSize: 12,
              fontWeight: 600,
              color: dark ? '#94a3b8' : '#64748b',
              letterSpacing: '0.05em',
              borderBottom: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
            }}
          >
            用户怎么说
          </th>
          <th
            style={{
              padding: '9px 14px',
              textAlign: 'left',
              fontSize: 12,
              fontWeight: 600,
              color: dark ? '#94a3b8' : '#64748b',
              letterSpacing: '0.05em',
              borderBottom: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
            }}
          >
            调用的端点
          </th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr
            key={i}
            style={{
              borderBottom:
                i < rows.length - 1 ? `1px solid ${dark ? DARK_BORDER : '#f1f5f9'}` : 'none',
            }}
          >
            <td
              style={{
                padding: '10px 14px',
                fontSize: 13.5,
                color: dark ? '#cbd5e1' : '#334155',
                lineHeight: 1.5,
              }}
            >
              {r.intent}
            </td>
            <td
              style={{
                padding: '10px 14px',
                fontFamily: 'ui-monospace, SFMono-Regular, monospace',
                fontSize: 12.5,
                color: TEAL,
                lineHeight: 1.5,
              }}
            >
              {r.endpoint}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);

const SectionLabel: React.FC<{ children: React.ReactNode; dark: boolean }> = ({ children, dark }) => (
  <div
    style={{
      fontSize: 11,
      fontWeight: 700,
      letterSpacing: '0.1em',
      textTransform: 'uppercase',
      color: TEAL,
      marginBottom: 16,
    }}
  >
    {children}
  </div>
);

const SectionTitle: React.FC<{ children: React.ReactNode; dark: boolean }> = ({ children, dark }) => (
  <h2
    style={{
      fontSize: 28,
      fontWeight: 700,
      margin: '0 0 12px',
      color: dark ? '#f1f5f9' : '#0f172a',
      lineHeight: 1.25,
    }}
  >
    {children}
  </h2>
);

const SectionDesc: React.FC<{ children: React.ReactNode; dark: boolean }> = ({ children, dark }) => (
  <p
    style={{
      fontSize: 15,
      lineHeight: 1.7,
      color: dark ? '#94a3b8' : '#475569',
      margin: '0 0 28px',
    }}
  >
    {children}
  </p>
);

const Divider: React.FC<{ dark: boolean }> = ({ dark }) => (
  <div
    style={{
      height: 1,
      background: dark ? DARK_BORDER : '#e2e8f0',
      margin: '48px 0',
    }}
  />
);

// ─── Tabs ─────────────────────────────────────────────────────────────────────

type TabKey = 'skill' | 'mcp' | 'http';

const TABS: { key: TabKey; label: string; sub: string }[] = [
  { key: 'skill', label: 'Skill', sub: '任意 Agent · SKILL.md 标准' },
  { key: 'mcp', label: 'MCP', sub: 'Cursor / Claude · stdio 接入' },
  { key: 'http', label: 'REST API', sub: '开发者 / 自定义集成' },
];

// ─── Page ─────────────────────────────────────────────────────────────────────

const AgentIntegrationPage: React.FC = () => {
  const navigate = useNavigate();
  const { themeMode } = useDashboardSettings();
  const dark = themeMode === 'dark';

  const [activeTab, setActiveTab] = useState<TabKey>('skill');

  const origin = typeof window !== 'undefined' ? window.location.origin : '';
  const skillUrl = `${origin}/agent/skill/SKILL.md`;
  const apiOrigin = suggestedApiOrigin();

  const installZh = useMemo(() => `帮我安装这个 skill：${skillUrl}`, [skillUrl]);
  const installEn = useMemo(() => `Install this skill: ${skillUrl}`, [skillUrl]);

  const mcpJson = useMemo(
    () =>
      JSON.stringify(
        {
          mcpServers: {
            guanhe: {
              command: '/opt/anaconda3/envs/yourEnv/bin/python',
              args: ['-m', 'guanhe_mcp.server'],
              cwd: '/path/to/guanhetech/tools/guanhe-mcp',
              env: {
                GUANHE_TOKEN: 'ght_…',
                GUANHE_BASE_URL: apiOrigin || 'https://your-api-host',
                PYTHONUNBUFFERED: '1',
              },
            },
          },
        },
        null,
        2
      ),
    [apiOrigin]
  );

  const intentRows: IntentRow[] = [
    { intent: '今天有什么值得关注的？', endpoint: 'GET /api/v1/agent/daily?since=24h' },
    { intent: '这周的内容帮我总结一下', endpoint: 'GET /api/v1/agent/daily（默认 7d）' },
    { intent: '帮我找找关于 NVDA 的内容', endpoint: 'GET /api/v1/agent/search?q=NVDA' },
    { intent: '只看我上传的文件', endpoint: 'GET /api/v1/agent/daily?sources=pdf,image,text' },
    { intent: '只看 Twitter 的内容', endpoint: 'GET /api/v1/agent/daily?sources=twitter' },
    { intent: '看看今天的 Twitter 日报', endpoint: 'GET /api/v1/agent/reports/twitter-daily/latest' },
    { intent: '5 月 10 号的 Twitter 日报', endpoint: 'GET /api/v1/agent/reports/twitter-daily/2026-05-10' },
    { intent: '最新的 YouTube 日报', endpoint: 'GET /api/v1/agent/reports/youtube-daily/latest' },
  ];

  const bg = dark ? DARK_BG : '#ffffff';
  const containerStyle: React.CSSProperties = {
    background: bg,
    minHeight: '100vh',
    fontFamily:
      '-apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Hiragino Sans GB", sans-serif',
  };

  return (
    <div style={containerStyle}>
      {/* ── Hero ── */}
      <div style={{ padding: '56px 40px 0', maxWidth: 760, margin: '0 auto' }}>
        <div
          style={{
            display: 'inline-block',
            fontSize: 12,
            fontWeight: 600,
            letterSpacing: '0.08em',
            textTransform: 'uppercase',
            color: TEAL,
            marginBottom: 20,
          }}
        >
          Agent 接入
        </div>

        <h1
          style={{
            fontSize: 40,
            fontWeight: 800,
            lineHeight: 1.15,
            margin: '0 0 16px',
            color: dark ? '#f1f5f9' : '#0f172a',
          }}
        >
          把{' '}
          <span style={{ color: TEAL }}>GuanHe</span>{' '}
          接进你的工作流
        </h1>

        <p style={{ fontSize: 16, lineHeight: 1.75, color: dark ? '#94a3b8' : '#475569', margin: 0 }}>
          让 Cursor、Claude Code、Codex 等任意 Agent 直接读取你在 GuanHe 订阅的 Feed 和上传的文件，
          用最自然的一句话拿到当日动态、关键词搜索、Twitter / YouTube 日报。
          装好后无需记忆 API，直接问就行。
        </p>
      </div>

      {/* ── Tab bar ── */}
      <div
        style={{
          position: 'sticky',
          top: 0,
          zIndex: 10,
          background: dark ? DARK_BG : '#fff',
          borderBottom: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
          marginTop: 40,
        }}
      >
        <div
          style={{
            maxWidth: 760,
            margin: '0 auto',
            padding: '0 40px',
            display: 'flex',
            gap: 0,
          }}
        >
          {TABS.map((t) => {
            const active = activeTab === t.key;
            return (
              <button
                key={t.key}
                onClick={() => setActiveTab(t.key)}
                style={{
                  background: 'none',
                  border: 'none',
                  borderBottom: active ? `2px solid ${TEAL}` : '2px solid transparent',
                  padding: '14px 20px',
                  cursor: 'pointer',
                  textAlign: 'left',
                  marginBottom: -1,
                  transition: 'all .15s',
                }}
              >
                <div
                  style={{
                    fontSize: 14,
                    fontWeight: active ? 700 : 500,
                    color: active ? TEAL : dark ? '#94a3b8' : '#64748b',
                    lineHeight: 1.3,
                  }}
                >
                  {t.label}
                </div>
                <div style={{ fontSize: 11, color: dark ? '#475569' : '#94a3b8', marginTop: 1 }}>
                  {t.sub}
                </div>
              </button>
            );
          })}
        </div>
      </div>

      {/* ── Tab content ── */}
      <div style={{ maxWidth: 760, margin: '0 auto', padding: '40px 40px 80px' }}>
        {activeTab === 'skill' && (
          <div>
            <SectionLabel dark={dark}>Skill · 任意 Agent · SKILL.md 标准</SectionLabel>
            <SectionTitle dark={dark}>一份 Skill，任何 Agent 都能用</SectionTitle>
            <SectionDesc dark={dark}>
              SKILL.md 标准格式，跨 Cursor、Claude Code、Codex CLI 等任意 Agent 平台都能装。
              装好后用最自然的中文一句话拿到 GuanHe 数据，不需要 API Key、不需要手动配 MCP。
            </SectionDesc>

            <h3 style={{ fontSize: 16, fontWeight: 700, margin: '0 0 12px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              安装
            </h3>
            <p style={{ fontSize: 14, color: dark ? '#94a3b8' : '#475569', margin: '0 0 10px' }}>
              在你的 Agent 里直接发这句话，Agent 会自己装到对应目录，不用你操心路径：
            </p>
            <CodeBlock dark={dark} label="中文（推荐）">
              {installZh}
            </CodeBlock>
            <div style={{ height: 10 }} />
            <CodeBlock dark={dark} label="English">
              {installEn}
            </CodeBlock>

            <div style={{ marginTop: 12, display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
              <a
                href="/agent/skill/SKILL.md"
                target="_blank"
                rel="noreferrer"
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 5,
                  fontSize: 13,
                  color: TEAL,
                  textDecoration: 'none',
                }}
              >
                <LinkOutlined /> 查看 SKILL.md 原文
              </a>
            </div>

            <Divider dark={dark} />

            <h3 style={{ fontSize: 16, fontWeight: 700, margin: '0 0 16px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              触发示例
            </h3>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {[
                '今天有什么值得关注的？',
                '帮我看看最近一周的 Daily',
                '搜索我关注的内容里有没有关于英伟达的',
                '我上传的文件里有什么新东西',
                '只看 Twitter 上的内容',
                '帮我看看今天的 Twitter 日报',
                '最新的 YouTube 日报是什么',
              ].map((t) => (
                <PromptItem key={t} text={t} dark={dark} />
              ))}
            </div>

            <Divider dark={dark} />

            <h3 style={{ fontSize: 16, fontWeight: 700, margin: '0 0 12px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              Skill 内部能做的事
            </h3>
            <p style={{ fontSize: 14, color: dark ? '#94a3b8' : '#475569', margin: '0 0 16px' }}>
              Skill 根据用户意图智能分流端点。<strong style={{ color: dark ? '#e2e8f0' : '#1e293b' }}>默认走时间线</strong>；
              用户明确说"上传的文件"才加 <code style={{ fontSize: 12, color: TEAL }}>sources=pdf</code>，
              明确说"日报"才调日报端点。
            </p>
            <IntentTable rows={intentRows} dark={dark} />
          </div>
        )}

        {activeTab === 'mcp' && (
          <div>
            <SectionLabel dark={dark}>MCP · Cursor / Claude · stdio</SectionLabel>
            <SectionTitle dark={dark}>原生 MCP 工具调用</SectionTitle>
            <SectionDesc dark={dark}>
              对于支持 MCP 的客户端（Cursor、Claude Desktop），运行仓库里的{' '}
              <code style={{ fontSize: 13, color: TEAL }}>guanhe-mcp</code> stdio 服务，
              可以直接在 Agent 面板里点名调用任何工具。
            </SectionDesc>

            <h3 style={{ fontSize: 16, fontWeight: 700, margin: '0 0 12px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              第一步：安装 Python 包
            </h3>
            <CodeBlock dark={dark} label="在你的 Python 环境里执行">
              {`# 切到项目目录\ncd /path/to/guanhetech\n\n# 安装（rcTool 环境为例）\npip install httpx\npip install -e tools/guanhe-mcp`}
            </CodeBlock>

            <div style={{ height: 28 }} />

            <h3 style={{ fontSize: 16, fontWeight: 700, margin: '0 0 12px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              第二步：配置 Cursor mcp.json
            </h3>
            <p style={{ fontSize: 14, color: dark ? '#94a3b8' : '#475569', margin: '0 0 10px' }}>
              编辑 <code style={{ color: TEAL, fontSize: 13 }}>~/.cursor/mcp.json</code>，
              <code style={{ color: TEAL, fontSize: 13 }}>command</code> 必须填你实际的 Python 绝对路径：
            </p>
            <CodeBlock dark={dark} label="~/.cursor/mcp.json">
              {mcpJson}
            </CodeBlock>

            <div style={{ height: 28 }} />

            <h3 style={{ fontSize: 16, fontWeight: 700, margin: '0 0 12px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              可用工具
            </h3>
            <IntentTable
              dark={dark}
              rows={[
                { intent: 'daily_feed', endpoint: '获取每日时间线（Feeds + 上传文件合并）' },
                { intent: 'search', endpoint: '全文关键词搜索' },
                { intent: 'daily_item', endpoint: '按 id 获取单条内容' },
                { intent: 'twitter_daily_latest', endpoint: '最新 Twitter 日报' },
                { intent: 'twitter_daily_by_date', endpoint: '指定日期的 Twitter 日报' },
                { intent: 'twitter_daily_list', endpoint: '列出所有 Twitter 日报' },
                { intent: 'youtube_daily_latest', endpoint: '最新 YouTube 日报' },
                { intent: 'youtube_daily_by_date', endpoint: '指定日期的 YouTube 日报' },
              ]}
            />

            <Divider dark={dark} />

            {/* Notice */}
            <div style={{ borderLeft: `3px solid ${dark ? DARK_BORDER : '#e2e8f0'}`, paddingLeft: 16 }}>
              <p style={{ margin: '0 0 10px', fontSize: 14, color: dark ? '#94a3b8' : '#475569', lineHeight: 1.6 }}>
                <strong style={{ color: dark ? '#e2e8f0' : '#1e293b' }}>GUANHE_TOKEN</strong> 填从「用户菜单 → API Keys」生成的 PAT（以 <code style={{ color: TEAL, fontSize: 12 }}>ght_</code> 开头）。
              </p>
              <p style={{ margin: 0, fontSize: 14, color: dark ? '#94a3b8' : '#475569', lineHeight: 1.6 }}>
                <strong style={{ color: dark ? '#e2e8f0' : '#1e293b' }}>GUANHE_BASE_URL</strong> 填 API 服务地址，结尾不加斜杠，如{' '}
                <code style={{ color: TEAL, fontSize: 12 }}>{apiOrigin || 'http://localhost:8000'}</code>。
              </p>
            </div>
          </div>
        )}

        {activeTab === 'http' && (
          <div>
            <SectionLabel dark={dark}>REST API · 开发者 · OpenAPI</SectionLabel>
            <SectionTitle dark={dark}>直接对接 JSON API</SectionTitle>
            <SectionDesc dark={dark}>
              Agent API 全部只读，路径前缀{' '}
              <code style={{ fontSize: 14, color: TEAL }}>/api/v1/agent</code>，
              携带 PAT 即可调用，无需登录 Cookie。
            </SectionDesc>

            <h3 style={{ fontSize: 16, fontWeight: 700, margin: '0 0 12px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              鉴权
            </h3>
            <CodeBlock dark={dark} label="HTTP Header">
              {`Authorization: Bearer <PAT>`}
            </CodeBlock>

            <div style={{ height: 28 }} />

            <h3 style={{ fontSize: 16, fontWeight: 700, margin: '0 0 16px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              端点速查
            </h3>
            <IntentTable
              dark={dark}
              rows={[
                { intent: 'GET /api/v1/agent/daily', endpoint: '每日时间线，支持 since / sources / ticker / q / limit / cursor' },
                { intent: 'GET /api/v1/agent/daily/{id}', endpoint: '单条内容，id 形如 upload_123 / feed_456' },
                { intent: 'GET /api/v1/agent/search?q=…', endpoint: '全文搜索，支持 since / sources / ticker' },
                { intent: 'GET /api/v1/agent/reports/twitter-daily/latest', endpoint: '最新 Twitter 日报' },
                { intent: 'GET /api/v1/agent/reports/twitter-daily/YYYY-MM-DD', endpoint: '指定日期 Twitter 日报' },
                { intent: 'GET /api/v1/agent/reports/twitter-daily', endpoint: '日报列表（分页）' },
                { intent: 'GET /api/v1/agent/reports/youtube-daily/…', endpoint: '同上，YouTube 版' },
              ]}
            />

            <div style={{ height: 28 }} />

            <h3 style={{ fontSize: 16, fontWeight: 700, margin: '0 0 12px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              返回结构示例
            </h3>
            <CodeBlock dark={dark} label="单条 daily 对象">
              {`{
  "id": "feed_12345",
  "title": "NVIDIA 发布新架构",
  "summary": "…",
  "url": "https://…",
  "source": "twitter",
  "author": "@handle",
  "published_at": "2026-05-13T04:00:00+00:00",
  "tickers": ["NVDA.US"],
  "tags": ["AI", "Chips"]
}`}
            </CodeBlock>

            <div style={{ height: 12 }} />
            <p style={{ fontSize: 13, color: dark ? '#64748b' : '#94a3b8', lineHeight: 1.6 }}>
              <code style={{ color: TEAL, fontSize: 12 }}>source</code> 可能值：
              twitter · youtube · web · news · podcast · pdf · image · audio · text。
              默认限速 <strong style={{ color: dark ? '#cbd5e1' : '#475569' }}>60 次 / 分钟</strong> / PAT。
            </p>
          </div>
        )}

        {/* ── Footer notice (all tabs) ── */}
        <Divider dark={dark} />

        <div
          style={{
            fontSize: 12,
            lineHeight: 1.7,
            color: dark ? '#475569' : '#94a3b8',
          }}
        >
          <div style={{ marginBottom: 8, fontWeight: 600, color: dark ? '#64748b' : '#64748b', textTransform: 'uppercase', letterSpacing: '0.07em' }}>
            使用须知
          </div>
          <p style={{ margin: '0 0 6px' }}>
            <strong style={{ color: dark ? '#94a3b8' : '#475569' }}>安全</strong>
            {' '}— PAT 等同于密码，不要分享或提交到代码仓库。可在「用户菜单 → {''}
            <button
              onClick={() => navigate('/settings/api-keys')}
              style={{
                background: 'none',
                border: 'none',
                padding: 0,
                cursor: 'pointer',
                color: TEAL,
                fontSize: 12,
                textDecoration: 'underline',
              }}
            >
              API Keys
            </button>
            」随时创建或撤销。
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: dark ? '#94a3b8' : '#475569' }}>原文为准</strong>
            {' '}— 摘要由 LLM 生成，引用前请用{' '}
            <code style={{ fontSize: 11, color: TEAL }}>url</code> 字段回原文核对。
          </p>
        </div>
      </div>
    </div>
  );
};

export default AgentIntegrationPage;

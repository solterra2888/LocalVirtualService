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

const SectionLabel: React.FC<{ children: React.ReactNode }> = ({ children }) => (
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

const StepHeading: React.FC<{ step: number; title: string; dark: boolean; badge?: string }> = ({ step, title, dark, badge }) => (
  <div style={{ display: 'flex', alignItems: 'center', gap: 10, margin: '0 0 12px' }}>
    <div style={{
      width: 26, height: 26, borderRadius: '50%',
      background: TEAL, color: '#fff',
      fontSize: 13, fontWeight: 700,
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      flexShrink: 0,
    }}>
      {step}
    </div>
    <h3 style={{ fontSize: 16, fontWeight: 700, margin: 0, color: dark ? '#e2e8f0' : '#1e293b' }}>
      {title}
    </h3>
    {badge && (
      <span style={{
        fontSize: 11, fontWeight: 600, padding: '2px 8px',
        borderRadius: 99, border: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
        color: dark ? '#64748b' : '#94a3b8',
      }}>
        {badge}
      </span>
    )}
  </div>
);

// ─── Tabs ─────────────────────────────────────────────────────────────────────

type TabKey = 'mcp' | 'http';

const TABS: { key: TabKey; label: string; sub: string }[] = [
  { key: 'mcp', label: 'MCP + Skill', sub: 'Cursor / Claude · 推荐方式' },
  { key: 'http', label: 'REST API', sub: '开发者 / 自定义集成' },
];

// ─── Page ─────────────────────────────────────────────────────────────────────

const AgentIntegrationPage: React.FC = () => {
  const navigate = useNavigate();
  const { themeMode } = useDashboardSettings();
  const dark = themeMode === 'dark';

  const [activeTab, setActiveTab] = useState<TabKey>('mcp');

  const origin = typeof window !== 'undefined' ? window.location.origin : '';
  const skillUrl = `${origin}/agent/skill/GUANHE_DAILY.md`;
  const apiOrigin = suggestedApiOrigin();

  const installZh = useMemo(() => `帮我安装这个 skill：${skillUrl}`, [skillUrl]);
  const installEn = useMemo(() => `Install this skill: ${skillUrl}`, [skillUrl]);

  const mcpJson = useMemo(
    () =>
      JSON.stringify(
        {
          mcpServers: {
            guanhe: {
              command: '/path/to/your/python',
              args: ['-m', 'guanhe_mcp.server'],
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
    { intent: '今天有什么值得关注的？', endpoint: 'daily_feed  since=24h' },
    { intent: '这周的内容帮我总结一下', endpoint: 'daily_feed  since=7d（默认）' },
    { intent: '帮我找找关于 NVDA 的内容', endpoint: 'search  q=NVDA' },
    { intent: '只看我上传的文件', endpoint: 'daily_feed  sources=pdf,image,text' },
    { intent: '只看 Twitter 的内容', endpoint: 'daily_feed  sources=twitter' },
    { intent: '看看今天的 Twitter 日报', endpoint: 'twitter_daily_latest' },
    { intent: '5 月 10 号的 Twitter 日报', endpoint: 'twitter_daily_by_date  date=2026-05-10' },
    { intent: '最新的 YouTube 日报', endpoint: 'youtube_daily_latest' },
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

        {/* ══════════════ MCP + Skill tab ══════════════ */}
        {activeTab === 'mcp' && (
          <div>
            <SectionLabel>MCP · Cursor / Claude · stdio</SectionLabel>
            <SectionTitle dark={dark}>三步接入，自然语言提问</SectionTitle>
            <SectionDesc dark={dark}>
              MCP（Model Context Protocol）让 Cursor / Claude Desktop 以原生工具调用方式读取你的 GuanHe 数据。
              完成前三步后再可选安装 Skill，实现自然语言自动路由。
            </SectionDesc>

            {/* Step 1 */}
            <StepHeading step={1} title="获取 Personal Access Token (PAT)" dark={dark} />
            <p style={{ fontSize: 14, color: dark ? '#94a3b8' : '#475569', margin: '0 0 12px' }}>
              在「用户菜单 →{' '}
              <button
                onClick={() => navigate('/settings/api-keys')}
                style={{ background: 'none', border: 'none', padding: 0, cursor: 'pointer', color: TEAL, fontSize: 14, textDecoration: 'underline' }}
              >
                API Keys
              </button>
              」页面生成一个 PAT，复制以{' '}
              <code style={{ fontSize: 12, color: TEAL }}>ght_</code> 开头的字符串备用。
            </p>

            <div style={{ height: 32 }} />

            {/* Step 2 */}
            <StepHeading step={2} title="安装 guanhe-mcp 包" dark={dark} />
            <p style={{ fontSize: 14, color: dark ? '#94a3b8' : '#475569', margin: '0 0 10px' }}>
              在<strong style={{ color: dark ? '#e2e8f0' : '#1e293b' }}>本地 Cursor / Claude Desktop 所在机器</strong>上执行。
              直接从服务器下载安装，无需 GitHub 访问权限：
            </p>
            <CodeBlock dark={dark} label="终端">
              {`pip install "${apiOrigin || 'https://your-api-host'}/downloads/guanhe-mcp.whl"`}
            </CodeBlock>
            <p style={{ fontSize: 13, color: dark ? '#64748b' : '#94a3b8', margin: '8px 0 0' }}>
              安装后执行 <code style={{ fontSize: 12, color: TEAL }}>which python</code> 记下 Python 绝对路径，下一步需要用。
            </p>

            <div style={{ height: 32 }} />

            {/* Step 3 */}
            <StepHeading step={3} title="配置 Cursor mcp.json" dark={dark} />
            <p style={{ fontSize: 14, color: dark ? '#94a3b8' : '#475569', margin: '0 0 10px' }}>
              编辑 <code style={{ color: TEAL, fontSize: 13 }}>~/.cursor/mcp.json</code>，将{' '}
              <code style={{ color: TEAL, fontSize: 13 }}>command</code> 替换为上一步查到的 Python 绝对路径
              （Cursor 子进程不继承 PATH，必须填绝对路径）：
            </p>
            <CodeBlock dark={dark} label="~/.cursor/mcp.json">
              {mcpJson}
            </CodeBlock>
            <div style={{
              marginTop: 12,
              padding: '10px 14px',
              borderRadius: 8,
              background: dark ? DARK_SURFACE : '#f8fafc',
              border: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
              fontSize: 13.5,
              color: dark ? '#94a3b8' : '#475569',
              lineHeight: 1.6,
            }}>
              <strong style={{ color: dark ? '#e2e8f0' : '#1e293b' }}>GUANHE_TOKEN</strong>
              {' '}— 填第一步复制的 PAT（<code style={{ fontSize: 12, color: TEAL }}>ght_…</code>）
              <br />
              <strong style={{ color: dark ? '#e2e8f0' : '#1e293b' }}>GUANHE_BASE_URL</strong>
              {' '}— 服务公网地址，不加结尾斜杠，如{' '}
              <code style={{ fontSize: 12, color: TEAL }}>{apiOrigin || 'https://your-host'}</code>
            </div>
            <p style={{ fontSize: 13, color: dark ? '#64748b' : '#94a3b8', margin: '10px 0 0' }}>
              保存后<strong>完全退出 Cursor 再重开</strong>（Reload Window 不会重启 MCP 子进程）。
              在 MCP 面板确认 <code style={{ fontSize: 12, color: TEAL }}>guanhe</code> 绿勾后即可直接调用工具。
            </p>

            <Divider dark={dark} />

            {/* 数据范围说明 */}
            <h3 style={{ fontSize: 16, fontWeight: 700, margin: '0 0 12px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              数据范围
            </h3>
            <p style={{ fontSize: 14, color: dark ? '#94a3b8' : '#475569', margin: '0 0 12px', lineHeight: 1.65 }}>
              MCP 工具只返回当前 PAT 所属账号有权限访问的数据，包含以下三类：
            </p>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8, marginBottom: 24 }}>
              {[
                { icon: '📁', title: '自己上传的文件', desc: 'PDF、图片、音频、文档等，经平台处理后的内容' },
                { icon: '🔗', title: '他人分享给你的文件', desc: '其他用户明确分享到你账号的上传内容' },
                { icon: '📡', title: '已启用的 Feed 订阅', desc: '订阅列表中状态为「已启用」的 Twitter、YouTube、新闻等频道；已停用（Disabled）的订阅不在结果中' },
              ].map(({ icon, title, desc }) => (
                <div key={title} style={{
                  display: 'flex', gap: 12, padding: '12px 14px',
                  borderRadius: 8,
                  border: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
                  background: dark ? DARK_SURFACE : '#f8fafc',
                }}>
                  <span style={{ fontSize: 18, lineHeight: 1.4 }}>{icon}</span>
                  <div>
                    <div style={{ fontSize: 14, fontWeight: 600, color: dark ? '#e2e8f0' : '#1e293b', marginBottom: 2 }}>{title}</div>
                    <div style={{ fontSize: 13, color: dark ? '#94a3b8' : '#475569', lineHeight: 1.5 }}>{desc}</div>
                  </div>
                </div>
              ))}
            </div>

            <Divider dark={dark} />

            {/* 可用工具 */}
            <h3 style={{ fontSize: 16, fontWeight: 700, margin: '0 0 12px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              可用工具
            </h3>
            <IntentTable
              dark={dark}
              rows={[
                { intent: 'daily_feed', endpoint: '获取每日时间线（Feeds + 上传文件合并）' },
                { intent: 'search', endpoint: '全文关键词搜索' },
                { intent: 'twitter_daily_latest', endpoint: '最新 Twitter 日报' },
                { intent: 'twitter_daily_by_date', endpoint: '指定日期的 Twitter 日报' },
                { intent: 'twitter_daily_list', endpoint: '列出所有 Twitter 日报' },
                { intent: 'youtube_daily_latest', endpoint: '最新 YouTube 日报' },
                { intent: 'youtube_daily_by_date', endpoint: '指定日期的 YouTube 日报' },
              ]}
            />

            <Divider dark={dark} />

            {/* Step 4 optional: Skill */}
            <StepHeading step={4} title="安装 Skill（可选）— 自然语言路由" dark={dark} badge="可选" />
            <p style={{ fontSize: 14, color: dark ? '#94a3b8' : '#475569', margin: '0 0 12px', lineHeight: 1.65 }}>
              Skill 是运行在 MCP 之上的语义路由层——它告诉 Agent「用户说"今天有啥新闻"时应该调用哪个工具、传什么参数」。
              装完后你无需记忆工具名，直接用中文提问即可。
            </p>
            <p style={{ fontSize: 14, color: dark ? '#94a3b8' : '#475569', margin: '0 0 10px' }}>
              在 Agent 对话里发这句话，Agent 会自动下载并安装：
            </p>
            <CodeBlock dark={dark} label="中文（推荐）">
              {installZh}
            </CodeBlock>
            <div style={{ height: 10 }} />
            <CodeBlock dark={dark} label="English">
              {installEn}
            </CodeBlock>
            <div style={{ marginTop: 10, display: 'flex', alignItems: 'center', gap: 8 }}>
              <a
                href="/agent/skill/GUANHE_DAILY.md"
                target="_blank"
                rel="noreferrer"
                style={{ display: 'inline-flex', alignItems: 'center', gap: 5, fontSize: 13, color: TEAL, textDecoration: 'none' }}
              >
                <LinkOutlined /> 查看 GUANHE_DAILY.md 原文
              </a>
            </div>

            <div style={{ height: 32 }} />

            {/* 自然语言路由表 */}
            <h4 style={{ fontSize: 14, fontWeight: 700, margin: '0 0 10px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              自然语言 → 工具路由
            </h4>
            <p style={{ fontSize: 13, color: dark ? '#64748b' : '#94a3b8', margin: '0 0 12px' }}>
              Skill 根据用户语义自动选择工具和参数，用户无需知道工具名称。
            </p>
            <IntentTable rows={intentRows} dark={dark} />

            <div style={{ height: 32 }} />

            {/* daily_feed 参数详情 */}
            <h4 style={{ fontSize: 14, fontWeight: 700, margin: '0 0 10px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              daily_feed / search 常用参数
            </h4>
            <IntentTable
              dark={dark}
              rows={[
                { intent: 'since', endpoint: '时间窗口：24h / 7d / 30d / ISO 日期。默认 7d' },
                { intent: 'sources', endpoint: '来源过滤（逗号分隔），见下方来源类型表' },
                { intent: 'q', endpoint: '关键词过滤，如 NVDA、英伟达' },
                { intent: 'ticker', endpoint: '股票代码过滤，如 NVDA' },
                { intent: 'limit', endpoint: '返回条数，默认 20，最大 100' },
                { intent: 'cursor', endpoint: '翻页游标，从上次返回的 next_cursor 获取' },
              ]}
            />

            <div style={{ height: 32 }} />

            {/* sources 类型参考 */}
            <h4 style={{ fontSize: 14, fontWeight: 700, margin: '0 0 10px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              sources= 参数可用值
            </h4>
            <IntentTable
              dark={dark}
              rows={[
                { intent: 'twitter', endpoint: '订阅的 Twitter Feed' },
                { intent: 'youtube / youtube_feed', endpoint: '订阅的 YouTube Feed' },
                { intent: 'web / news / podcast', endpoint: '订阅的网页 / 新闻 / 播客 Feed' },
                { intent: 'pdf', endpoint: '用户上传的 PDF' },
                { intent: 'image', endpoint: '用户上传的图片' },
                { intent: 'audio', endpoint: '用户上传的音频' },
                { intent: 'text', endpoint: '用户上传的文本文件' },
              ]}
            />

            <div style={{ height: 32 }} />

            {/* 回答格式指引 */}
            <h4 style={{ fontSize: 14, fontWeight: 700, margin: '0 0 10px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              回答格式指引
            </h4>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {[
                '用 3–8 条要点，每条格式：标题 — 一句话摘要（来源）[链接]',
                '提到投资标的时，只引用返回数据中 tickers 字段的内容，不自行推断股票代码',
                '内容较多时，先给整体概述，再分类展开（「上传文件」「Twitter」「YouTube」各自有什么）',
                '没有查到内容时，如实告知并建议用户检查订阅设置或扩大时间范围',
              ].map((tip, i) => (
                <div key={i} style={{
                  display: 'flex', gap: 10, padding: '10px 14px',
                  borderRadius: 8,
                  border: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
                  fontSize: 13.5,
                  color: dark ? '#cbd5e1' : '#334155',
                  lineHeight: 1.55,
                }}>
                  <span style={{ color: TEAL, fontWeight: 700, flexShrink: 0 }}>{i + 1}.</span>
                  <span>{tip}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ══════════════ REST API tab ══════════════ */}
        {activeTab === 'http' && (
          <div>
            <SectionLabel>REST API · 开发者 · OpenAPI</SectionLabel>
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
            {' '}— PAT 等同于密码，不要分享或提交到代码仓库。可在「用户菜单 →{' '}
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

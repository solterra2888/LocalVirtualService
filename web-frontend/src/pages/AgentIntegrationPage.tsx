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

type IntentRow = { intent: string; endpoint: string; params?: string };
const IntentTable: React.FC<{
  rows: IntentRow[];
  dark: boolean;
  col1?: string;
  col2?: string;
  col3?: string;
}> = ({ rows, dark, col1 = '用户怎么说', col2 = '调用的端点', col3 }) => (
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
            {col1}
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
            {col2}
          </th>
          {col3 && (
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
              {col3}
            </th>
          )}
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
            {col3 && (
              <td
                style={{
                  padding: '10px 14px',
                  fontFamily: 'ui-monospace, SFMono-Regular, monospace',
                  fontSize: 12,
                  color: dark ? '#94a3b8' : '#64748b',
                  lineHeight: 1.5,
                }}
              >
                {r.params ?? '—'}
              </td>
            )}
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

  /** 与 docs/agent/GUANHE_DAILY.md「典型用户问法 → 工具映射」一致 */
  const skillRoutingRows: IntentRow[] = [
    { intent: '今天有什么值得关注的？最近有什么新动态？', endpoint: 'daily_feed', params: 'since=24h' },
    { intent: '这周的信息帮我总结一下', endpoint: 'daily_feed', params: 'since=7d（默认，可省略）' },
    { intent: '帮我找找关于 NVDA / 英伟达的内容', endpoint: 'search', params: 'q=NVDA' },
    { intent: '我上传的文件里有没有关于 XX 的内容', endpoint: 'search', params: 'q=XX, sources=pdf' },
    { intent: '只看 Twitter 上的内容', endpoint: 'daily_feed', params: 'sources=twitter' },
    { intent: '只看我上传的文件', endpoint: 'daily_feed', params: 'sources=pdf,image,text,audio' },
    {
      intent: '这条具体讲了什么 / 展开第 N 条 / 给我看原文',
      endpoint: 'daily_item',
      params: 'id（取上一轮列表里的 id）',
    },
    {
      intent: 'Bloomberg / YouTube 视频的 transcript 是什么',
      endpoint: 'daily_item',
      params: 'id，读返回里的 transcript 字段',
    },
    {
      intent: '那份 PDF / 我上传的研报里关于 XX 怎么说',
      endpoint: 'search → daily_item',
      params: '先 q=XX 拿 id → daily_item',
    },
    { intent: '帮我看看今天的 Twitter 日报', endpoint: 'twitter_daily_latest', params: '—' },
    { intent: '帮我看看 5 月 10 号的 Twitter 日报', endpoint: 'twitter_daily_by_date', params: 'date=2026-05-10' },
    { intent: '帮我看看最新的 YouTube 日报', endpoint: 'youtube_daily_latest', params: '—' },
    { intent: '有哪些日报可以看？', endpoint: 'twitter_daily_list', params: '—' },
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
              {`pip install "${apiOrigin || 'https://your-api-host'}/downloads/guanhe_mcp-0.1.0-py3-none-any.whl"`}
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

            <h4 style={{ fontSize: 14, fontWeight: 700, margin: '20px 0 10px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              用户使用场景
            </h4>
            <p style={{ fontSize: 14, color: dark ? '#94a3b8' : '#475569', margin: '0 0 24px', lineHeight: 1.65 }}>
              在 GuanHe 网页端完成「订阅 Feed」与「上传文件」后，内容会合并进每日 Daily 时间线。
              你可以直接用自然语言向 Agent 提问，无需知道任何 API 或工具名称。
            </p>

            <Divider dark={dark} />

            {/* 可用工具 */}
            <h3 style={{ fontSize: 16, fontWeight: 700, margin: '0 0 12px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              可用工具
            </h3>
            <IntentTable
              dark={dark}
              col1="工具"
              col2="说明"
              rows={[
                { intent: 'daily_feed', endpoint: '获取每日时间线（Feed 订阅 + 上传文件合并）' },
                { intent: 'search', endpoint: '在所有内容（标题、摘要、正文）中做全文关键词搜索' },
                {
                  intent: 'daily_item',
                  endpoint: '按 id 取单条完整字段，用于追问原文、字幕、全文；id 来自上一轮 daily_feed / search 列表',
                },
                { intent: 'twitter_daily_latest', endpoint: '最新 Twitter 日报' },
                { intent: 'twitter_daily_by_date', endpoint: '指定日期的 Twitter 日报（date=YYYY-MM-DD）' },
                { intent: 'twitter_daily_list', endpoint: '列出可查看的 Twitter 日报' },
                { intent: 'youtube_daily_latest', endpoint: '最新 YouTube 日报' },
                { intent: 'youtube_daily_by_date', endpoint: '指定日期的 YouTube 日报（date=YYYY-MM-DD）' },
              ]}
            />

            <Divider dark={dark} />

            {/* Step 4 optional: Skill */}
            <StepHeading step={4} title="安装 Skill（可选）— 自然语言路由" dark={dark} badge="可选" />
            <p style={{ fontSize: 14, color: dark ? '#94a3b8' : '#475569', margin: '0 0 12px', lineHeight: 1.65 }}>
              Skill 是运行在 MCP 之上的语义路由层——它告诉 Agent「用户说"今天有啥新闻"时应该调用哪个工具、传什么参数」。
              装完后你无需记忆工具名，直接用中文提问即可。
              须已完成上文步骤 1–3，且 MCP 中 <code style={{ fontSize: 12, color: TEAL }}>guanhe</code> 可用；Skill 通过{' '}
              <code style={{ fontSize: 12, color: TEAL }}>guanhe-mcp</code> 执行实际数据请求。
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
            <IntentTable
              rows={skillRoutingRows}
              dark={dark}
              col1="用户会这样说"
              col2="应调用的工具"
              col3="关键参数"
            />

            <div
              style={{
                marginTop: 16,
                padding: '12px 14px',
                borderRadius: 8,
                borderLeft: `3px solid ${TEAL}`,
                background: dark ? DARK_SURFACE : '#f0fdfa',
                fontSize: 13.5,
                color: dark ? '#94a3b8' : '#475569',
                lineHeight: 1.65,
              }}
            >
              <strong style={{ color: dark ? '#e2e8f0' : '#0f766e' }}>重要</strong>
              ：用户不会说「调用 daily_feed」——只会用日常中文提问；由 AI 助手根据语义自动选工具与参数。
            </div>

            <div style={{ height: 28 }} />

            <h4 style={{ fontSize: 14, fontWeight: 700, margin: '0 0 10px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              列表 → 详情下钻（核心）
            </h4>
            <ol style={{ margin: '0 0 0 18px', padding: 0, fontSize: 14, color: dark ? '#94a3b8' : '#475569', lineHeight: 1.75 }}>
              <li style={{ marginBottom: 8 }}>
                第一轮 <code style={{ fontSize: 12, color: TEAL }}>daily_feed</code> /{' '}
                <code style={{ fontSize: 12, color: TEAL }}>search</code> 返回轻量列表（标题、摘要、链接、<strong style={{ color: dark ? '#e2e8f0' : '#1e293b' }}>id</strong>、tickers/tags）。
              </li>
              <li style={{ marginBottom: 8 }}>
                Agent 把列表展示给你时，应记住每条对应的 <code style={{ fontSize: 12, color: TEAL }}>id</code>（用户不需要手填 id）。
              </li>
              <li style={{ marginBottom: 8 }}>
                当你追问「这条具体讲了什么 / 全文 / 字幕」时，应用上一轮列表里的 <code style={{ fontSize: 12, color: TEAL }}>id</code> 调{' '}
                <code style={{ fontSize: 12, color: TEAL }}>daily_item</code>，而不是重复大范围检索。
              </li>
              <li>
                若对话过长已丢失 id，先用 <code style={{ fontSize: 12, color: TEAL }}>search</code> 关键词重定位拿到 id，再{' '}
                <code style={{ fontSize: 12, color: TEAL }}>daily_item</code>。
              </li>
            </ol>
            <p style={{ fontSize: 13, color: dark ? '#64748b' : '#94a3b8', margin: '12px 0 0', lineHeight: 1.6 }}>
              <strong style={{ color: dark ? '#94a3b8' : '#64748b' }}>何时下钻</strong>
              ：列表里的 <code style={{ fontSize: 12, color: TEAL }}>summary</code> 不足以回答（要原文、时间戳、数字细节、原话）时，应调用{' '}
              <code style={{ fontSize: 12, color: TEAL }}>daily_item</code>。
            </p>

            <div style={{ height: 32 }} />

            {/* daily_feed 参数详情 */}
            <h4 style={{ fontSize: 14, fontWeight: 700, margin: '0 0 10px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              daily_feed / search 常用参数
            </h4>
            <IntentTable
              dark={dark}
              col1="参数"
              col2="说明"
              rows={[
                { intent: 'since', endpoint: '时间窗口：24h / 7d / 30d / ISO 日期时间。默认 7d' },
                { intent: 'sources', endpoint: '逗号分隔来源子类型；不填则使用网页端默认 preset。见下方来源类型表' },
                {
                  intent: 'q',
                  endpoint:
                    'search：必填，全文搜索关键词。daily_feed：可选，在已有时间线结果上再做关键词过滤（与 search 语义不同）',
                },
                { intent: 'ticker', endpoint: '按股票代码过滤，如 NVDA' },
                { intent: 'limit', endpoint: '返回条数，默认 20，最大 100' },
                { intent: 'cursor', endpoint: '翻页游标，从上次返回的 next_cursor 获取' },
              ]}
            />

            <div style={{ height: 24 }} />

            <h4 style={{ fontSize: 14, fontWeight: 700, margin: '0 0 10px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              daily_item 要点
            </h4>
            <IntentTable
              dark={dark}
              col1="字段 / 参数"
              col2="说明"
              rows={[
                { intent: 'id（必填）', endpoint: '来自 daily_feed / search，如 feed_88247、upload_123' },
                {
                  intent: 'content',
                  endpoint: '主正文：上传类为解析/Markdown；推文/新闻为全文；YouTube 订阅为视频描述等',
                },
                {
                  intent: 'transcript',
                  endpoint: '字幕或转写文本；无字幕的类型为 null；上传音视频可能来自 ASR 等',
                },
                {
                  intent: 'content_truncated / transcript_truncated',
                  endpoint: '为 true 时表示已截断，应引导用户打开 url 查看完整内容，勿假装已见全文',
                },
                { intent: 'body_excerpt', endpoint: '向后兼容字段；新逻辑优先使用 content' },
              ]}
            />

            <div style={{ height: 32 }} />

            {/* sources 类型参考 */}
            <h4 style={{ fontSize: 14, fontWeight: 700, margin: '0 0 10px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              sources= 参数可用值
            </h4>
            <IntentTable
              dark={dark}
              col1="source 值"
              col2="含义"
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
            <p style={{ fontSize: 13, color: dark ? '#64748b' : '#94a3b8', margin: '0 0 12px' }}>
              列表概览与追问下钻的呈现方式（与文档「回答格式指引」一致）：
            </p>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {[
                '用 3–8 条要点，每条格式：标题 — 一句话摘要（来源）[链接]',
                '提到投资标的时，只引用返回数据中 tickers 字段的内容，不自行推断股票代码',
                '内容较多时，先给整体概述，再分类展开（「上传文件」「Twitter」「YouTube」各自有什么）',
                '没有查到内容时，如实告知并建议用户检查订阅设置或扩大时间范围',
                '用户对某条追问「具体讲了什么 / 全文 / 字幕」时，先调 daily_item 读取 content / transcript',
                '围绕 content / transcript 直接引用关键段落或原话，避免重复念一遍 summary',
                '引用 transcript 时尽量标注时间戳与讲话人，并附 url 便于核对',
                '若 content_truncated 或 transcript_truncated 为 true，明确说明「以上为节选，完整内容请打开链接」',
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
              col1="请求"
              col2="说明"
              rows={[
                { intent: 'GET /api/v1/agent/daily', endpoint: '每日时间线，支持 since / sources / ticker / q / limit / cursor' },
                {
                  intent: 'GET /api/v1/agent/daily/{item_id}',
                  endpoint: '单条详情（正文 content、字幕 transcript、截断标记等），item_id 同列表中的 id',
                },
                { intent: 'GET /api/v1/agent/search?q=…', endpoint: '全文搜索（q 必填），支持 since / sources / ticker / limit / cursor' },
                { intent: 'GET /api/v1/agent/reports/twitter-daily/latest', endpoint: '最新 Twitter 日报' },
                { intent: 'GET /api/v1/agent/reports/twitter-daily/YYYY-MM-DD', endpoint: '指定日期 Twitter 日报' },
                { intent: 'GET /api/v1/agent/reports/twitter-daily', endpoint: '日报列表（分页）' },
                { intent: 'GET /api/v1/agent/reports/youtube-daily/…', endpoint: '同上，YouTube 版（含 latest、按日期、列表）' },
              ]}
            />

            <div style={{ height: 28 }} />

            <h3 style={{ fontSize: 16, fontWeight: 700, margin: '0 0 12px', color: dark ? '#e2e8f0' : '#1e293b' }}>
              返回结构示例
            </h3>
            <CodeBlock dark={dark} label="列表条目（daily / search）">
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

            <div style={{ height: 16 }} />

            <CodeBlock dark={dark} label="详情条目（GET /daily/{item_id}）">
              {`{
  "id": "feed_88247",
  "title": "Horizons Middle East — Trump China Visit Special",
  "summary": "…",
  "url": "https://youtube.com/watch?v=…",
  "source": "youtube",
  "author": "Bloomberg Television",
  "published_at": "2026-05-13T05:30:00+00:00",
  "tickers": ["NVDA.US", "AAPL.US", "TSLA.US", "BLK.US"],
  "tags": ["China", "Tariffs", "CEO"],
  "content": "本期节目讨论特朗普访华代表团构成 …",
  "content_truncated": false,
  "transcript": "Welcome to Horizons Middle East …",
  "transcript_truncated": true,
  "body_excerpt": "本期节目讨论特朗普访华代表团构成 …"
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

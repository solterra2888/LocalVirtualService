import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Button, Checkbox, Form, Input, Modal, Select, message } from 'antd';
import { CopyOutlined, KeyOutlined, LinkOutlined, PlusOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

// ─── Visual tokens (aligned with AgentIntegrationPage) ───────────────────────

const TEAL = '#0d9488';
const DARK_BG = '#0f172a';
const DARK_SURFACE = '#1e293b';
const DARK_BORDER = '#334155';

type TokenRow = {
  id: number;
  name: string;
  prefix: string;
  scopes: string[];
  expires_at: string | null;
  last_used_at: string | null;
  revoked_at: string | null;
  created_at: string | null;
};

const SCOPE_OPTIONS = [
  { label: 'Daily feed & search (daily:read)', value: 'daily:read' },
  { label: 'Twitter / YouTube reports (reports:read)', value: 'reports:read' },
];

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
      message.error('复制失败');
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
        type="button"
        onClick={() => void handleCopy()}
        title={copied ? '已复制' : '复制'}
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

type QuickTab = 'claude' | 'cursor' | 'codex';

const QUICK_TABS: { key: QuickTab; label: string; sub: string }[] = [
  { key: 'claude', label: 'Claude', sub: 'Claude Code / Desktop' },
  { key: 'cursor', label: 'Cursor', sub: 'Cursor Agent 一句话安装' },
  { key: 'codex', label: 'Codex', sub: 'CLI 或兼容客户端' },
];

/** 完整 PAT 明文仅存于本浏览器 session（服务端只存 hash，无法再次下发） */
const PAT_SECRETS_STORAGE_KEY = 'guanhe_pat_secrets';

function loadPatSecrets(): Record<string, string> {
  if (typeof window === 'undefined') return {};
  try {
    const raw = sessionStorage.getItem(PAT_SECRETS_STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== 'object') return {};
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(parsed)) {
      if (typeof v === 'string' && v.startsWith('ght_')) out[k] = v;
    }
    return out;
  } catch {
    return {};
  }
}

function savePatSecret(tokenId: number, plaintext: string): void {
  if (typeof window === 'undefined' || !plaintext) return;
  const all = loadPatSecrets();
  all[String(tokenId)] = plaintext;
  sessionStorage.setItem(PAT_SECRETS_STORAGE_KEY, JSON.stringify(all));
}

function removePatSecret(tokenId: number): void {
  if (typeof window === 'undefined') return;
  const all = loadPatSecrets();
  delete all[String(tokenId)];
  sessionStorage.setItem(PAT_SECRETS_STORAGE_KEY, JSON.stringify(all));
}

function prunePatSecrets(activeIds: Set<number>): Record<string, string> {
  const all = loadPatSecrets();
  let changed = false;
  for (const key of Object.keys(all)) {
    const id = Number(key);
    if (!activeIds.has(id)) {
      delete all[key];
      changed = true;
    }
  }
  if (changed && typeof window !== 'undefined') {
    sessionStorage.setItem(PAT_SECRETS_STORAGE_KEY, JSON.stringify(all));
  }
  return all;
}

const CopyFullTokenButton: React.FC<{ token: string; dark: boolean }> = ({ token, dark }) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(token);
      setCopied(true);
      message.success('已复制完整令牌');
      setTimeout(() => setCopied(false), 1800);
    } catch {
      message.error('复制失败');
    }
  };

  return (
    <button
      type="button"
      title={copied ? '已复制' : '复制完整令牌（仅本浏览器创建后可用）'}
      onClick={() => void handleCopy()}
      style={{
        background: 'none',
        border: 'none',
        padding: '2px 4px',
        cursor: 'pointer',
        color: copied ? TEAL : dark ? '#475569' : '#94a3b8',
        fontSize: 12,
        display: 'inline-flex',
        alignItems: 'center',
        gap: 4,
        transition: 'color .15s',
        flexShrink: 0,
      }}
    >
      <CopyOutlined />
      <span style={{ fontSize: 11 }}>{copied ? '已复制' : '复制'}</span>
    </button>
  );
};

function formatTs(v: string | null): string {
  if (!v) return '—';
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? v : d.toLocaleString();
}

const ApiKeysPage: React.FC = () => {
  const navigate = useNavigate();
  const { themeMode } = useDashboardSettings();
  const dark = themeMode === 'dark';

  const token = useAuthStore((s) => s.token);
  const [rows, setRows] = useState<TokenRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [createOpen, setCreateOpen] = useState(false);
  const [createdPlaintext, setCreatedPlaintext] = useState<string | null>(null);
  const [patSecrets, setPatSecrets] = useState<Record<string, string>>(() => loadPatSecrets());
  const [showRevoked, setShowRevoked] = useState(false);
  const [form] = Form.useForm();
  const [quickTab, setQuickTab] = useState<QuickTab>('claude');

  const apiBase = useMemo(() => process.env.REACT_APP_API_BASE_URL || '/api', []);

  const revokedCount = useMemo(() => rows.filter((r) => r.revoked_at).length, [rows]);
  const visibleRows = useMemo(
    () => (showRevoked ? rows : rows.filter((r) => !r.revoked_at)),
    [rows, showRevoked]
  );

  const authHeaders = useCallback(() => {
    const t = token || localStorage.getItem('access_token');
    return {
      Authorization: `Bearer ${t}`,
      'Content-Type': 'application/json',
    } as Record<string, string>;
  }, [token]);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${apiBase}/users/me/tokens`, { headers: authHeaders() });
      if (!res.ok) throw new Error(await res.text());
      const json = await res.json();
      const list: TokenRow[] = json.data || [];
      setRows(list);
      const activeIds = new Set(
        list.filter((r) => !r.revoked_at).map((r) => r.id)
      );
      setPatSecrets(prunePatSecrets(activeIds));
    } catch (e) {
      message.error('加载令牌列表失败');
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, [apiBase, authHeaders]);

  useEffect(() => {
    void load();
  }, [load]);

  const origin = typeof window !== 'undefined' ? window.location.origin : '';
  const skillUrl = `${origin}/agent/skill/GUANHE_DAILY.md`;

  const quickInstall = useMemo(
    () => ({
      claude: `帮我安装这个 skill：${skillUrl}`,
      cursor: `Install this skill: ${skillUrl}`,
      codex: `Install this skill: ${skillUrl}`,
    }),
    [skillUrl]
  );

  const openCreate = () => {
    form.resetFields();
    form.setFieldsValue({
      name: 'Agent',
      scopes: ['daily:read', 'reports:read'],
      expires_days: 'never',
    });
    setCreatedPlaintext(null);
    setCreateOpen(true);
  };

  const submitCreate = async () => {
    try {
      const v = await form.validateFields();
      let expires_days: number | null = null;
      if (v.expires_days === '30') expires_days = 30;
      else if (v.expires_days === '90') expires_days = 90;
      const body = {
        name: v.name,
        scopes: v.scopes as string[],
        expires_days,
      };
      const res = await fetch(`${apiBase}/users/me/tokens`, {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const t = await res.text();
        throw new Error(t);
      }
      const json = await res.json();
      const plain = json?.data?.token as string;
      const metaId = json?.data?.meta?.id as number | undefined;
      setCreatedPlaintext(plain || null);
      if (plain && metaId != null) {
        savePatSecret(metaId, plain);
        setPatSecrets(loadPatSecrets());
      }
      message.success('令牌已创建，请立即复制保存；关闭页面前可在列表再次复制。');
      await load();
    } catch (e) {
      message.error('创建失败');
      console.error(e);
    }
  };

  const revoke = async (id: number) => {
    Modal.confirm({
      title: '撤销此 API 令牌？',
      okText: '撤销',
      cancelText: '取消',
      okType: 'danger',
      onOk: async () => {
        const res = await fetch(`${apiBase}/users/me/tokens/${id}`, {
          method: 'DELETE',
          headers: authHeaders(),
        });
        if (!res.ok) {
          message.error('撤销失败');
          return;
        }
        removePatSecret(id);
        setPatSecrets(loadPatSecrets());
        message.success('已撤销');
        await load();
      },
    });
  };

  const bg = dark ? DARK_BG : '#ffffff';
  const containerStyle: React.CSSProperties = {
    background: bg,
    minHeight: '100vh',
    fontFamily:
      '-apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Hiragino Sans GB", sans-serif',
  };

  const inputSurface: React.CSSProperties = dark
    ? { background: '#0f172a', color: '#e2e8f0', borderColor: DARK_BORDER }
    : {};

  const modalStyles = {
    content: { background: dark ? DARK_SURFACE : '#fff' },
    header: {
      background: dark ? DARK_SURFACE : '#fff',
      borderBottom: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
    },
    body: { background: dark ? DARK_SURFACE : '#fff' },
    footer: {
      background: dark ? DARK_SURFACE : '#fff',
      borderTop: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
    },
  };

  const primaryBtnStyle: React.CSSProperties = {
    background: TEAL,
    borderColor: TEAL,
    fontWeight: 600,
  };

  return (
    <div style={containerStyle}>
      <div style={{ padding: '56px 40px 0', maxWidth: 760, margin: '0 auto' }}>
        <div
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: 8,
            fontSize: 12,
            fontWeight: 600,
            letterSpacing: '0.08em',
            textTransform: 'uppercase',
            color: TEAL,
            marginBottom: 20,
          }}
        >
          <KeyOutlined style={{ fontSize: 14 }} />
          Agent / MCP
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
          <span style={{ color: TEAL }}>API Keys</span>
          {' · '}
          个人访问令牌
        </h1>

        <p style={{ fontSize: 16, lineHeight: 1.75, color: dark ? '#94a3b8' : '#475569', margin: '0 0 20px' }}>
          令牌以 <code style={{ color: TEAL, fontSize: 14 }}>ght_</code> 开头，用于{' '}
          <code style={{ color: TEAL, fontSize: 14 }}>Authorization: Bearer …</code> 调用只读的{' '}
          <code style={{ color: TEAL, fontSize: 14 }}>/api/v1/agent</code>（Daily 与报告类接口）。
          请勿将令牌写入代码仓库或分享给他人。
        </p>

        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16, alignItems: 'center', marginBottom: 8 }}>
          <a
            href="/agent/skill/GUANHE_DAILY.md"
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
            <LinkOutlined /> 查看 GUANHE_DAILY.md
          </a>
          <span style={{ color: dark ? '#475569' : '#cbd5e1' }}>|</span>
          <button
            type="button"
            onClick={() => navigate('/agent-integration')}
            style={{
              background: 'none',
              border: 'none',
              padding: 0,
              cursor: 'pointer',
              color: TEAL,
              fontSize: 13,
              textDecoration: 'underline',
            }}
          >
            Agent 接入说明
          </button>
        </div>

        <div style={{ marginTop: 28 }}>
          <button
            type="button"
            onClick={openCreate}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 8,
              background: TEAL,
              color: '#fff',
              border: 'none',
              borderRadius: 8,
              padding: '10px 18px',
              fontSize: 14,
              fontWeight: 600,
              cursor: 'pointer',
              boxShadow: dark ? 'none' : '0 1px 2px rgba(15, 118, 110, 0.25)',
            }}
          >
            <PlusOutlined />
            生成令牌
          </button>
        </div>
      </div>

      {/* Quick connect tabs */}
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
          {QUICK_TABS.map((t) => {
            const active = quickTab === t.key;
            return (
              <button
                key={t.key}
                type="button"
                onClick={() => setQuickTab(t.key)}
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
                <div style={{ fontSize: 11, color: dark ? '#475569' : '#94a3b8', marginTop: 1 }}>{t.sub}</div>
              </button>
            );
          })}
        </div>
      </div>

      <div style={{ maxWidth: 760, margin: '0 auto', padding: '40px 40px 80px' }}>
        <SectionLabel>快速接入 · Skill 一句话安装</SectionLabel>
        <SectionTitle dark={dark}>不用手抄，直接丢给 Agent</SectionTitle>
        <SectionDesc dark={dark}>
          {quickTab === 'claude' && '在 Claude Code 或 Claude Desktop 的对话里粘贴下面这句，Agent 会自行拉取并安装 Skill。'}
          {quickTab === 'cursor' && '在 Cursor 里对 Agent 说下面这句（英文提示），即可安装同一套 Skill。'}
          {quickTab === 'codex' && '在 Codex CLI 或兼容客户端中发送安装指令。'}
        </SectionDesc>

        <CodeBlock dark={dark} label={quickTab === 'cursor' ? 'English prompt' : '安装指令'}>
          {quickInstall[quickTab]}
        </CodeBlock>

        <Divider dark={dark} />

        <SectionLabel>令牌列表</SectionLabel>
        <SectionTitle dark={dark}>您的令牌</SectionTitle>
        <SectionDesc dark={dark}>
          默认只显示有效令牌。服务端不保存完整明文（只存哈希），无法从数据库再次取出。
          在本页刚创建的令牌，完整密钥会暂存在当前浏览器会话中，可点击「复制」；换设备或关闭标签页后需重新生成。
        </SectionDesc>

        {revokedCount > 0 && (
          <div style={{ marginBottom: 12 }}>
            <button
              type="button"
              onClick={() => setShowRevoked((v) => !v)}
              style={{
                background: 'none',
                border: 'none',
                padding: 0,
                cursor: 'pointer',
                fontSize: 13,
                color: TEAL,
                textDecoration: 'underline',
              }}
            >
              {showRevoked ? '隐藏已撤销' : `显示已撤销（${revokedCount}）`}
            </button>
          </div>
        )}

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
                {['名称', '前缀', '权限', '过期', '最后使用', ''].map((h, i) => (
                  <th
                    key={h || `c${i}`}
                    style={{
                      padding: '9px 14px',
                      textAlign: 'left',
                      fontSize: 12,
                      fontWeight: 600,
                      color: dark ? '#94a3b8' : '#64748b',
                      letterSpacing: '0.05em',
                      borderBottom: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={6} style={{ padding: '28px 14px', textAlign: 'center', color: dark ? '#64748b' : '#94a3b8' }}>
                    加载中…
                  </td>
                </tr>
              ) : visibleRows.length === 0 ? (
                <tr>
                  <td colSpan={6} style={{ padding: '28px 14px', textAlign: 'center', color: dark ? '#64748b' : '#94a3b8' }}>
                    {rows.length === 0
                      ? '暂无令牌，点击上方「生成令牌」创建。'
                      : '暂无有效令牌。可点击上方「显示已撤销」查看历史记录。'}
                  </td>
                </tr>
              ) : (
                visibleRows.map((r, idx) => (
                  <tr
                    key={r.id}
                    style={{
                      opacity: r.revoked_at ? 0.55 : 1,
                      borderBottom:
                        idx < visibleRows.length - 1 ? `1px solid ${dark ? DARK_BORDER : '#f1f5f9'}` : 'none',
                    }}
                  >
                    <td
                      style={{
                        padding: '10px 14px',
                        fontSize: 14,
                        fontWeight: 600,
                        color: dark ? '#e2e8f0' : '#1e293b',
                      }}
                    >
                      {r.name}
                    </td>
                    <td
                      style={{
                        padding: '10px 14px',
                        fontFamily: 'ui-monospace, SFMono-Regular, monospace',
                        fontSize: 12.5,
                        color: TEAL,
                      }}
                    >
                      <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                        {r.prefix}
                        {!r.revoked_at && patSecrets[String(r.id)] && (
                          <CopyFullTokenButton token={patSecrets[String(r.id)]} dark={dark} />
                        )}
                      </span>
                    </td>
                    <td style={{ padding: '10px 14px' }}>
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                        {(r.scopes || []).map((s) => (
                          <span
                            key={s}
                            style={{
                              fontSize: 11,
                              padding: '2px 8px',
                              borderRadius: 6,
                              border: `1px solid ${dark ? DARK_BORDER : '#e2e8f0'}`,
                              color: dark ? '#cbd5e1' : '#475569',
                              fontFamily: 'ui-monospace, SFMono-Regular, monospace',
                            }}
                          >
                            {s}
                          </span>
                        ))}
                      </div>
                    </td>
                    <td style={{ padding: '10px 14px', fontSize: 13, color: dark ? '#94a3b8' : '#64748b' }}>
                      {formatTs(r.expires_at)}
                    </td>
                    <td style={{ padding: '10px 14px', fontSize: 13, color: dark ? '#94a3b8' : '#64748b' }}>
                      {formatTs(r.last_used_at)}
                    </td>
                    <td style={{ padding: '10px 14px', textAlign: 'right' }}>
                      {r.revoked_at ? (
                        <span style={{ fontSize: 12, color: dark ? '#64748b' : '#94a3b8' }}>已撤销</span>
                      ) : (
                        <button
                          type="button"
                          onClick={() => void revoke(r.id)}
                          style={{
                            background: 'none',
                            border: 'none',
                            padding: 0,
                            cursor: 'pointer',
                            fontSize: 13,
                            color: '#f87171',
                            fontWeight: 500,
                          }}
                        >
                          撤销
                        </button>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        <Divider dark={dark} />

        <div
          style={{
            fontSize: 12,
            lineHeight: 1.7,
            color: dark ? '#475569' : '#94a3b8',
          }}
        >
          <div
            style={{
              marginBottom: 8,
              fontWeight: 600,
              color: dark ? '#64748b' : '#64748b',
              textTransform: 'uppercase',
              letterSpacing: '0.07em',
            }}
          >
            使用须知
          </div>
          <p style={{ margin: '0 0 6px' }}>
            <strong style={{ color: dark ? '#94a3b8' : '#475569' }}>安全</strong>
            {' — PAT 等同于密码；泄露后请立即撤销并重新生成。完整密钥仅创建时由服务端返回一次，数据库只保存哈希，无法再次查询明文。'}
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: dark ? '#94a3b8' : '#475569' }}>MCP / REST</strong>
            {' — 配置方式见 '}
            <button
              type="button"
              onClick={() => navigate('/agent-integration')}
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
              Agent 接入
            </button>
            页面。
          </p>
        </div>
      </div>

      <Modal
        title={
          <span style={{ color: dark ? '#f1f5f9' : '#0f172a', fontWeight: 700 }}>
            {createdPlaintext ? '请保存您的令牌' : '生成 API 令牌'}
          </span>
        }
        open={createOpen}
        onCancel={() => setCreateOpen(false)}
        styles={modalStyles}
        footer={
          createdPlaintext ? (
            <Button type="primary" style={primaryBtnStyle} onClick={() => setCreateOpen(false)}>
              完成
            </Button>
          ) : (
            <>
              <Button onClick={() => setCreateOpen(false)}>取消</Button>
              <Button type="primary" style={primaryBtnStyle} onClick={() => void submitCreate()}>
                创建
              </Button>
            </>
          )
        }
        destroyOnClose
      >
        {createdPlaintext ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            <div
              style={{
                padding: '12px 14px',
                borderRadius: 8,
                border: `1px solid ${dark ? '#854d0e' : '#fcd34d'}`,
                background: dark ? 'rgba(234, 179, 8, 0.12)' : '#fffbeb',
                fontSize: 13,
                color: dark ? '#fde68a' : '#92400e',
                lineHeight: 1.5,
              }}
            >
              完整明文不会存入服务器数据库。请立即复制到密码管理器；关闭本标签页前也可在下方列表再次复制。
            </div>
            <Input.TextArea
              readOnly
              rows={4}
              value={createdPlaintext}
              style={{ ...inputSurface, fontFamily: 'ui-monospace, monospace', fontSize: 13 }}
            />
            <Button
              type="default"
              icon={<CopyOutlined />}
              onClick={() => {
                void navigator.clipboard.writeText(createdPlaintext);
                message.success('已复制');
              }}
              style={dark ? { ...inputSurface } : undefined}
            >
              复制令牌
            </Button>
          </div>
        ) : (
          <Form form={form} layout="vertical">
            <Form.Item
              name="name"
              label={<span style={{ color: dark ? '#e2e8f0' : '#334155', fontWeight: 600 }}>名称</span>}
              rules={[{ required: true, message: '请填写名称' }]}
            >
              <Input placeholder="例如 MacBook Claude" maxLength={80} style={inputSurface} />
            </Form.Item>
            <Form.Item
              name="scopes"
              label={<span style={{ color: dark ? '#e2e8f0' : '#334155', fontWeight: 600 }}>权限范围</span>}
              rules={[{ required: true, message: '请至少选择一项权限' }]}
            >
              <Checkbox.Group options={SCOPE_OPTIONS} />
            </Form.Item>
            <Form.Item
              name="expires_days"
              label={<span style={{ color: dark ? '#e2e8f0' : '#334155', fontWeight: 600 }}>过期时间</span>}
            >
              <Select
                style={{ ...inputSurface, minWidth: 200 }}
                options={[
                  { value: 'never', label: '永不过期' },
                  { value: '30', label: '30 天' },
                  { value: '90', label: '90 天' },
                ]}
              />
            </Form.Item>
          </Form>
        )}
      </Modal>
    </div>
  );
};

export default ApiKeysPage;

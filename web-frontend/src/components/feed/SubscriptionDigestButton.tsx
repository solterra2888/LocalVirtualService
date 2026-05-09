/**
 * SubscriptionDigestButton
 *
 * Flow:
 *   Button → Popover（配置面板：天数 / 节目筛选 / 字符上限）→ Generate
 *   → loading → Modal（统计行 + 可读 textarea + Copy / Download）
 *
 * Props:
 *   subscriptionId  — 当前订阅 ID（null → 按钮禁用）
 *   channelName     — 显示名称
 *   availableShows  — 当前内容列表中出现的节目名数组（来自父组件 availableShows）
 *   themeMode       — 'light' | 'dark'
 *   iconOnly        — 仅图标（移动端窄布局）
 */

import React, { useState, useCallback, useEffect, useRef } from 'react';
import {
  App,
  Button,
  InputNumber,
  Modal,
  Popover,
  Segmented,
  Select,
  Space,
  Spin,
  Tag,
  Typography,
} from 'antd';
import {
  CopyOutlined,
  DownloadOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { subscriptionsAPI, type SubscriptionDigestPayload } from '../../services/subscriptions';
import { getModalStyles } from '../../utils/modalStyles';

/** Gemini-style sparkle (approximation; not an official Google asset). */
function GeminiDigestIcon(props: React.SVGProps<SVGSVGElement> & { title?: string }) {
  const { title, style, ...rest } = props;
  const uid = React.useId().replace(/[^a-zA-Z0-9_-]/g, '');
  const gid = `geminiDigestGrad-${uid}`;
  return (
    <svg
      viewBox="0 0 24 24"
      width="1em"
      height="1em"
      style={{ display: 'inline-block', verticalAlign: '-0.125em', ...style }}
      aria-hidden={title ? undefined : true}
      role={title ? 'img' : undefined}
      aria-label={title}
      {...rest}
    >
      {title ? <title>{title}</title> : null}
      <defs>
        <linearGradient id={gid} x1="3" y1="3" x2="21" y2="21" gradientUnits="userSpaceOnUse">
          <stop stopColor="#4285F4" />
          <stop offset="0.5" stopColor="#9B72CB" />
          <stop offset="1" stopColor="#D96570" />
        </linearGradient>
      </defs>
      <path
        fill={`url(#${gid})`}
        d="M12 2.25l1.35 5.4h5.55l-4.5 3.45 1.72 5.35L12 13.9l-4.57 3.55 1.72-5.35-4.5-3.45h5.55L12 2.25z"
      />
    </svg>
  );
}

const { Text } = Typography;

export interface SubscriptionDigestButtonProps {
  subscriptionId: number | null | undefined;
  channelName?: string;
  themeMode: 'light' | 'dark';
  /** 仅图标（移动端） */
  iconOnly?: boolean;
}

type Days = 1 | 3 | 7;

const DEFAULT_MAX_TOTAL_CHARS = 400_000;

const SubscriptionDigestButton: React.FC<SubscriptionDigestButtonProps> = ({
  subscriptionId,
  channelName,
  themeMode,
  iconOnly = false,
}) => {
  const { message } = App.useApp();
  const isDark = themeMode === 'dark';

  // ── 配置面板状态 ─────────────────────────────────────────
  const [popoverOpen, setPopoverOpen] = useState(false);
  const [days, setDays] = useState<Days>(1);
  const [showFilter, setShowFilter] = useState<string | null>(null);
  const [maxTotalChars, setMaxTotalChars] = useState<number>(DEFAULT_MAX_TOTAL_CHARS);

  // ── 动态节目列表（按天数从后端查询，避免依赖前端已加载的有限列表）──────────
  const [availableShows, setAvailableShows] = useState<string[]>([]);
  const [showsLoading, setShowsLoading] = useState(false);

  // ── 结果弹窗状态 ─────────────────────────────────────────
  const [loading, setLoading] = useState(false);
  const [digest, setDigest] = useState<SubscriptionDigestPayload | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [copied, setCopied] = useState(false);

  // reset when subscription changes
  const prevSubId = useRef(subscriptionId);
  if (prevSubId.current !== subscriptionId) {
    prevSubId.current = subscriptionId;
    setShowFilter(null);
    setAvailableShows([]);
  }

  // 当 popover 打开且 subscriptionId / days 变化时，重新拉取节目列表
  useEffect(() => {
    if (!popoverOpen || !subscriptionId) return;
    let cancelled = false;
    setShowsLoading(true);
    subscriptionsAPI.getSubscriptionShows(subscriptionId, days).then((res) => {
      if (cancelled) return;
      setShowsLoading(false);
      if (res.success && res.data) {
        setAvailableShows(res.data.shows);
        // 如果当前选中的节目在新列表里不存在（换了 days 后消失），重置为全部
        setShowFilter((prev) =>
          prev && res.data!.shows.includes(prev) ? prev : null,
        );
      } else {
        setAvailableShows([]);
      }
    });
    return () => { cancelled = true; };
  }, [popoverOpen, subscriptionId, days]);

  const handleGenerate = useCallback(async () => {
    if (!subscriptionId) return;
    setPopoverOpen(false);
    setLoading(true);
    setDigest(null);
    setModalOpen(true);

    const res = await subscriptionsAPI.getSubscriptionDigest(subscriptionId, days, {
      showFilter: showFilter || undefined,
      maxTotalChars,
    });
    setLoading(false);

    if (!res.success || !res.data) {
      setModalOpen(false);
      message.error(res.error || 'Failed to fetch digest');
      return;
    }
    setDigest(res.data);
  }, [subscriptionId, days, showFilter, maxTotalChars, message]);

  const handleCopy = async () => {
    if (!digest) return;
    try {
      await navigator.clipboard.writeText(digest.text);
    } catch {
      const el = document.createElement('textarea');
      el.value = digest.text;
      el.style.cssText = 'position:fixed;opacity:0';
      document.body.appendChild(el);
      el.focus();
      el.select();
      document.execCommand('copy');
      document.body.removeChild(el);
    }
    setCopied(true);
    message.success('Copied digest to clipboard');
    setTimeout(() => setCopied(false), 1500);
  };

  const handleDownload = () => {
    if (!digest) return;
    const name = (channelName || `subscription-${digest.subscription_id}`)
      .replace(/[^a-zA-Z0-9\-_ ]/g, '')
      .trim()
      .replace(/\s+/g, '-');
    const showSuffix = digest.show_filter
      ? `-${digest.show_filter.replace(/\s+/g, '-')}`
      : '';
    const blob = new Blob([digest.text], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${name}${showSuffix}-${digest.days}d-digest.txt`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // ── 色彩 token ────────────────────────────────────────────
  const textareaBg  = isDark ? '#1a1f2e' : '#f8f8f8';
  const textColor   = isDark ? 'rgba(255,255,255,0.85)' : 'rgba(0,0,0,0.88)';
  const mutedColor  = isDark ? 'rgba(255,255,255,0.45)' : 'rgba(0,0,0,0.45)';
  const borderColor = isDark ? 'rgba(255,255,255,0.10)' : '#d9d9d9';
  const popBg       = isDark ? '#1e2738' : '#fff';
  const labelColor  = isDark ? 'rgba(255,255,255,0.65)' : 'rgba(0,0,0,0.65)';

  // ── 配置面板内容 ───────────────────────────────────────────
  const configPanel = (
    <div style={{ width: 260 }}>
      <Space direction="vertical" size={14} style={{ width: '100%' }}>

        {/* 天数 */}
        <div>
          <Text style={{ fontSize: 12, color: labelColor, display: 'block', marginBottom: 6 }}>
            Time range
          </Text>
          <Segmented
            block
            value={days}
            onChange={(v) => setDays(v as Days)}
            options={[
              { label: '1 day',  value: 1 },
              { label: '3 days', value: 3 },
              { label: '7 days', value: 7 },
            ]}
          />
        </div>

        {/* 节目筛选（YouTube 订阅时渲染；动态从后端按天数查询） */}
        {(showsLoading || availableShows.length > 0) && (
          <div>
            <Text style={{ fontSize: 12, color: labelColor, display: 'block', marginBottom: 6 }}>
              Program filter
            </Text>
            <Select
              style={{ width: '100%' }}
              loading={showsLoading}
              disabled={showsLoading}
              value={showFilter ?? '__all__'}
              onChange={(v) => setShowFilter(v === '__all__' ? null : v)}
              options={[
                { label: 'All programs', value: '__all__' },
                ...availableShows.map((s) => ({ label: s, value: s })),
              ]}
              size="small"
              placeholder={showsLoading ? 'Loading programs…' : 'All programs'}
            />
          </div>
        )}

        {/* 字符上限 */}
        <div>
          <Text style={{ fontSize: 12, color: labelColor, display: 'block', marginBottom: 6 }}>
            Max total chars
          </Text>
          <InputNumber
            style={{ width: '100%' }}
            min={10_000}
            max={1_500_000}
            step={50_000}
            value={maxTotalChars}
            onChange={(v) => setMaxTotalChars(v ?? DEFAULT_MAX_TOTAL_CHARS)}
            formatter={(v) => `${v}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}
            parser={(v) => Number((v || '').replace(/,/g, '')) as any}
            size="small"
          />
          <Text style={{ fontSize: 11, color: mutedColor }}>
            Default 400,000 ≈ Gemini 1.5 Pro safe range
          </Text>
        </div>

        <Button
          type="primary"
          block
          icon={<GeminiDigestIcon />}
          onClick={handleGenerate}
          disabled={!subscriptionId}
        >
          Generate digest
        </Button>
      </Space>
    </div>
  );

  // ── Modal 标题 ────────────────────────────────────────────
  const modalTitle = digest
    ? [
        digest.channel_name,
        digest.show_filter,
        `Last ${digest.days} day${digest.days > 1 ? 's' : ''}`,
      ]
        .filter(Boolean)
        .join(' · ')
    : 'Generating digest…';

  return (
    <>
      <Popover
        open={popoverOpen}
        onOpenChange={(v) => {
          if (!subscriptionId) return;
          setPopoverOpen(v);
        }}
        trigger="click"
        placement="bottomRight"
        arrow={false}
        content={configPanel}
        overlayInnerStyle={{
          padding: 16,
          backgroundColor: popBg,
          border: `1px solid ${borderColor}`,
          borderRadius: 10,
          boxShadow: isDark
            ? '0 8px 24px rgba(0,0,0,0.5)'
            : '0 4px 16px rgba(0,0,0,0.12)',
        }}
      >
        <Button
          size="small"
          icon={<GeminiDigestIcon />}
          disabled={!subscriptionId}
          style={{ flexShrink: 0 }}
          title="Export digest for Gemini / LLM analysis"
        >
          {!iconOnly && 'Digest'}
        </Button>
      </Popover>

      <Modal
        title={
          <Space size={6}>
            <GeminiDigestIcon style={{ fontSize: 18 }} />
            <span>{modalTitle}</span>
          </Space>
        }
        open={modalOpen}
        onCancel={() => { setModalOpen(false); setDigest(null); }}
        width={780}
        centered
        styles={getModalStyles(themeMode)}
        maskStyle={isDark ? { background: 'rgba(0,0,0,0.65)' } : undefined}
        footer={
          digest ? (
            <Space>
              <Button icon={<CopyOutlined />} type="primary" onClick={handleCopy}>
                {copied ? 'Copied ✓' : 'Copy to clipboard'}
              </Button>
              <Button icon={<DownloadOutlined />} onClick={handleDownload}>
                Download .txt
              </Button>
              <Button onClick={() => { setModalOpen(false); setDigest(null); }}>
                Close
              </Button>
            </Space>
          ) : null
        }
        destroyOnClose
      >
        {/* Loading */}
        {loading && (
          <div style={{ textAlign: 'center', padding: '48px 0' }}>
            <Spin size="large" />
            <div style={{ marginTop: 16, color: mutedColor, fontSize: 13 }}>
              Fetching digest…
            </div>
          </div>
        )}

        {/* Result */}
        {!loading && digest && (
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            {/* stat row */}
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'center' }}>
              <Tag color="blue">{digest.items_count} items</Tag>
              <Tag color="default">
                {digest.total_chars >= 1000
                  ? `${(digest.total_chars / 1000).toFixed(0)}k chars`
                  : `${digest.total_chars} chars`}
              </Tag>
              {digest.skipped_count > 0 && (
                <Tag color="orange" icon={<WarningOutlined />}>
                  {digest.skipped_count} skipped (no body)
                </Tag>
              )}
              {digest.truncated && (
                <Tag color="red" icon={<WarningOutlined />}>
                  Truncated — reached {(maxTotalChars / 1000).toFixed(0)}k char limit
                </Tag>
              )}
              <Text type="secondary" style={{ fontSize: 11 }}>
                Since {new Date(digest.since).toLocaleString()}
              </Text>
            </div>

            {/* textarea */}
            <textarea
              readOnly
              value={digest.text}
              style={{
                width: '100%',
                height: '52vh',
                resize: 'vertical',
                fontFamily: '"SF Mono","Fira Code","Consolas",monospace',
                fontSize: 12,
                lineHeight: 1.6,
                padding: '12px 14px',
                borderRadius: 8,
                border: `1px solid ${borderColor}`,
                backgroundColor: textareaBg,
                color: textColor,
                outline: 'none',
                boxSizing: 'border-box',
                overflowY: 'auto',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-word',
              }}
              onClick={(e) => (e.target as HTMLTextAreaElement).select()}
              aria-label="Digest text"
            />

          </Space>
        )}

        {/* Usage instructions — always at bottom while modal is open */}
        {modalOpen && (
          <div
            style={{
              marginTop: loading || digest ? 16 : 0,
              padding: '10px 12px',
              borderRadius: 8,
              border: `1px solid ${borderColor}`,
              backgroundColor: isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.02)',
            }}
          >
            <Text strong style={{ fontSize: 12, color: textColor, display: 'block', marginBottom: 6 }}>
              How to use this digest with Gemini
            </Text>
            <Text type="secondary" style={{ fontSize: 12, lineHeight: 1.65, display: 'block' }}>
              This text bundles recent items from your subscription (Twitter: tweet text; YouTube:
              transcripts where available; News: article body). Copy it or download the .txt file,
              then paste into Google Gemini (web or the Chrome extension). Gemini can summarize,
              compare themes, extract tickers, or answer questions across everything included.
              If you hit the character limit, raise &quot;Max total chars&quot; before generating again,
              or narrow the time range / program filter.
            </Text>
          </div>
        )}
      </Modal>
    </>
  );
};

export default SubscriptionDigestButton;

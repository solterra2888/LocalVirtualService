/**
 * Daily 模块 — 信息流卡片.
 *
 * 单条卡片的呈现 (右上角 SourceBadge + 标题 + AI 摘要 / 内容片段 +
 * Tickers / Concepts 标签 + 时间和阅读时长). 设计参考 AI Notes Daily 的 Summaries 视图,
 * 来源徽章固定在右上角与标题同行 (扫读时一眼可辨内容来源).
 */

import React from 'react';
import { ClockCircleOutlined } from '@ant-design/icons';
import { Avatar, Tooltip } from 'antd';
import type { DailyFeedItem } from '../../services/daily';
import SourceBadge from './SourceBadge';
import { DAILY_THEME_COLORS } from './dailyTheme';
import { formatTime as fmtTimeInTz } from '../../utils/datetime';
import { useDashboardSettings } from '../../contexts/DashboardSettingsContext';
import { aiTagChipStyle } from '../shared/aiTagChipStyles';
import { useAuthStore } from '../../store/authStore';

interface DailyCardProps {
  item: DailyFeedItem;
  themeMode: 'light' | 'dark';
  onClick?: (item: DailyFeedItem) => void;
  onTickerClick?: (ticker: string) => void;
}

const DailyCard: React.FC<DailyCardProps> = ({ item, themeMode, onClick, onTickerClick }) => {
  const colors = DAILY_THEME_COLORS[themeMode];
  const { userTimezone } = useDashboardSettings();
  const currentUser = useAuthStore((s) => s.user);
  const [hover, setHover] = React.useState(false);

  // 去重: 同一 theme_name 可能通过 ticker + vector 两种模式匹配
  const dedupedThemes = React.useMemo(() => {
    const seen = new Set<string>();
    return (item.matched_themes ?? []).filter(t => {
      if (seen.has(t.theme_name)) return false;
      seen.add(t.theme_name);
      return true;
    });
  }, [item.matched_themes]);

  const subThemes = React.useMemo(
    () => Array.from(new Set((item.matched_themes ?? []).map(t => t.category).filter(Boolean) as string[])),
    [item.matched_themes],
  );

  const isIncomingShare = item.source === 'upload' && !!item.shared_by_username;
  const isOutgoingShare =
    item.source === 'upload' &&
    !item.shared_by_username &&
    (item.shared_to?.length ?? 0) > 0;

  const summaryOrSnippet = item.summary || item.snippet;
  const hasSummary = !!item.summary;

  const handleClick = () => {
    onClick?.(item);
  };

  return (
    <div
      onClick={handleClick}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{
        position: 'relative',
        background: hover ? colors.cardHoverBg : colors.cardBg,
        border: `1px solid ${hover ? colors.cardHoverBorder : colors.cardBorder}`,
        borderRadius: 8,
        padding: '14px 16px',
        cursor: 'pointer',
        transition: 'all 0.15s ease',
      }}
    >
      {/* 第一行: 标题 + 右上角来源徽章 */}
      <div
        style={{
          display: 'flex',
          alignItems: 'flex-start',
          gap: 12,
          marginBottom: summaryOrSnippet ? 6 : 8,
        }}
      >
        <h3
          style={{
            margin: 0,
            flex: 1,
            minWidth: 0,
            fontSize: 13,
            fontWeight: 600,
            lineHeight: 1.45,
            color: colors.text,
            overflow: 'hidden',
            display: '-webkit-box',
            WebkitLineClamp: 2,
            WebkitBoxOrient: 'vertical',
            wordBreak: 'break-word',
          }}
        >
          {item.title}
        </h3>
        <div style={{ flexShrink: 0, paddingTop: 2 }}>
          <SourceBadge subtype={item.source_subtype} themeMode={themeMode} iconOnly />
        </div>
      </div>

      {/* 摘要 / 片段 (摘要用正常透明度, snippet 兜底用稍浅) */}
      {summaryOrSnippet && (
        <p
          style={{
            margin: '0 0 10px',
            fontSize: 13,
            lineHeight: 1.55,
            color: hasSummary ? colors.summaryText : colors.textMuted,
            overflow: 'hidden',
            display: '-webkit-box',
            WebkitLineClamp: 3,
            WebkitBoxOrient: 'vertical',
            wordBreak: 'break-word',
          }}
        >
          {summaryOrSnippet}
        </p>
      )}

      {/* Tickers + Concepts 标签行 */}
      {(item.tickers.length > 0 || item.concepts.length > 0) && (
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: 6,
            marginBottom: 8,
          }}
        >
          {item.tickers.slice(0, 8).map((ticker) => (
            <span
              key={`t-${ticker}`}
              onClick={(e) => {
                if (!onTickerClick) return;
                e.stopPropagation();
                onTickerClick(ticker);
              }}
              style={{
                ...aiTagChipStyle('ticker', themeMode),
                cursor: onTickerClick ? 'pointer' : 'default',
              }}
            >
              {ticker.replace(/\.US$/, '')}
            </span>
          ))}
          {item.concepts.slice(0, 6).map((concept) => (
            <span key={`c-${concept}`} style={aiTagChipStyle('concept', themeMode)}>
              {concept}
            </span>
          ))}
          {dedupedThemes.length > 0 && dedupedThemes.slice(0, 3).map((theme) => (
            <Tooltip
              key={`th-${theme.theme_id || theme.theme_name}`}
              title={theme.confidence ? `${(theme.confidence * 100).toFixed(0)}%` : undefined}
              mouseEnterDelay={0.4}
            >
              <span style={aiTagChipStyle('theme', themeMode)}>
                {theme.theme_name}
              </span>
            </Tooltip>
          ))}
          {dedupedThemes.length > 3 && (
            <Tooltip
              title={dedupedThemes.slice(3).map(t => t.theme_name).join(', ')}
              mouseEnterDelay={0.3}
            >
              <span style={aiTagChipStyle('overflow', themeMode)}>
                +{dedupedThemes.length - 3}
              </span>
            </Tooltip>
          )}
          {subThemes.length > 0 && subThemes.slice(0, 2).map((cat) => (
            <span key={`st-${cat}`} style={aiTagChipStyle('concept', themeMode)}>
              {cat}
            </span>
          ))}
          {subThemes.length > 2 && (
            <Tooltip
              title={subThemes.slice(2).join(', ')}
              mouseEnterDelay={0.3}
            >
              <span style={aiTagChipStyle('overflow', themeMode)}>
                +{subThemes.length - 2}
              </span>
            </Tooltip>
          )}
        </div>
      )}

      {/* 底部 meta 行: 作者头像(Feed/Twitter) + 作者名 + 时间 + 阅读时长 + 共享 Avatar 对(Upload 共享时) */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 10,
          fontSize: 11,
          color: colors.textMuted,
        }}
      >
        {item.author_name && (
          <>
            {/* Twitter 作者头像 - 16px 小头像 */}
            {item.source_subtype === 'twitter' && item.author_avatar && (
              <Avatar
                size={16}
                src={item.author_avatar}
                style={{ flexShrink: 0 }}
              />
            )}
            <span
              style={{
                maxWidth: 180,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
              title={item.author_name}
            >
              {item.author_name}
            </span>
            <span style={{ opacity: 0.5 }}>·</span>
          </>
        )}
        <span>{fmtTimeInTz(item.published_at, userTimezone)}</span>
        {item.reading_time_min ? (
          <>
            <span style={{ opacity: 0.5 }}>·</span>
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
              <ClockCircleOutlined style={{ fontSize: 10 }} />
              {item.reading_time_min} min
            </span>
          </>
        ) : null}
        {!hasSummary && item.snippet && (
          <>
            <span style={{ opacity: 0.5 }}>·</span>
            <span style={{ fontStyle: 'italic', opacity: 0.7 }}>summary pending</span>
          </>
        )}

        {/* 被分享：分享者 | 当前用户 */}
        {isIncomingShare && item.shared_by_username && (
          <div
            style={{
              marginLeft: 'auto',
              display: 'flex',
              alignItems: 'center',
              gap: 4,
              flexShrink: 0,
            }}
          >
            <Tooltip title={`Shared by ${item.shared_by_username}`}>
              <Avatar
                size={20}
                src={item.shared_by_avatar_url ?? undefined}
                style={{ backgroundColor: '#722ed1', fontSize: 10, cursor: 'default' }}
              >
                {item.shared_by_username.charAt(0).toUpperCase()}
              </Avatar>
            </Tooltip>
            <span style={{ color: colors.textMuted, fontSize: 12, lineHeight: '20px', opacity: 0.5, userSelect: 'none' }}>
              |
            </span>
            <Tooltip title={currentUser?.username ?? 'You'}>
              <Avatar
                size={20}
                src={currentUser?.avatar_url ?? undefined}
                style={{ backgroundColor: '#1677ff', fontSize: 10, cursor: 'default' }}
              >
                {(currentUser?.username ?? 'Y').charAt(0).toUpperCase()}
              </Avatar>
            </Tooltip>
          </div>
        )}

        {/* 分享者（owner）：自己 | 收件人组 — 与 Sources 左侧栏一致 */}
        {isOutgoingShare && (
          <div
            style={{
              marginLeft: 'auto',
              display: 'flex',
              alignItems: 'center',
              gap: 4,
              flexShrink: 0,
            }}
          >
            <Tooltip title={currentUser?.username ?? 'You'}>
              <Avatar
                size={20}
                src={currentUser?.avatar_url ?? undefined}
                style={{ backgroundColor: '#722ed1', fontSize: 10, cursor: 'default' }}
              >
                {(currentUser?.username ?? 'Y').charAt(0).toUpperCase()}
              </Avatar>
            </Tooltip>
            <span style={{ color: colors.textMuted, fontSize: 12, lineHeight: '20px', opacity: 0.5, userSelect: 'none' }}>
              |
            </span>
            <Avatar.Group size={20} max={{ count: 3, style: { backgroundColor: '#722ed1', fontSize: 10 } }}>
              {item.shared_to.map((u) => (
                <Tooltip key={u.user_id} title={u.username}>
                  <Avatar
                    size={20}
                    src={u.avatar_url ?? undefined}
                    style={{ backgroundColor: '#1890ff', fontSize: 10 }}
                  >
                    {u.username.charAt(0).toUpperCase()}
                  </Avatar>
                </Tooltip>
              ))}
            </Avatar.Group>
          </div>
        )}
      </div>
    </div>
  );
};

export default DailyCard;

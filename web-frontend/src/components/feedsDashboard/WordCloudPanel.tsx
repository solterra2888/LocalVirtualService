/**
 * Feeds Dashboard 词云面板。
 *
 * 数据: GET /api/feeds-dashboard/wordcloud (词频来自 subscription_content_tags)
 * 布局 (自上而下):
 *   ① Watchlist · 热门 Ticker 合并区 (10px): watchlist 有命中的符号置顶 (带命中徽标,
 *      0 条不显示省空间; 按基础符号与热门去重), 热门 ticker 续排; 点击走 ticker 过滤
 *      (后端智能格式变体匹配)
 *   ② 今日热词 (concepts): 字号 ∝ 词频 (12→22px), 颜色 = 单色顺序蓝按频次分档
 *      (已过对比度校验): light #5598e7→#104281 / dark #9ec5f4→#256abf; 折并词带 topic_id
 *
 * 可视化规范 (dataviz): hover = ghost wash, 命中区 ≥24px, Tooltip 显示条数,
 * 词用 <button> 渲染 (键盘可达), 重载保持原帧降透明度不闪骨架屏。
 */

import React from 'react';
import { Tooltip } from 'antd';
import { DAILY_THEME_COLORS } from '../daily/dailyTheme';

export interface WordCloudWord {
  word: string;
  count: number;
  type: 'concept' | 'ticker' | 'watchlist';
  /** 折并到的主题 id; 散词/ticker 为 null/undefined */
  topic_id?: number | null;
}

export interface SelectedWord {
  word: string;
  type: 'concept' | 'ticker';
  /** 词云折并词点击时携带 — /items 走 topic_id 过滤 (命中该主题全部变体) */
  topicId?: number;
}

// 顺序蓝分档 (已过 validate_palette ordinal 校验; 4 档按词频四分位)
const CONCEPT_RAMP = {
  light: ['#5598e7', '#2a78d6', '#1c5cab', '#104281'],
  dark: ['#9ec5f4', '#6da7ec', '#3987e5', '#256abf'],
} as const;

const FONT_MIN = 12;
const FONT_MAX = 22;

export interface WordCloudPanelProps {
  words: WordCloudWord[];
  /** 用户 watchlist 符号 + 当日命中条数 (0 条也展示, 置灰) */
  watchlistWords: WordCloudWord[];
  selected: SelectedWord | null;
  onSelect: (word: SelectedWord | null) => void;
  themeMode: 'light' | 'dark';
  loading?: boolean;
  /** 移动端紧凑模式: 限制词数 */
  maxWords?: number;
  /** 根卡片样式透传 (桌面满高布局传 minHeight:'100%' 撑满列) */
  style?: React.CSSProperties;
}

const WordCloudPanel: React.FC<WordCloudPanelProps> = ({
  words,
  watchlistWords,
  selected,
  onSelect,
  themeMode,
  loading = false,
  maxWords,
  style,
}) => {
  const colors = DAILY_THEME_COLORS[themeMode];
  const ramp = CONCEPT_RAMP[themeMode];

  const concepts = words.filter((w) => w.type === 'concept');
  const tickers = words.filter((w) => w.type === 'ticker');

  const counts = concepts.map((w) => w.count);
  const maxCount = Math.max(...counts, 1);
  const minCount = Math.min(...counts, 0);

  /** 词频 → 字号 (线性映射到 [FONT_MIN, FONT_MAX]) */
  const fontSizeOf = (count: number): number => {
    if (maxCount <= minCount) return (FONT_MIN + FONT_MAX) / 2;
    const t = (count - minCount) / (maxCount - minCount);
    return Math.round(FONT_MIN + t * (FONT_MAX - FONT_MIN));
  };

  /** 词频排名 → ramp 分档 (四分位; 词频越高颜色越深/亮) */
  const rampIndexOf = (index: number, total: number): number => {
    if (total <= 1) return ramp.length - 1;
    const t = index / total; // 0 = 频次最高
    return Math.min(ramp.length - 1, Math.floor(t * ramp.length));
  };

  /** 概念词按钮 (顺序蓝 + 字号∝词频; ticker 已移至合并区专用渲染) */
  const renderWordBtn = (
    w: WordCloudWord,
    rampIndex: number | null,
    key: string,
  ) => {
    const isSelected = !!selected && selected.word === w.word && selected.type === 'concept';
    return (
      <Tooltip key={key} title={`${w.count} items · click to ${isSelected ? 'clear' : 'filter'}`}>
        <button
          type="button"
          onClick={() =>
            onSelect(
              isSelected
                ? null
                : {
                    word: w.word,
                    type: 'concept' as const,
                    topicId: w.topic_id ? w.topic_id : undefined,
                  },
            )
          }
          style={{
            color: ramp[rampIndex ?? ramp.length - 1],
            fontWeight: rampIndex !== null && rampIndex >= 2 ? 600 : 500,
            fontSize: fontSizeOf(w.count),
            lineHeight: '20px',
            padding: '3px 5px',
            minHeight: 24, // 命中区 ≥ 24px (大于绘制像素)
            margin: 2,
            border: 'none',
            cursor: 'pointer',
            background: isSelected ? colors.accent : 'transparent',
            // 选中态覆盖上方 ramp 颜色 (后写键生效)
            ...(isSelected ? { color: '#fff' } : null),
            borderRadius: 6,
            transition: 'background .15s, opacity .2s',
            fontFamily: 'inherit',
          }}
          onMouseEnter={(e) => {
            if (!isSelected) e.currentTarget.style.background = colors.cardHoverBg;
          }}
          onMouseLeave={(e) => {
            if (!isSelected) e.currentTarget.style.background = 'transparent';
          }}
        >
          {w.word}
        </button>
      </Tooltip>
    );
  };

  const shownConcepts = maxWords ? concepts.slice(0, maxWords) : concepts;

  // 合并 Ticker 区: watchlist 仅展示有命中的符号 (0 条隐藏省空间), 热门 ticker 去重续排
  // (watchlist 存 "NVDA" 无后缀, 热门词是 "NVDA.US", 用去后缀基础符号比对;
  //  watchlist 0 命中 ⇒ 同基础符号的热门也必为 0, 去重集用过滤后的列表即可)
  const watchlistWithHits = watchlistWords.filter((w) => w.count > 0);
  const watchlistBases = new Set(
    watchlistWithHits.map((w) => w.word.split('.')[0].trim().toUpperCase()),
  );
  const hotTickers = tickers.filter(
    (w) => !watchlistBases.has(w.word.split('.')[0].trim().toUpperCase()),
  );
  const hasTickerSection = watchlistWithHits.length > 0 || hotTickers.length > 0;

  return (
    <div
      style={{
        background: colors.cardBg,
        border: `1px solid ${colors.cardBorder}`,
        borderRadius: 8,
        padding: 12,
        transition: 'opacity .2s',
        opacity: loading ? 0.55 : 1, // 重载保持原帧, 不闪骨架
        ...style,
      }}
    >
      {/* Ticker 合并区: Watchlist 命中符号 (边框+徽标, 0 条不显示) + 热门 ticker (底色chip) 续排 */}
      {hasTickerSection && (
        <>
          <div
            style={{
              fontSize: 13,
              fontWeight: 600,
              color: colors.text,
              marginBottom: 8,
              display: 'flex',
              alignItems: 'center',
              gap: 6,
            }}
          >
            Watchlist · Top Tickers
            <span style={{ fontSize: 10, fontWeight: 400, color: colors.textSecondary }}>
              click to filter
            </span>
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center' }}>
            {/* Watchlist 符号 (仅有命中): 实线边框 + 命中徽标, 10px */}
            {watchlistWithHits.map((w) => {
              const isSelected = !!selected && selected.type === 'ticker' && selected.word === w.word;
              return (
                <Tooltip
                  key={`wl-${w.word}`}
                  title={`${w.count} items · click to ${isSelected ? 'clear' : 'filter'}`}
                >
                  <button
                    type="button"
                    onClick={() =>
                      onSelect(isSelected ? null : { word: w.word, type: 'ticker' })
                    }
                    style={{
                      fontSize: 10,
                      lineHeight: '16px',
                      padding: '2px 6px',
                      minHeight: 24,
                      margin: 2,
                      border: `1px solid ${colors.tickerFg}`,
                      borderRadius: 4,
                      cursor: 'pointer',
                      background: isSelected ? colors.accent : colors.tickerBg,
                      color: isSelected ? '#fff' : colors.tickerFg,
                      fontWeight: 600,
                      transition: 'background .15s, opacity .2s',
                      fontFamily: 'inherit',
                    }}
                    onMouseEnter={(e) => {
                      if (!isSelected) e.currentTarget.style.background = colors.cardHoverBg;
                    }}
                    onMouseLeave={(e) => {
                      if (!isSelected) e.currentTarget.style.background = colors.tickerBg;
                    }}
                  >
                    {w.word}
                    <span
                      style={{
                        marginLeft: 3,
                        fontSize: 9,
                        opacity: 0.85,
                      }}
                    >
                      {w.count}
                    </span>
                  </button>
                </Tooltip>
              );
            })}
            {/* 热门 ticker (与 watchlist 基础符号去重后), 10px + 命中数 */}
            {hotTickers.map((w) => {
              const isSelected = !!selected && selected.type === 'ticker' && selected.word === w.word;
              return (
                <Tooltip key={`t-${w.word}`} title={`${w.count} items · click to ${isSelected ? 'clear' : 'filter'}`}>
                  <button
                    type="button"
                    onClick={() =>
                      onSelect(isSelected ? null : { word: w.word, type: 'ticker' })
                    }
                    style={{
                      fontSize: 10,
                      lineHeight: '16px',
                      padding: '2px 6px',
                      minHeight: 24,
                      margin: 2,
                      border: 'none',
                      borderRadius: 4,
                      cursor: 'pointer',
                      background: isSelected ? colors.accent : 'transparent',
                      color: isSelected ? '#fff' : colors.tickerFg,
                      fontWeight: 500,
                      transition: 'background .15s',
                      fontFamily: 'inherit',
                    }}
                    onMouseEnter={(e) => {
                      if (!isSelected) e.currentTarget.style.background = colors.cardHoverBg;
                    }}
                    onMouseLeave={(e) => {
                      if (!isSelected) e.currentTarget.style.background = 'transparent';
                    }}
                  >
                    {w.word}
                    <span style={{ marginLeft: 3, fontSize: 9, opacity: 0.75 }}>{w.count}</span>
                  </button>
                </Tooltip>
              );
            })}
          </div>
          <div
            style={{
              fontSize: 11,
              color: colors.textSecondary,
              margin: '10px 0 8px',
              borderTop: `1px solid ${colors.divider}`,
              paddingTop: 8,
              fontWeight: 600,
            }}
          >
            Trending Keywords
            <span style={{ fontSize: 11, fontWeight: 400, marginLeft: 6 }}>
              click to filter
            </span>
          </div>
        </>
      )}

      {!hasTickerSection && (
        <div
          style={{
            fontSize: 13,
            fontWeight: 600,
            color: colors.text,
            marginBottom: 8,
            display: 'flex',
            alignItems: 'center',
            gap: 6,
          }}
        >
          Trending Keywords
          <span style={{ fontSize: 11, fontWeight: 400, color: colors.textSecondary }}>
            click to filter
          </span>
        </div>
      )}

      {words.length === 0 && watchlistWithHits.length === 0 ? (
        <div style={{ fontSize: 12, color: colors.textSecondary, padding: '8px 0' }}>
          No keyword data
        </div>
      ) : (
        <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center' }}>
          {shownConcepts.map((w, idx) =>
            renderWordBtn(w, rampIndexOf(idx, shownConcepts.length), `c-${w.word}`),
          )}
        </div>
      )}
    </div>
  );
};

export default WordCloudPanel;

import React, { useMemo } from 'react';
import { Tag, Space, Tooltip } from 'antd';
import { AI_TAG_CHIP_BASE } from './shared/aiTagChipStyles';

/** 与新闻详情里 ticker 标签一致，用于 Sources / 研报文件卡片 */
export const SOURCE_FILE_TICKER_TAG_STYLE: React.CSSProperties = {
  margin: 0,
  marginInlineEnd: 0,
  fontSize: '7px',
  lineHeight: '12px',
  padding: '0 5px',
};

/**
 * @deprecated 请使用 `aiTagChipStyle('ticker' | 'sector' | 'concept', themeMode)`（`shared/aiTagChipStyles`）。
 * 仍导出为 `AI_TAG_CHIP_BASE` 的拷贝，供尚未接入主题的调用方兜底。
 */
export const PC_INFO_TAGS_TAG_STYLE: React.CSSProperties = { ...AI_TAG_CHIP_BASE };

/**
 * Normalize file tag `ticker` from API: may be a JSON array, comma-separated string, or a single symbol.
 */
export function parseReportTickers(raw: string | string[] | null | undefined): string[] {
  if (raw == null) return [];
  if (Array.isArray(raw)) {
    return Array.from(new Set(raw.map(t => String(t).trim()).filter(Boolean)));
  }
  const s = String(raw).trim();
  if (!s) return [];
  if (s.startsWith('[')) {
    try {
      const parsed = JSON.parse(s) as unknown;
      if (Array.isArray(parsed)) {
        return parseReportTickers(parsed as string[]);
      }
    } catch {
      /* treat as plain string below */
    }
  }
  return Array.from(
    new Set(s.split(/[,;，、]+|\s+/u).map(x => x.trim()).filter(Boolean))
  );
}

export interface ReportTickerTagsProps {
  tickers: string | string[] | null | undefined;
  /** Max number of ticker tags to show before collapsing to +N */
  maxVisible?: number;
  tagColor?: string;
  /** 合并进默认紧凑样式（默认与新闻详情 ticker 一致） */
  tagStyle?: React.CSSProperties;
  /** 传给 antd Tag；Feeds 药丸标签可传 false 去掉描边 */
  tagBordered?: boolean;
  /** Space wrap 间距，默认 [4, 4] */
  spaceSize?: number | [number, number];
  /** 仅用于 "+K" 溢出标签；不传则与主 tagStyle 一致 */
  overflowTagStyle?: React.CSSProperties;
  className?: string;
  style?: React.CSSProperties;
}

/**
 * Renders ticker tags with overflow: first N symbols as tags, remainder summarized as +K with full list in tooltip.
 */
const ReportTickerTags: React.FC<ReportTickerTagsProps> = ({
  tickers,
  maxVisible = 4,
  tagColor,
  tagStyle,
  tagBordered,
  spaceSize = [4, 4],
  overflowTagStyle,
  className,
  style,
}) => {
  const list = useMemo(() => parseReportTickers(tickers), [tickers]);

  const { visible, overflow } = useMemo(() => {
    const v = list.slice(0, maxVisible);
    const o = list.slice(maxVisible);
    return { visible: v, overflow: o };
  }, [list, maxVisible]);

  const mergedTagStyle = useMemo(
    () => ({ ...SOURCE_FILE_TICKER_TAG_STYLE, ...tagStyle }),
    [tagStyle]
  );

  const mergedOverflowStyle = useMemo(
    () =>
      overflowTagStyle
        ? { ...SOURCE_FILE_TICKER_TAG_STYLE, ...tagStyle, ...overflowTagStyle }
        : mergedTagStyle,
    [mergedTagStyle, overflowTagStyle, tagStyle]
  );

  if (list.length === 0) {
    return null;
  }

  const overflowTitle =
    overflow.length > 0 ? (
      <div
        style={{
          maxHeight: 280,
          overflowY: 'auto',
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
        }}
      >
        {overflow.join(', ')}
      </div>
    ) : null;

  return (
    <Space size={spaceSize} wrap className={className} style={style}>
      {visible.map(t => (
        <Tag key={t} color={tagColor} bordered={tagBordered} style={mergedTagStyle}>
          {t}
        </Tag>
      ))}
      {overflow.length > 0 && (
        <Tooltip title={overflowTitle}>
          <Tag color={tagColor} bordered={tagBordered} style={{ ...mergedOverflowStyle, cursor: 'default' }}>
            +{overflow.length}
          </Tag>
        </Tooltip>
      )}
    </Space>
  );
};

export default ReportTickerTags;

import React, { useMemo } from 'react';
import { Tag, Space, Tooltip } from 'antd';

/** 与新闻详情里 ticker 标签一致，用于 Sources / 研报文件卡片 */
export const SOURCE_FILE_TICKER_TAG_STYLE: React.CSSProperties = {
  margin: 0,
  marginInlineEnd: 0,
  fontSize: '7px',
  lineHeight: '12px',
  padding: '0 5px',
};

/** PC 右栏 Info > Tags：与左侧文件卡片 ticker 标签同结构，字号 10px */
export const PC_INFO_TAGS_TAG_STYLE: React.CSSProperties = {
  margin: 0,
  marginInlineEnd: 0,
  fontSize: '10px',
  lineHeight: '14px',
  padding: '0 5px',
};

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
    <Space size={[4, 4]} wrap className={className} style={style}>
      {visible.map(t => (
        <Tag key={t} color={tagColor} style={mergedTagStyle}>
          {t}
        </Tag>
      ))}
      {overflow.length > 0 && (
        <Tooltip title={overflowTitle}>
          <Tag color={tagColor} style={{ ...mergedTagStyle, cursor: 'default' }}>
            +{overflow.length}
          </Tag>
        </Tooltip>
      )}
    </Space>
  );
};

export default ReportTickerTags;

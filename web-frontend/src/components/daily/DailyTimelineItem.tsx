/**
 * Daily 列表项左侧时间轴轨道（竖线 + 节点），视觉参考 AIHOT 等信息流.
 *
 * 按日期分组内 consecutive items 连成一条竖线；首条无上段、末条无下段。
 */

import React from 'react';
import { useDashboardSettings } from '../../contexts/DashboardSettingsContext';
import { formatClockTime } from '../../utils/datetime';
import { DAILY_THEME_COLORS } from './dailyTheme';

/** 左侧：右对齐时刻 + 竖线/圆点 */
const TIME_LABEL_W = 44;
const LINE_W = 2;
const LINE_LEFT = TIME_LABEL_W + 4;
const TIMELINE_COL_W = LINE_LEFT + LINE_W + 6;
const DOT_TOP = 18;
const DOT_SIZE = 8;
const GAP_TO_CARD = 12;
/** 与 Daily 列表卡片间距一致，竖线延伸进间隙以保持连续 */
const STACK_GAP_AFTER = 10;

export interface DailyTimelineItemProps {
  themeMode: 'light' | 'dark';
  /** ISO 8601，用于时间轴显示时刻 */
  publishedAt: string;
  isFirst: boolean;
  isLast: boolean;
  /** 与卡片纵向间距一致，默认 10（桌面 Daily）；移动端可传 8 */
  stackGapAfter?: number;
  children: React.ReactNode;
}

const DailyTimelineItem: React.FC<DailyTimelineItemProps> = ({
  themeMode,
  publishedAt,
  isFirst,
  isLast,
  stackGapAfter,
  children,
}) => {
  const colors = DAILY_THEME_COLORS[themeMode];
  const { userTimezone } = useDashboardSettings();
  const clockLabel = formatClockTime(publishedAt, userTimezone);
  const lineLeft = LINE_LEFT;
  const dotCenterX = lineLeft + LINE_W / 2;
  const dotCenterY = DOT_TOP + DOT_SIZE / 2;
  const padBottom = isLast ? 0 : stackGapAfter ?? STACK_GAP_AFTER;

  return (
    <div
      style={{
        display: 'flex',
        gap: GAP_TO_CARD,
        alignItems: 'stretch',
        paddingBottom: padBottom,
      }}
    >
      <div
        style={{
          width: TIMELINE_COL_W,
          flexShrink: 0,
          position: 'relative',
          alignSelf: 'stretch',
        }}
      >
        <time
          dateTime={publishedAt}
          title={clockLabel}
          style={{
            position: 'absolute',
            left: 0,
            top: DOT_TOP - 1,
            width: TIME_LABEL_W,
            textAlign: 'right',
            fontSize: 11,
            fontVariantNumeric: 'tabular-nums',
            color: colors.textMuted,
            lineHeight: `${DOT_SIZE + 2}px`,
            pointerEvents: 'none',
          }}
        >
          {clockLabel}
        </time>
        {!isFirst ? (
          <div
            style={{
              position: 'absolute',
              left: lineLeft,
              top: 0,
              width: LINE_W,
              height: dotCenterY,
              background: colors.timelineRail,
              borderRadius: 1,
            }}
            aria-hidden
          />
        ) : null}
        {!isLast ? (
          <div
            style={{
              position: 'absolute',
              left: lineLeft,
              top: dotCenterY,
              width: LINE_W,
              bottom: 0,
              background: colors.timelineRail,
              borderRadius: 1,
            }}
            aria-hidden
          />
        ) : null}
        <div
          style={{
            position: 'absolute',
            left: dotCenterX,
            top: DOT_TOP,
            width: DOT_SIZE,
            height: DOT_SIZE,
            marginLeft: -DOT_SIZE / 2,
            borderRadius: '50%',
            background: colors.accent,
            border: `2px solid ${colors.pageBg}`,
            boxSizing: 'border-box',
            zIndex: 1,
          }}
          aria-hidden
        />
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>{children}</div>
    </div>
  );
};

export default DailyTimelineItem;

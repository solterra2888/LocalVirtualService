import React from 'react';
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  ClockCircleOutlined,
  LoadingOutlined,
} from '@ant-design/icons';

export type DailyReportStatus = 'completed' | 'generating' | 'failed' | 'pending';

type Palette = { bg: string; color: string; ariaLabel: string };

const LIGHT: Record<DailyReportStatus, Palette & { icon: React.ReactNode }> = {
  completed: {
    bg: '#E8F5E9',
    color: '#1B5E20',
    ariaLabel: 'Completed',
    icon: <CheckCircleOutlined />,
  },
  failed: {
    bg: '#FFF3E0',
    color: '#E65100',
    ariaLabel: 'Failed',
    icon: <CloseCircleOutlined />,
  },
  generating: {
    bg: '#E3F2FD',
    color: '#0D47A1',
    ariaLabel: 'Generating',
    icon: <LoadingOutlined spin />,
  },
  pending: {
    bg: '#F3E5F5',
    color: '#6A1B9A',
    ariaLabel: 'Pending',
    icon: <ClockCircleOutlined />,
  },
};

const DARK: Record<DailyReportStatus, Palette & { icon: React.ReactNode }> = {
  completed: {
    bg: 'rgba(76, 175, 80, 0.22)',
    color: '#A5D6A7',
    ariaLabel: 'Completed',
    icon: <CheckCircleOutlined />,
  },
  failed: {
    bg: 'rgba(255, 152, 0, 0.22)',
    color: '#FFCC80',
    ariaLabel: 'Failed',
    icon: <CloseCircleOutlined />,
  },
  generating: {
    bg: 'rgba(33, 150, 243, 0.22)',
    color: '#90CAF9',
    ariaLabel: 'Generating',
    icon: <LoadingOutlined spin />,
  },
  pending: {
    bg: 'rgba(186, 104, 200, 0.22)',
    color: '#CE93D8',
    ariaLabel: 'Pending',
    icon: <ClockCircleOutlined />,
  },
};

export interface DailyReportStatusTagProps {
  status: string;
  /** Dark dashboard theme */
  dark?: boolean;
  /** Slightly smaller (list rows) */
  compact?: boolean;
}

/** Icon-only pill: soft background + icon color (no visible text). */
export const DailyReportStatusTag: React.FC<DailyReportStatusTagProps> = ({
  status,
  dark = false,
  compact = false,
}) => {
  const key = (['completed', 'generating', 'failed', 'pending'].includes(status)
    ? status
    : 'pending') as DailyReportStatus;
  const spec = dark ? DARK[key] : LIGHT[key];
  const iconPx = compact ? 12 : 14;

  return (
    <span
      role="img"
      aria-label={spec.ariaLabel}
      title={spec.ariaLabel}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: compact ? 4 : 5,
        borderRadius: 50,
        background: spec.bg,
        color: spec.color,
        lineHeight: 1,
        border: 'none',
        flexShrink: 0,
      }}
    >
      <span
        style={{
          fontSize: iconPx,
          display: 'inline-flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}
      >
        {spec.icon}
      </span>
    </span>
  );
};

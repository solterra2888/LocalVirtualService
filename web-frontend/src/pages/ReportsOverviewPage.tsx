import React, { useState, useEffect } from 'react';
import { Card, Typography, Row, Col, Space, Spin } from 'antd';
import {
  LineChartOutlined,
  BarChartOutlined,
  PieChartOutlined,
  TwitterOutlined,
  VideoCameraOutlined,
  ClockCircleOutlined,
  PlayCircleOutlined,
  BellOutlined,
  RadarChartOutlined,
  SunOutlined,
} from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { twitterDailyReportsAPI } from '../services/twitterDailyReports';
import { youtubeDailyReportsAPI } from '../services/youtubeDailyReports';
import { marketWeeklyReportAPI } from '../services/marketWeeklyReport';
import { themeReportAPI } from '../services/themeReport';
import { dailyMarketReportAPI } from '../services/dailyMarketReport';
import { closeBriefReportAPI } from '../services/closeBriefReport';
import { closingBellReportAPI } from '../services/closingBellReport';
import { morningBriefReportAPI } from '../services/morningBriefReport';
import { surveillanceReportAPI } from '../services/surveillanceReport';
import dayjs from 'dayjs';

const { Title, Text } = Typography;

interface ReportCardProps {
  title: string;
  description: string;
  icon: React.ReactNode;
  path: string;
  color: string;
  disabled?: boolean;
  latestDate?: string | null;
  loading?: boolean;
}

const ReportCard: React.FC<ReportCardProps> = ({
  title,
  description,
  icon,
  path,
  color,
  disabled = false,
  latestDate,
  loading = false,
}) => {
  const navigate = useNavigate();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';

  const cardBg = isDark ? '#1a1d27' : '#ffffff';
  const borderColor = isDark ? '#2e3347' : '#e8ecf2';
  const hoverBg = isDark ? '#232736' : '#f5f7fa';

  // 格式化日期显示
  const formatDate = (date: string | null | undefined): string => {
    if (!date) return '';
    return dayjs(date).format('MMM DD, YYYY');
  };

  return (
    <Card
      hoverable={!disabled}
      onClick={() => !disabled && navigate(path)}
      style={{
        background: disabled ? (isDark ? '#15171e' : '#f0f0f0') : cardBg,
        border: `1px solid ${disabled ? (isDark ? '#1e2027' : '#d9d9d9') : borderColor}`,
        borderRadius: 12,
        cursor: disabled ? 'not-allowed' : 'pointer',
        opacity: disabled ? 0.6 : 1,
        transition: 'all 0.2s ease',
        height: 180, // 固定高度
      }}
      styles={{
        body: {
          padding: '20px 24px',
          height: '100%',
          display: 'flex',
          flexDirection: 'column',
        },
      }}
      onMouseEnter={(e) => {
        if (!disabled) {
          e.currentTarget.style.background = hoverBg;
          e.currentTarget.style.borderColor = color;
        }
      }}
      onMouseLeave={(e) => {
        if (!disabled) {
          e.currentTarget.style.background = cardBg;
          e.currentTarget.style.borderColor = borderColor;
        }
      }}
    >
      {/* Icon 区域 */}
      <div
        style={{
          width: 48,
          height: 48,
          borderRadius: 12,
          background: disabled
            ? (isDark ? '#1e2027' : '#f5f5f5')
            : `${color}15`,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          flexShrink: 0,
        }}
      >
        <span style={{ fontSize: 24, color: disabled ? (isDark ? '#4a4a5a' : '#bfbfbf') : color }}>
          {icon}
        </span>
      </div>

      {/* 标题和描述 */}
      <div style={{ flex: 1, marginTop: 12 }}>
        <Title
          level={5}
          style={{
            margin: 0,
            color: disabled
              ? (isDark ? '#6a6a7a' : '#8c8c8c')
              : (isDark ? '#e0e6ed' : '#262626'),
          }}
        >
          {title}
        </Title>
        <Text
          style={{
            fontSize: 13,
            color: disabled
              ? (isDark ? '#4a4a5a' : '#bfbfbf')
              : (isDark ? '#8a8a9a' : '#595959'),
            display: 'block',
            marginTop: 4,
          }}
        >
          {description}
        </Text>
      </div>

      {/* 底部日期/状态 */}
      <div style={{ marginTop: 'auto', flexShrink: 0 }}>
        {!disabled && (
          <>
            {loading ? (
              <Spin size="small" />
            ) : latestDate ? (
              <Space size={4} style={{ fontSize: 12 }}>
                <ClockCircleOutlined style={{ color: isDark ? '#6a6a7a' : '#8c8c8c' }} />
                <Text style={{ fontSize: 12, color: isDark ? '#6a6a7a' : '#8c8c8c' }}>
                  Latest: {formatDate(latestDate)}
                </Text>
              </Space>
            ) : (
              <Text style={{ fontSize: 12, color: isDark ? '#5a5a6a' : '#bfbfbf' }}>
                No reports yet
              </Text>
            )}
          </>
        )}
        {disabled && (
          <Text
            style={{
              fontSize: 12,
              color: isDark ? '#5a5a6a' : '#8c8c8c',
            }}
          >
            Coming soon...
          </Text>
        )}
      </div>
    </Card>
  );
};

interface ReportCategoryProps {
  title: string;
  description?: string;
  cards: ReportCardProps[];
}

const ReportCategory: React.FC<ReportCategoryProps> = ({
  title,
  description,
  cards,
}) => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';

  return (
    <div style={{ marginBottom: 32 }}>
      <div style={{ marginBottom: 16 }}>
        <Title
          level={4}
          style={{
            margin: 0,
            color: isDark ? '#e0e6ed' : '#262626',
          }}
        >
          {title}
        </Title>
        {description && (
          <Text
            style={{
              fontSize: 13,
              color: isDark ? '#8a8a9a' : '#595959',
              marginTop: 4,
            }}
          >
            {description}
          </Text>
        )}
      </div>
      <Row gutter={[16, 16]}>
        {cards.map((card, index) => (
          <Col key={index} xs={24} sm={12} md={8} lg={6}>
            <ReportCard {...card} />
          </Col>
        ))}
      </Row>
    </div>
  );
};

// 报告日期状态
interface ReportDatesState {
  twitter: string | null;
  youtube: string | null;
  marketWeekly: string | null;
  theme: string | null;
  dailyMarket: string | null;
  closeBrief: string | null;
  closingBell: string | null;
  morningBrief: string | null;
  surveillance: string | null;
}

const ReportsOverviewPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';

  const bgColor = isDark ? '#0f1117' : '#f5f7fa';

  // 各报告的最新日期
  const [reportDates, setReportDates] = useState<ReportDatesState>({
    twitter: null,
    youtube: null,
    marketWeekly: null,
    theme: null,
    dailyMarket: null,
    closeBrief: null,
    closingBell: null,
    morningBrief: null,
    surveillance: null,
  });

  // 加载状态
  const [loading, setLoading] = useState({
    twitter: true,
    youtube: true,
    marketWeekly: true,
    theme: true,
    dailyMarket: true,
    closeBrief: true,
    closingBell: true,
    morningBrief: true,
    surveillance: true,
  });

  // 加载各报告的最新日期
  useEffect(() => {
    const loadReportDates = async () => {
      // Daily Market Report
      setLoading(prev => ({ ...prev, dailyMarket: true }));
      try {
        const dmrRes = await dailyMarketReportAPI.getLatest();
        if (dmrRes && dmrRes.report_date) {
          setReportDates(prev => ({ ...prev, dailyMarket: dmrRes.report_date }));
        }
      } catch { /* ignore */ }
      setLoading(prev => ({ ...prev, dailyMarket: false }));

      // Twitter
      setLoading(prev => ({ ...prev, twitter: true }));
      try {
        const twitterRes = await twitterDailyReportsAPI.getLatestReport();
        if (twitterRes.success && twitterRes.data?.report_date) {
          setReportDates(prev => ({ ...prev, twitter: twitterRes.data!.report_date }));
        }
      } catch { /* ignore */ }
      setLoading(prev => ({ ...prev, twitter: false }));

      // YouTube
      setLoading(prev => ({ ...prev, youtube: true }));
      try {
        const youtubeRes = await youtubeDailyReportsAPI.getLatestReport();
        if (youtubeRes.success && youtubeRes.data?.report_date) {
          setReportDates(prev => ({ ...prev, youtube: youtubeRes.data!.report_date }));
        }
      } catch { /* ignore */ }
      setLoading(prev => ({ ...prev, youtube: false }));

      // Market Weekly
      setLoading(prev => ({ ...prev, marketWeekly: true }));
      try {
        const marketWeeklyRes = await marketWeeklyReportAPI.getLatest();
        if (marketWeeklyRes && marketWeeklyRes.week_end) {
          setReportDates(prev => ({ ...prev, marketWeekly: marketWeeklyRes.week_end }));
        }
      } catch { /* ignore */ }
      setLoading(prev => ({ ...prev, marketWeekly: false }));

      // Theme
      setLoading(prev => ({ ...prev, theme: true }));
      try {
        const themeRes = await themeReportAPI.getLatest();
        if (themeRes && themeRes.report_date) {
          setReportDates(prev => ({ ...prev, theme: themeRes.report_date }));
        }
      } catch { /* ignore */ }
      setLoading(prev => ({ ...prev, theme: false }));

      // The Close Report
      setLoading(prev => ({ ...prev, closeBrief: true }));
      try {
        const cbRes = await closeBriefReportAPI.getLatest();
        if (cbRes && cbRes.report_date) {
          setReportDates(prev => ({ ...prev, closeBrief: cbRes.report_date }));
        }
      } catch { /* ignore */ }
      setLoading(prev => ({ ...prev, closeBrief: false }));

      // Closing Bell Report
      setLoading(prev => ({ ...prev, closingBell: true }));
      try {
        const cbellRes = await closingBellReportAPI.getLatest();
        if (cbellRes && cbellRes.report_date) {
          setReportDates(prev => ({ ...prev, closingBell: cbellRes.report_date }));
        }
      } catch { /* ignore */ }
      setLoading(prev => ({ ...prev, closingBell: false }));

      // Morning Brief (facts-report)
      setLoading(prev => ({ ...prev, morningBrief: true }));
      try {
        const mbRes = await morningBriefReportAPI.getLatest();
        if (mbRes && mbRes.report_date) {
          setReportDates(prev => ({ ...prev, morningBrief: mbRes.report_date }));
        }
      } catch { /* ignore */ }
      setLoading(prev => ({ ...prev, morningBrief: false }));

      // Bloomberg Surveillance Report
      setLoading(prev => ({ ...prev, surveillance: true }));
      try {
        const survRes = await surveillanceReportAPI.getLatest();
        if (survRes && survRes.report_date) {
          setReportDates(prev => ({ ...prev, surveillance: survRes.report_date }));
        }
      } catch { /* ignore */ }
      setLoading(prev => ({ ...prev, surveillance: false }));
    };

    loadReportDates();
  }, []);

  // Market Reports 分类
  const marketReportsCards: ReportCardProps[] = [
    {
      title: 'Daily Market Report',
      description: 'Daily market analysis and key highlights',
      icon: <LineChartOutlined />,
      path: '/reports/daily-market',
      color: '#1890ff',
      latestDate: reportDates.dailyMarket,
      loading: loading.dailyMarket,
    },
    {
      title: 'Weekly Market Report',
      description: 'Weekly market summary and trends analysis',
      icon: <BarChartOutlined />,
      path: '/reports/market-weekly',
      color: '#52c41a',
      latestDate: reportDates.marketWeekly,
      loading: loading.marketWeekly,
    },
    {
      title: 'Theme Performance Report',
      description: 'Investment theme tracking and performance',
      icon: <PieChartOutlined />,
      path: '/reports/theme',
      color: '#722ed1',
      latestDate: reportDates.theme,
      loading: loading.theme,
    },
  ];

  // Market News 分类
  const marketNewsCards: ReportCardProps[] = [
    {
      title: 'Twitter Daily Report',
      description: 'Daily digest from Twitter subscriptions',
      icon: <TwitterOutlined />,
      path: '/reports/twitter',
      color: '#1DA1F2',
      latestDate: reportDates.twitter,
      loading: loading.twitter,
    },
    {
      title: 'YouTube Daily Report',
      description: 'Daily digest from YouTube subscriptions',
      icon: <VideoCameraOutlined />,
      path: '/reports/youtube',
      color: '#FF0000',
      latestDate: reportDates.youtube,
      loading: loading.youtube,
    },
    {
      title: 'The Close Report',
      description: 'Bloomberg The Close daily PM brief',
      icon: <PlayCircleOutlined />,
      path: '/reports/close-brief',
      color: '#faad14',
      latestDate: reportDates.closeBrief,
      loading: loading.closeBrief,
    },
    {
      title: 'Closing Bell Report',
      description: 'CNBC Closing Bell daily PM brief',
      icon: <BellOutlined />,
      path: '/reports/closing-bell',
      color: '#13c2c2',
      latestDate: reportDates.closingBell,
      loading: loading.closingBell,
    },
    {
      title: 'Morning Brief',
      description: 'facts-report LLM daily brief (auto, 08:00 Beijing)',
      icon: <SunOutlined />,
      path: '/reports/morning-brief',
      color: '#fa8c16',
      latestDate: reportDates.morningBrief,
      loading: loading.morningBrief,
    },
    {
      title: 'Bloomberg Surveillance Report',
      description: 'Bloomberg Surveillance daily AM brief',
      icon: <RadarChartOutlined />,
      path: '/reports/surveillance',
      color: '#2f54eb',
      latestDate: reportDates.surveillance,
      loading: loading.surveillance,
    },
  ];

  return (
    <div
      style={{
        padding: '24px 32px',
        background: bgColor,
        minHeight: '100%',
        overflow: 'auto',
      }}
    >
      <Title
        level={3}
        style={{
          marginBottom: 32,
          color: isDark ? '#e0e6ed' : '#262626',
        }}
      >
        Reports
      </Title>

      <ReportCategory
        title="Market Reports"
        description="Market analysis and investment tracking reports"
        cards={marketReportsCards}
      />

      <ReportCategory
        title="Market News"
        description="Daily news digests from social media subscriptions"
        cards={marketNewsCards}
      />
    </div>
  );
};

export default ReportsOverviewPage;
import React, { useState, useEffect } from 'react';
import { ConfigProvider, Spin, Typography, Space, theme as antdTheme } from 'antd';
import {
  LineChartOutlined,
  BarChartOutlined,
  PieChartOutlined,
  TwitterOutlined,
  VideoCameraOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import dayjs from 'dayjs';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import { MOBILE_TASK_PROGRESS_BAR_HEIGHT } from '../components/mobile/MobileTaskProgressBar';
import { twitterDailyReportsAPI } from '../services/twitterDailyReports';
import { youtubeDailyReportsAPI } from '../services/youtubeDailyReports';
import { marketWeeklyReportAPI } from '../services/marketWeeklyReport';
import { themeReportAPI } from '../services/themeReport';
import { dailyMarketReportAPI } from '../services/dailyMarketReport';

const { Title, Text } = Typography;

const HEADER_HEIGHT = 56;
const BOTTOM_NAV_HEIGHT = 60;

interface ReportCardProps {
  title: string;
  description: string;
  icon: React.ReactNode;
  path: string;
  color: string;
  latestDate?: string | null;
  loading?: boolean;
}

const formatDate = (date: string | null | undefined): string => {
  if (!date) return '';
  return dayjs(date).format('MMM DD, YYYY');
};

const ReportCard: React.FC<ReportCardProps> = ({
  title,
  description,
  icon,
  path,
  color,
  latestDate,
  loading = false,
}) => {
  const navigate = useNavigate();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';

  const cardBg = isDark ? '#1a1d27' : '#ffffff';
  const borderColor = isDark ? '#2e3347' : '#e8ecf2';
  const hoverBg = isDark ? '#232736' : '#f5f7fa';

  return (
    <div
      onClick={() => navigate(path)}
      style={{
        background: cardBg,
        border: `1px solid ${borderColor}`,
        borderRadius: 12,
        padding: '14px 16px',
        display: 'flex',
        alignItems: 'center',
        gap: 14,
        cursor: 'pointer',
        transition: 'background 0.2s ease',
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.background = hoverBg;
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = cardBg;
      }}
    >
      {/* Icon */}
      <div
        style={{
          width: 44,
          height: 44,
          borderRadius: 10,
          background: `${color}15`,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          flexShrink: 0,
        }}
      >
        <span style={{ fontSize: 22, color }}>{icon}</span>
      </div>

      {/* Text */}
      <div style={{ flex: 1, minWidth: 0 }}>
        <Title
          level={5}
          style={{ margin: 0, color: isDark ? '#e0e6ed' : '#262626' }}
          ellipsis
        >
          {title}
        </Title>
        <Text
          style={{
            fontSize: 12,
            color: isDark ? '#8a8a9a' : '#595959',
            display: 'block',
            marginTop: 2,
          }}
          ellipsis
        >
          {description}
        </Text>
        {loading ? (
          <Spin size="small" style={{ marginTop: 6 }} />
        ) : latestDate ? (
          <Space size={4} style={{ fontSize: 11, marginTop: 6 }}>
            <ClockCircleOutlined style={{ color: isDark ? '#6a6a7a' : '#8c8c8c' }} />
            <Text style={{ fontSize: 11, color: isDark ? '#6a6a7a' : '#8c8c8c' }}>
              Latest: {formatDate(latestDate)}
            </Text>
          </Space>
        ) : (
          <Text style={{ fontSize: 11, color: isDark ? '#5a5a6a' : '#bfbfbf', marginTop: 6, display: 'block' }}>
            No reports yet
          </Text>
        )}
      </div>
    </div>
  );
};

interface ReportCategoryProps {
  title: string;
  description?: string;
  cards: ReportCardProps[];
}

const ReportCategory: React.FC<ReportCategoryProps> = ({ title, description, cards }) => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';

  return (
    <div style={{ marginBottom: 24 }}>
      <div style={{ marginBottom: 10 }}>
        <Title level={5} style={{ margin: 0, color: isDark ? '#e0e6ed' : '#262626' }}>
          {title}
        </Title>
        {description && (
          <Text style={{ fontSize: 12, color: isDark ? '#8a8a9a' : '#595959', marginTop: 2, display: 'block' }}>
            {description}
          </Text>
        )}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        {cards.map((card, index) => (
          <ReportCard key={index} {...card} />
        ))}
      </div>
    </div>
  );
};

interface ReportDatesState {
  twitter: string | null;
  youtube: string | null;
  marketWeekly: string | null;
  theme: string | null;
  dailyMarket: string | null;
}

const MobileReportsOverviewPageInner: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const { tasks } = useFileProcessing();

  const chromeHeight =
    HEADER_HEIGHT + BOTTOM_NAV_HEIGHT + (tasks.size > 0 ? MOBILE_TASK_PROGRESS_BAR_HEIGHT : 0);

  const [reportDates, setReportDates] = useState<ReportDatesState>({
    twitter: null,
    youtube: null,
    marketWeekly: null,
    theme: null,
    dailyMarket: null,
  });

  const [loading, setLoading] = useState({
    twitter: true,
    youtube: true,
    marketWeekly: true,
    theme: true,
    dailyMarket: true,
  });

  useEffect(() => {
    const loadReportDates = async () => {
      // Daily Market
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
    };

    loadReportDates();
  }, []);

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
  ];

  return (
    <div
      style={{
        height: `calc(100dvh - ${chromeHeight}px)`,
        overflowY: 'auto',
        WebkitOverflowScrolling: 'touch',
        padding: `16px max(16px, env(safe-area-inset-left, 0px)) max(24px, env(safe-area-inset-bottom, 0px)) max(16px, env(safe-area-inset-right, 0px))`,
        background: isDark ? '#0f1117' : '#f5f7fa',
      }}
    >
      <Title level={4} style={{ marginBottom: 18, color: isDark ? '#e0e6ed' : '#262626' }}>
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

const MobileReportsOverviewPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        components: {
          Typography: {
            colorText: isDark ? '#e0e6ed' : '#262626',
          },
        },
      }}
    >
      <MobileReportsOverviewPageInner />
    </ConfigProvider>
  );
};

export default MobileReportsOverviewPage;

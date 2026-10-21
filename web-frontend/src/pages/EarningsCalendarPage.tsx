import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import {
  Card,
  Typography,
  Table,
  Space,
  Input,
  message,
  Tag,
  Tooltip,
  Button,
  ConfigProvider,
  theme as antdTheme,
  Segmented,
  Empty,
} from 'antd';
import {
  LeftOutlined,
  RightOutlined,
  CloseOutlined,
} from '@ant-design/icons';
import dayjs, { Dayjs } from 'dayjs';
import {
  marketsAPI,
  EarningsCalendarItem,
  EarningsCalendarData,
  EarningsMonthSummary,
  EarningsCompanyPanel,
  EarningsFeedData,
} from '../services/markets';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import WeekBoard from '../components/EarningsCalendar/WeekBoard';
import MonthBoard from '../components/EarningsCalendar/MonthBoard';
import CompanyPanel from '../components/EarningsCalendar/CompanyPanel';
import FeedList from '../components/EarningsCalendar/FeedList';
import './EarningsCalendarPage.css';

const { Title, Text } = Typography;

const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    neutralTagBg: 'rgba(128, 128, 128, 0.1)',
    neutralTagColor: '#666',
    neutralTagBorder: 'rgba(128, 128, 128, 0.3)',
    beat: '#389e0d',
    miss: '#cf1322',
  },
  dark: {
    pageBg: '#1a202c',
    cardBg: '#1a2234',
    cardBorder: '#364152',
    text: '#e0e6f1',
    textSecondary: 'rgba(255, 255, 255, 0.5)',
    neutralTagBg: 'rgba(148, 163, 184, 0.15)',
    neutralTagColor: '#94a3b8',
    neutralTagBorder: 'rgba(148, 163, 184, 0.35)',
    beat: '#95de64',
    miss: '#ff7875',
  },
} as const;

type Scope = 'watchlist' | 'theme' | 'us' | 'hk';
type ViewMode = 'calendar' | 'month' | 'table';

/** 取某日期所在周的周一 */
const getMonday = (d: Dayjs): Dayjs => {
  const dow = d.day(); // 0=周日
  return d.subtract(dow === 0 ? 6 : dow - 1, 'day').startOf('day');
};

const formatEps = (v?: number | null): string =>
  v === null || v === undefined ? '—' : v.toFixed(2);

const EarningsCalendarPage: React.FC = () => {
  const { isAuthenticated } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];

  const [scope, setScope] = useState<Scope>('watchlist');
  const [viewMode, setViewMode] = useState<ViewMode>('calendar');
  const [weekStart, setWeekStart] = useState<Dayjs>(getMonday(dayjs()));
  const [monthStart, setMonthStart] = useState<Dayjs>(dayjs().startOf('month'));

  // 市值筛选(USD, 港股已按汇率换算), 默认 $10B+(对齐 EarningsHub)
  const [mcapFilter, setMcapFilter] = useState<string>('30');
  const mcapUsd = mcapFilter !== '0' ? Number(mcapFilter) * 1e9 : undefined;

  const [searchText, setSearchText] = useState('');
  const [data, setData] = useState<EarningsCalendarData | null>(null);
  const [monthData, setMonthData] = useState<EarningsMonthSummary | null>(null);
  const [loading, setLoading] = useState(false);

  // 拉取窗口：所选周周一 ~ 周五（Calendar/Table/Feed 三个视图同一窗口, 数量保持一致）
  const fetchCalendar = useCallback(async () => {
    setLoading(true);
    try {
      const result = await marketsAPI.getEarningsCalendar({
        from_date: weekStart.format('YYYY-MM-DD'),
        to_date: weekStart.add(4, 'day').format('YYYY-MM-DD'),
        scope,
        search: searchText.trim() || undefined,
        min_market_cap: mcapUsd,
        limit: 2000,
      });
      setData(result);
    } catch (error) {
      console.error('Failed to get earnings calendar:', error);
      message.error('Failed to retrieve earnings calendar data');
    } finally {
      setLoading(false);
    }
  }, [scope, weekStart, searchText, mcapUsd]);

  // 月视图汇总（只应用 scope/search；月视图是总览，明细筛选不参与）
  const fetchMonthSummary = useCallback(async () => {
    setLoading(true);
    try {
      const result = await marketsAPI.getEarningsCalendarMonthSummary(
        monthStart.format('YYYY-MM'),
        scope,
        searchText.trim() || undefined,
        mcapUsd
      );
      setMonthData(result);
    } catch (error) {
      console.error('Failed to get earnings month summary:', error);
      message.error('Failed to retrieve earnings month summary');
    } finally {
      setLoading(false);
    }
  }, [scope, monthStart, searchText, mcapUsd]);

  useEffect(() => {
    if (isAuthenticated && viewMode !== 'month') {
      fetchCalendar();
    }
  }, [fetchCalendar, isAuthenticated, viewMode]);

  useEffect(() => {
    if (isAuthenticated && viewMode === 'month') {
      fetchMonthSummary();
    }
  }, [fetchMonthSummary, isAuthenticated, viewMode]);

  /** 月视图点击某天 → 跳到该天的周视图(并直接选中点击的那天) */
  const handleMonthDayClick = useCallback(
    (date: string) => {
      monthJumpDateRef.current = date;
      setWeekStart(getMonday(dayjs(date)));
      setViewMode('calendar');
    },
    []
  );

  // 右侧公司详情面板
  const [panelCode, setPanelCode] = useState<string | null>(null);
  // 所点事件的 report_date: 未来事件(未公布)不展示 Recap 按钮
  const [panelReportDate, setPanelReportDate] = useState<string | null>(null);
  const [panelData, setPanelData] = useState<EarningsCompanyPanel | null>(null);
  const [panelLoading, setPanelLoading] = useState(false);

  const openPanel = useCallback((code: string, reportDate?: string | null) => {
    setPanelCode(code);
    setPanelReportDate(reportDate ?? null);
  }, []);

  // Calendar 视图选中的日期(YYYY-MM-DD): 选中后右栏 Feed 只显示该日财报(对齐 EarningsHub 按日查看)
  const [selectedDate, setSelectedDate] = useState<string | null>(null);

  // 月视图跳转要选中的具体日期(effect 里优先消费, 否则会被默认规则覆盖)
  const monthJumpDateRef = useRef<string | null>(null);

  // 默认选中规则: 进入/翻动 Calendar 视图时, 当周包含今天则默认选中今天, 否则不选;
  // 非 Calendar 视图不选中(Feed 回到整周窗口)
  useEffect(() => {
    if (monthJumpDateRef.current) {
      setSelectedDate(monthJumpDateRef.current);
      monthJumpDateRef.current = null;
      return;
    }
    if (viewMode === 'calendar') {
      const today = dayjs();
      const inWeek = !today.isBefore(weekStart, 'day') && today.isBefore(weekStart.add(5, 'day'), 'day');
      setSelectedDate(inWeek ? today.format('YYYY-MM-DD') : null);
    } else {
      setSelectedDate(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [weekStart, viewMode]);

  // 右栏默认态: Earnings Feed（跟随当前视图窗口与范围; 明细筛选不参与）
  const [feedData, setFeedData] = useState<EarningsFeedData | null>(null);
  const [feedLoading, setFeedLoading] = useState(false);

  const feedWindow = useMemo(
    () =>
      viewMode === 'month'
        ? {
            from: monthStart.format('YYYY-MM-DD'),
            to: monthStart.endOf('month').format('YYYY-MM-DD'),
          }
        : selectedDate
        ? { from: selectedDate, to: selectedDate }
        : {
            // 与 Calendar/Table 同一窗口(所选周), 三视图数量一致
            from: weekStart.format('YYYY-MM-DD'),
            to: weekStart.add(4, 'day').format('YYYY-MM-DD'),
          },
    [viewMode, monthStart, weekStart, selectedDate]
  );

  const fetchFeed = useCallback(async () => {
    setFeedLoading(true);
    try {
      const result = await marketsAPI.getEarningsCalendarFeed({
        from_date: feedWindow.from,
        to_date: feedWindow.to,
        scope,
        search: searchText.trim() || undefined,
        min_market_cap: mcapUsd,
        limit: 150,
      });
      setFeedData(result);
    } catch (error) {
      console.error('Failed to get earnings feed:', error);
    } finally {
      setFeedLoading(false);
    }
  }, [feedWindow, scope, searchText, mcapUsd]);

  useEffect(() => {
    if (isAuthenticated) {
      fetchFeed();
    }
  }, [fetchFeed, isAuthenticated]);

  useEffect(() => {
    if (!isAuthenticated || !panelCode) return;
    let cancelled = false;
    setPanelLoading(true);
    setPanelData(null);
    marketsAPI
      .getEarningsCompanyPanel(panelCode)
      .then((result) => {
        if (!cancelled) setPanelData(result);
      })
      .catch((error) => {
        console.error('Failed to get company panel:', error);
        if (!cancelled) message.error('Failed to load company details');
      })
      .finally(() => {
        if (!cancelled) setPanelLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [panelCode, isAuthenticated]);

  const items = data?.items ?? [];
  const todayStr = dayjs().format('YYYY-MM-DD');
  const today = dayjs();

  const columns = [
    {
      title: 'Date',
      dataIndex: 'report_date',
      key: 'report_date',
      width: 130,
      sorter: (a: EarningsCalendarItem, b: EarningsCalendarItem) =>
        (a.report_date || '').localeCompare(b.report_date || ''),
      render: (date: string) =>
        date ? (
          <Space size={4}>
            <span>{dayjs(date).format('ddd, MMM D')}</span>
            {date === todayStr && <Tag color="blue">Today</Tag>}
          </Space>
        ) : (
          '—'
        ),
    },
    {
      title: 'Ticker',
      dataIndex: 'code',
      key: 'code',
      width: 120,
      render: (code: string, record: EarningsCalendarItem) => (
        <Space size={8}>
          {record.logo_url ? (
            <img
              src={record.logo_url}
              alt={code}
              style={{ width: 18, height: 18, objectFit: 'contain' }}
              onError={(e) => {
                e.currentTarget.style.display = 'none';
              }}
            />
          ) : null}
          <Text strong>{code}</Text>
        </Space>
      ),
    },
    {
      title: 'Company',
      dataIndex: 'company_name',
      key: 'company_name',
      ellipsis: { showTitle: false },
      render: (name?: string | null) =>
        name ? (
          <Tooltip title={name}>
            <span>{name}</span>
          </Tooltip>
        ) : (
          '—'
        ),
    },
    {
      title: 'Time',
      dataIndex: 'before_after_market',
      key: 'before_after_market',
      width: 110,
      render: (v?: string | null) => {
        const s = (v || '').toUpperCase();
        if (s.includes('BEFORE')) return <Tag color="orange">BMO</Tag>;
        if (s.includes('AFTER')) return <Tag color="geekblue">AMC</Tag>;
        return (
          <Tag
            style={{
              backgroundColor: colors.neutralTagBg,
              color: colors.neutralTagColor,
              border: `1px solid ${colors.neutralTagBorder}`,
            }}
          >
            N/A
          </Tag>
        );
      },
    },
    {
      title: 'EPS Est.',
      dataIndex: 'eps_estimate',
      key: 'eps_estimate',
      width: 90,
      align: 'right' as const,
      render: (v?: number | null) => (
        <span style={{ color: colors.textSecondary }}>{formatEps(v)}</span>
      ),
    },
    {
      title: 'EPS Actual',
      dataIndex: 'eps_actual',
      key: 'eps_actual',
      width: 100,
      align: 'right' as const,
      render: (v?: number | null) => formatEps(v),
    },
    {
      title: 'Surprise',
      dataIndex: 'eps_surprise_percent',
      key: 'eps_surprise_percent',
      width: 100,
      align: 'right' as const,
      render: (v?: number | null) => {
        if (v === null || v === undefined) {
          return (
            <Tag
              style={{
                backgroundColor: colors.neutralTagBg,
                color: colors.neutralTagColor,
                border: `1px solid ${colors.neutralTagBorder}`,
              }}
            >
              Pending
            </Tag>
          );
        }
        const isBeat = v >= 0;
        return (
          <Text strong style={{ color: isBeat ? colors.beat : colors.miss }}>
            {isBeat ? '▲' : '▼'} {Math.abs(v).toFixed(1)}%
          </Text>
        );
      },
    },
  ];

  const scopeOptions = [
    { label: 'Watchlist', value: 'watchlist' },
    { label: 'US', value: 'us' },
    { label: 'HK', value: 'hk' },
  ];

  if (!isAuthenticated) {
    return (
      <ConfigProvider
        theme={{
          algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
          ...(isDark && {
            token: {
              colorBgContainer: colors.cardBg,
              colorBorderSecondary: colors.cardBorder,
            },
          }),
        }}
      >
        <div style={{ padding: '24px', textAlign: 'center', backgroundColor: colors.pageBg, minHeight: '100%' }}>
          <Card>
            <Title level={3}>Login Required</Title>
            <Text type="secondary">Please login to access earnings calendar data</Text>
          </Card>
        </div>
      </ConfigProvider>
    );
  }

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: colors.cardBg,
            colorBorderSecondary: colors.cardBorder,
          },
        }),
      }}
    >
      <div
        style={{
          // 顶部 padding 为 0: 查询卡片顶边与左侧导航栏顶边(viewport y=12px)对齐
          padding: '0 24px 24px',
          // 与 .layout-content 的 min-height calc(100vh - 2*inset) 对齐, 拉满白色卡片高度
          height: 'calc(100vh - 24px)',
          backgroundColor: colors.pageBg,
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
        }}
      >
        {/* 内容区: 左列 = 查询筛选栏 + 视图卡片; 右列 = 股票信息栏(顶边与左侧导航栏齐平) */}
        <div className="ec-split">
          <div className="ec-split-left">
            {/* 控制面板: 查询控件按全站规范(styles/control-style.css), 仅占左列(右侧与下方视图卡片对齐) */}
            <Card className="app-controls" style={{ marginBottom: 16 }}>
          {/* 全部筛选项左对齐单行流式布局(窄屏自动换行), 顺序: 视图 → 范围 → 市值 → 周/月导航 → 搜索 */}
          <Space size={12} wrap>
            <Segmented
              options={[
                { label: 'Calendar', value: 'calendar' },
                { label: 'Month', value: 'month' },
                { label: 'Table', value: 'table' },
              ]}
              value={viewMode}
              onChange={(v) => setViewMode(v as ViewMode)}
            />
            <Segmented options={scopeOptions} value={scope} onChange={(v) => setScope(v as Scope)} />
            {/* 市值筛选: 三视图通用, 港股已按汇率换算为 USD(默认 $30B+) */}
            <Segmented
              options={[
                { label: 'No Filter', value: '0' },
                { label: '$100B+', value: '100' },
                { label: '$30B+', value: '30' },
                { label: '$10B+', value: '10' },
                { label: '$1B+', value: '1' },
              ]}
              value={mcapFilter}
              onChange={(v) => setMcapFilter(v as string)}
            />
            {viewMode === 'calendar' && (
              <Space size={4}>
                <Button
                  icon={<LeftOutlined />}
                  onClick={() => setWeekStart(weekStart.subtract(1, 'week'))}
                />
                <Button size="small" onClick={() => setWeekStart(getMonday(dayjs()))}>
                  W{' '}
                  {today.isBefore(weekStart.add(5, 'day'), 'day') && !today.isBefore(weekStart, 'day')
                    ? today.date()
                    : weekStart.date()}
                </Button>
                <Button
                  icon={<RightOutlined />}
                  onClick={() => setWeekStart(weekStart.add(1, 'week'))}
                />
              </Space>
            )}
            {viewMode === 'month' && (
              <Space size={4}>
                <Button
                  icon={<LeftOutlined />}
                  onClick={() => setMonthStart(monthStart.subtract(1, 'month'))}
                />
                <Button size="small" onClick={() => setMonthStart(dayjs().startOf('month'))}>
                  {monthStart.format('MMM YYYY')}
                </Button>
                <Button
                  icon={<RightOutlined />}
                  onClick={() => setMonthStart(monthStart.add(1, 'month'))}
                />
              </Space>
            )}
            <Input.Search
              placeholder="Search ticker or company"
              allowClear
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              onSearch={fetchCalendar}
              style={{ width: 220 }}
            />
          </Space>
            </Card>

            {/* 视图卡片(Calendar / Month / Table) */}
            <Card
              className="ec-split-main"
            style={{ overflow: 'hidden', display: 'flex', flexDirection: 'column' }}
            styles={{ body: { padding: '12px', flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflow: 'auto' } }}
          >
            {viewMode === 'month' ? (
              <MonthBoard
                data={monthData}
                monthStart={monthStart}
                isDark={isDark}
                onDayClick={handleMonthDayClick}
              />
            ) : viewMode === 'calendar' ? (
              loading ? (
                <Empty description="Loading..." style={{ padding: '60px 0' }} />
              ) : items.length === 0 ? (
                <Empty
                  description={
                    scope === 'watchlist'
                      ? 'No earnings found for your watchlist in this window'
                      : 'No earnings found for the selected filters'
                  }
                  style={{ padding: '60px 0' }}
                />
              ) : (
                <WeekBoard
                  data={items}
                  weekStart={weekStart}
                  isDark={isDark}
                  onSelect={openPanel}
                  selectedDate={selectedDate}
                  onDateSelect={(d) => {
                    // 点日期 = 想看该日财报列表, 右栏从公司详情切回 Feed
                    setSelectedDate(d);
                    setPanelCode(null);
                  }}
                />
              )
            ) : (
              <Table
                columns={columns}
                dataSource={items}
                rowKey={(r) => `${r.code}-${r.report_date}`}
                loading={loading}
                size="middle"
                onRow={(r) => ({
                  onClick: () => openPanel(r.code, r.report_date),
                  style: { cursor: 'pointer' },
                })}
                pagination={{ pageSize: 50, showSizeChanger: false, showTotal: (t) => `${t} reports` }}
                scroll={{ x: 800 }}
              />
            )}
          </Card>
          </div>

          {/* 右侧: 未选中时为 Earnings Feed 事件流, 选中后为公司详情; 独立整列, 顶边与导航栏齐平 */}
          <Card
            className="ec-split-panel"
            style={{ overflow: 'hidden', display: 'flex', flexDirection: 'column' }}
            styles={{
              body: {
                padding: '12px 16px',
                flex: 1,
                minHeight: 0,
                overflow: 'hidden',
                display: 'flex',
                flexDirection: 'column',
              },
            }}
            title={
              <span style={{ fontSize: 14 }}>
                {panelCode
                  ? panelData?.company?.name || panelCode
                  : selectedDate && viewMode === 'calendar'
                  ? `Earnings · ${dayjs(selectedDate).format('ddd, MMM D')}${
                      feedData ? ` · ${feedData.meta.reported} reported` : ''
                    }`
                  : `Earnings Feed${feedData ? ` · ${feedData.meta.reported} reported` : ''}`}
              </span>
            }
            extra={
              panelCode ? (
                <Button
                  size="small"
                  type="text"
                  icon={<CloseOutlined />}
                  onClick={() => setPanelCode(null)}
                />
              ) : null
            }
          >
            {panelCode ? (
              <CompanyPanel
                code={panelCode}
                data={panelData}
                loading={panelLoading}
                isDark={isDark}
                reportDate={panelReportDate}
              />
            ) : feedLoading && !feedData ? (
              <Empty description="Loading feed..." style={{ padding: '60px 0' }} />
            ) : (
              /* Feed 态: 列表在面板内滚动(标题栏固定), 滚动条静止时隐藏 */
              <div className="ec-scroll-hidden" style={{ flex: 1, minHeight: 0 }}>
                <FeedList items={feedData?.items ?? []} isDark={isDark} onSelect={openPanel} />
              </div>
            )}
          </Card>
        </div>
      </div>
    </ConfigProvider>
  );
};

export default EarningsCalendarPage;

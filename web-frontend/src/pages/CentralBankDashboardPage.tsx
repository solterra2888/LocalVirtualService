import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Card,
  Typography,
  Button,
  Space,
  message,
  Tag,
  Tooltip,
  Spin,
  Skeleton,
  Empty,
  Tabs,
  List,
  Switch,
  Pagination,
  Collapse,
} from 'antd';
import {
  ReloadOutlined,
  BankOutlined,
  LinkOutlined,
  RiseOutlined,
  FallOutlined,
  MinusOutlined,
  RobotOutlined,
  FileTextOutlined,
} from '@ant-design/icons';
import { fomcAPI, DashboardData, SummaryInfo, UpdateItem } from '../services/fomc';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import MarkdownRenderer from '../components/MarkdownRenderer';

const { Title, Text, Paragraph } = Typography;

const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    hawkBg: 'rgba(255, 77, 79, 0.1)',
    hawkColor: '#cf1322',
    doveBg: 'rgba(22, 119, 255, 0.1)',
    doveColor: '#1677ff',
    neutralTagBg: 'rgba(128, 128, 128, 0.1)',
    neutralTagColor: '#666',
  },
  dark: {
    pageBg: '#1a202c',
    cardBg: '#1a2234',
    cardBorder: '#364152',
    text: '#e0e6f1',
    textSecondary: 'rgba(255, 255, 255, 0.5)',
    hawkBg: 'rgba(255, 77, 79, 0.15)',
    hawkColor: '#ff7875',
    doveBg: 'rgba(22, 119, 255, 0.15)',
    doveColor: '#69b1ff',
    neutralTagBg: 'rgba(148, 163, 184, 0.15)',
    neutralTagColor: '#94a3b8',
  },
} as const;

const fmtBp = (bp?: number | null) =>
  bp === null || bp === undefined ? '—' : `${(bp / 100).toFixed(2)}%`;

const CentralBankDashboardPage: React.FC = () => {
  const { isAuthenticated } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const colors = THEME_COLORS[themeMode];

  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [data, setData] = useState<DashboardData | null>(null);

  // updates 流
  const [monetaryOnly, setMonetaryOnly] = useState(false);
  const [updates, setUpdates] = useState<UpdateItem[]>([]);
  const [updatesTotal, setUpdatesTotal] = useState(0);
  const [updatesPage, setUpdatesPage] = useState(1);
  const [updatesLoading, setUpdatesLoading] = useState(false);

  // generating 态轮询（照 useDailyReportStatusPoll 模式）
  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pollCount = useRef(0);

  const fetchDashboard = useCallback(async () => {
    setLoading(true);
    try {
      const d = await fomcAPI.getDashboard();
      setData(d);
    } catch (error) {
      console.error('Failed to load central bank dashboard:', error);
      message.error('Failed to load central bank dashboard');
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchUpdates = useCallback(async (page: number, monetary: boolean) => {
    setUpdatesLoading(true);
    try {
      const r = await fomcAPI.getUpdates({
        page, page_size: 10, monetary_only: monetary,
      });
      setUpdates(r.items);
      setUpdatesTotal(r.total);
    } catch (error) {
      console.error('Failed to load updates:', error);
    } finally {
      setUpdatesLoading(false);
    }
  }, []);

  // 初始加载
  useEffect(() => {
    if (isAuthenticated) {
      fetchDashboard();
      fetchUpdates(1, false);
    }
  }, [isAuthenticated, fetchDashboard, fetchUpdates]);

  // generating 态自动轮询（最长 ~2.5 分钟）
  useEffect(() => {
    const anyGenerating =
      data?.statement_summary?.status === 'generating' ||
      data?.minutes_summary?.status === 'generating';
    if (!anyGenerating) {
      pollCount.current = 0;
      return;
    }
    if (pollCount.current > 60) return; // 上限保护
    pollRef.current = setTimeout(() => {
      pollCount.current += 1;
      fomcAPI.getDashboard().then(setData).catch(() => {});
    }, 5000);
    return () => {
      if (pollRef.current) clearTimeout(pollRef.current);
    };
  }, [data]);

  // 手动刷新（派发 poll 任务）
  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      await fomcAPI.refresh('poll');
      message.success('Refresh dispatched — new data will appear shortly');
      setTimeout(() => {
        fetchDashboard();
        fetchUpdates(updatesPage, monetaryOnly);
      }, 4000);
    } catch (error) {
      message.error('Failed to dispatch refresh');
    } finally {
      setRefreshing(false);
    }
  };

  const decision = data?.decision;
  const dirIcon = (dir?: string | null) =>
    dir === 'hike' ? <RiseOutlined style={{ color: colors.hawkColor }} />
    : dir === 'cut' ? <FallOutlined style={{ color: colors.doveColor }} />
    : <MinusOutlined style={{ color: colors.neutralTagColor }} />;

  const dirLabel: Record<string, string> = {
    hike: 'Hike', cut: 'Cut', hold: 'Hold',
  };

  return (
    <div style={{ padding: '24px', background: colors.pageBg, minHeight: '100vh' }}>
      {/* Header */}
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <BankOutlined style={{ fontSize: 22, color: colors.text }} />
          <Title level={4} style={{ color: colors.text, margin: 0 }}>
            Central Bank Dashboard
          </Title>
          <Tag color="blue">Federal Reserve</Tag>
        </Space>
        <Button
          icon={<ReloadOutlined spin={refreshing} />}
          onClick={handleRefresh}
          loading={refreshing}
        >
          Refresh
        </Button>
      </div>

      {loading && !data ? (
        <Skeleton active paragraph={{ rows: 8 }} />
      ) : !data ? (
        <Empty description="No data — click Refresh to poll the Fed RSS feed" />
      ) : (
        <>
          {/* ═══ 卡片 1: 利率决议 ═══ */}
          <Card
            size="small"
            style={{ marginBottom: 16, background: colors.cardBg, borderColor: colors.cardBorder }}
            loading={loading}
            title={<Text strong style={{ color: colors.text }}>FOMC Rate Decision</Text>}
            extra={
              decision?.source_url ? (
                <Tooltip title="View statement on federalreserve.gov">
                  <Button
                    type="link" size="small"
                    icon={<LinkOutlined />}
                    href={decision.source_url}
                    target="_blank"
                  >
                    Fed Statement
                  </Button>
                </Tooltip>
              ) : null
            }
          >
            {decision ? (
              <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap', alignItems: 'center' }}>
                {/* 区间 */}
                <div>
                  <Text type="secondary" style={{ fontSize: 12 }}>Fed Funds Target Range</Text>
                  <div style={{ fontSize: 28, fontWeight: 700, color: colors.text, lineHeight: 1.2 }}>
                    {fmtBp(decision.rate_low_bp)} – {fmtBp(decision.rate_high_bp)}
                  </div>
                  <Space size={4} style={{ marginTop: 4 }}>
                    {dirIcon(decision.decision_direction)}
                    <Text style={{ color: colors.textSecondary, fontSize: 13 }}>
                      {dirLabel[decision.decision_direction ?? ''] ?? ''}
                      {decision.change_bp ? ` ${decision.change_bp > 0 ? '+' : ''}${decision.change_bp}bp` : ''}
                    </Text>
                  </Space>
                </div>

                {/* 票型 */}
                <div>
                  <Text type="secondary" style={{ fontSize: 12 }}>Vote</Text>
                  <div style={{ fontSize: 28, fontWeight: 700, color: colors.text, lineHeight: 1.2 }}>
                    {decision.vote_for ?? '–'} : {decision.vote_against ?? '–'}
                  </div>
                  {decision.previous && (
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      prev meeting {decision.previous.meeting_date}: {(decision.previous.rate_high_bp / 100).toFixed(2)}%
                    </Text>
                  )}
                </div>

                {/* 异议者 */}
                <div style={{ flex: 1, minWidth: 220 }}>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    Dissents ({decision.dissenters.length})
                  </Text>
                  <div style={{ marginTop: 6, display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                    {decision.dissenters.length === 0 ? (
                      <Text type="secondary" style={{ fontSize: 13 }}>None — unanimous</Text>
                    ) : decision.dissenters.map((d) => (
                      <Tooltip key={d.name} title={d.direction === 'hawkish' ? 'Preferred to raise rates (hawkish)' : 'Preferred to lower rates (dovish)'}>
                        <Tag
                          style={{
                            background: d.direction === 'hawkish' ? colors.hawkBg : colors.doveBg,
                            color: d.direction === 'hawkish' ? colors.hawkColor : colors.doveColor,
                            border: 'none',
                          }}
                        >
                          {d.name}{d.direction === 'hawkish' ? ' 🦅' : ' 🕊'}
                        </Tag>
                      </Tooltip>
                    ))}
                  </div>
                </div>

                {/* 会议日期 + EODHD 交叉验证 */}
                <div>
                  <Text type="secondary" style={{ fontSize: 12 }}>Meeting</Text>
                  <div style={{ color: colors.text, fontSize: 15 }}>
                    {decision.meeting_date ?? '—'}
                  </div>
                  <Tooltip title={`EODHD cross-check: actual ${decision.eodhd_cross_check?.actual ?? 'n/a'} / forecast ${decision.eodhd_cross_check?.forecast ?? 'n/a'} — from economic_events`}>
                    <Tag
                      style={{
                        background: colors.neutralTagBg,
                        color: decision.eodhd_cross_check
                          ? (decision.eodhd_cross_check.actual !== null &&
                             decision.rate_high_bp !== null &&
                             decision.eodhd_cross_check.actual * 100 === decision.rate_high_bp)
                            ? '#52c41a' : '#faad14'
                        : colors.neutralTagColor,
                        border: 'none', marginTop: 4,
                      }}
                    >
                      {decision.eodhd_cross_check ? (
                        decision.eodhd_cross_check.actual !== null &&
                        decision.rate_high_bp !== null &&
                        decision.eodhd_cross_check.actual * 100 === decision.rate_high_bp
                          ? '✓ EODHD consistent' : '⚠ EODHD mismatch'
                      ) : 'EODHD n/a'}
                    </Tag>
                  </Tooltip>
                </div>
              </div>
            ) : (
              <Empty description="No FOMC decision data — run Refresh (poll)" />
            )}
          </Card>

          {/* ═══ 卡片 2: AI 概括（声明 / 纪要 Tabs）═══ */}
          <Card
            size="small"
            style={{ marginBottom: 16, background: colors.cardBg, borderColor: colors.cardBorder }}
            title={
              <Space>
                <RobotOutlined style={{ color: colors.text }} />
                <Text strong style={{ color: colors.text }}>AI Summary</Text>
              </Space>
            }
          >
            <Tabs
              defaultActiveKey="statement"
              items={[
                {
                  key: 'statement',
                  label: 'Statement',
                  children: <SummarySection summary={data.statement_summary} colors={colors} />,
                },
                {
                  key: 'minutes',
                  label: 'Minutes',
                  children: <SummarySection summary={data.minutes_summary} colors={colors} />,
                },
              ]}
            />
          </Card>

          {/* ═══ 卡片 3: Fed Updates 流 ═══ */}
          <Card
            size="small"
            style={{ marginBottom: 16, background: colors.cardBg, borderColor: colors.cardBorder }}
            title={
              <Space>
                <FileTextOutlined />
                <Text strong style={{ color: colors.text }}>Fed Updates</Text>
                <Tag style={{ background: colors.neutralTagBg, color: colors.neutralTagColor, border: 'none' }}>
                  press_all.xml RSS
                </Tag>
              </Space>
            }
            extra={
              <Space>
                <Text type="secondary" style={{ fontSize: 12 }}>Monetary Policy only</Text>
                <Switch
                  size="small"
                  checked={monetaryOnly}
                  onChange={(v) => {
                    setMonetaryOnly(v);
                    setUpdatesPage(1);
                    fetchUpdates(1, v);
                  }}
                />
              </Space>
            }
          >
            <Spin spinning={updatesLoading}>
              <List
                size="small"
                dataSource={updates}
                locale={{ emptyText: 'No updates' }}
                renderItem={(item) => (
                  <List.Item
                    style={{ padding: '8px 0' }}
                    actions={[
                      <Button
                        key="open" type="link" size="small"
                        icon={<LinkOutlined />}
                        href={item.link} target="_blank"
                      >
                        Fed
                      </Button>,
                    ]}
                  >
                    <List.Item.Meta
                      title={
                        <Space>
                          <Text style={{ color: colors.text, fontSize: 13 }}>{item.title}</Text>
                          {item.is_monetary_policy && <Tag color="blue">Monetary Policy</Tag>}
                        </Space>
                      }
                      description={
                        <Text type="secondary" style={{ fontSize: 12 }}>
                          {item.pub_date ? new Date(item.pub_date).toLocaleDateString('en-US', {
                            month: 'short', day: 'numeric', year: 'numeric',
                          }) : ''}
                          {item.category ? ` · ${item.category}` : ''}
                        </Text>
                      }
                    />
                  </List.Item>
                )}
              />
              <div style={{ textAlign: 'right', marginTop: 8 }}>
                <Pagination
                  size="small"
                  current={updatesPage}
                  pageSize={10}
                  total={updatesTotal}
                  showSizeChanger={false}
                  onChange={(p) => {
                    setUpdatesPage(p);
                    fetchUpdates(p, monetaryOnly);
                  }}
                />
              </div>
            </Spin>
          </Card>

          {/* ═══ 卡片 4: 概率占位（V1 缓做）═══ */}
          <Card
            size="small"
            style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
            title={<Text strong style={{ color: colors.text }}>Rate Probabilities (next meeting)</Text>}
          >
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description={
                <Text type="secondary">
                  Data source under research (CME FedWatch / ZQ futures) — coming in a later version
                </Text>
              }
            />
          </Card>
        </>
      )}
    </div>
  );
};

// ═══ AI 概括子组件（声明/纪要共用）═══

type ThemeColors = (typeof THEME_COLORS)[keyof typeof THEME_COLORS];

const SummarySection: React.FC<{
  summary?: SummaryInfo | null;
  colors: ThemeColors;
}> = ({ summary, colors }) => {
  const { themeMode } = useDashboardSettings();
  const [doc, setDoc] = useState<{ raw_content?: string | null; word_count?: number | null } | null>(null);
  const [docLoading, setDocLoading] = useState(false);

  useEffect(() => { setDoc(null); }, [summary?.document_id]);

  if (!summary) {
    return <Empty description="No document yet — will appear after the Fed publishes it" />;
  }

  if (summary.status === 'generating') {
    return (
      <div style={{ padding: '24px 0', textAlign: 'center' }}>
        <Spin tip="AI summary generating…" />
      </div>
    );
  }

  if (summary.status === 'failed') {
    return <Empty description="AI summary failed — will retry on next poll" />;
  }

  const loadDoc = async () => {
    if (doc || !summary.document_id) return;
    setDocLoading(true);
    try {
      const d = await fomcAPI.getDocument(summary.document_id);
      setDoc(d);
    } finally {
      setDocLoading(false);
    }
  };

  return (
    <div>
      {/* 概括正文 */}
      <MarkdownRenderer content={summary.summary_content || ''} variant={themeMode} />

      {/* AI 加工链溯源 */}
      <Paragraph type="secondary" style={{ fontSize: 12, marginTop: 8, marginBottom: 4 }}>
        <RobotOutlined /> AI chain: template <code>{summary.template_id}</code>
        {' · '}model <code>{summary.model}</code>
        {summary.generated_at ? ` · ${new Date(summary.generated_at).toLocaleString()}` : ''}
        {summary.source_url && (
          <>
            {' · '}
            <a href={summary.source_url} target="_blank" rel="noreferrer">
              <LinkOutlined /> original
            </a>
          </>
        )}
      </Paragraph>

      {/* 原文对照（展开懒加载） */}
      <Collapse
        ghost
        size="small"
        items={[{
          key: 'original',
          label: <Text type="secondary" style={{ fontSize: 12 }}>
            Show original text {doc?.word_count ? `(${doc.word_count} words)` : ''}
          </Text>,
          children: (
            <Spin spinning={docLoading}>
              <Paragraph
                style={{
                  color: colors.textSecondary,
                  fontSize: 12,
                  maxHeight: 300,
                  overflow: 'auto',
                  whiteSpace: 'pre-wrap',
                  background: colors.neutralTagBg,
                  padding: 12,
                  borderRadius: 6,
                }}
              >
                {doc?.raw_content ?? 'Loading…'}
              </Paragraph>
            </Spin>
          ),
        }]}
        onChange={loadDoc}
      />
    </div>
  );
};

export default CentralBankDashboardPage;

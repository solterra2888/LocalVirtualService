import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
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
  Select,
  Table,
  Modal,
} from 'antd';
import {
  BankOutlined,
  LinkOutlined,
  RiseOutlined,
  FallOutlined,
  MinusOutlined,
  RobotOutlined,
  FileTextOutlined,
  QuestionCircleOutlined,
} from '@ant-design/icons';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip as RTooltip,
  ResponsiveContainer, Legend,
} from 'recharts';
import {
  fomcAPI, DashboardData, DecisionCard, SummaryInfo, UpdateItem,
  ProbabilitiesPayload, NextMeetingProb, RecentMeeting, Dissenter,
} from '../services/fomc';
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
    stripActive: 'rgba(22, 119, 255, 0.12)',
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
    stripActive: 'rgba(22, 119, 255, 0.22)',
  },
} as const;

type ThemeColors = (typeof THEME_COLORS)[keyof typeof THEME_COLORS];

const fmtBp = (bp?: number | null) =>
  bp === null || bp === undefined ? '—' : `${(bp / 100).toFixed(2)}%`;

const fmtPct = (v?: number | null) =>
  v === null || v === undefined ? '—' : `${Math.round(v * 100)}%`;

const fmtDate = (iso?: string | null, withYear = false) => {
  if (!iso) return '—';
  const d = new Date(`${iso.slice(0, 10)}T00:00:00`);
  return d.toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', ...(withYear ? { year: 'numeric' } : {}),
  });
};

const isoDate = (s?: string | null) => (s || '').slice(0, 10);

const daysFromToday = (iso?: string | null) => {
  const day = isoDate(iso);
  if (!day) return null;
  const a = Date.parse(`${day}T00:00:00Z`);
  const now = new Date();
  const b = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  return Math.round((a - b) / 86400000);
};

const daysLabel = (n: number | null) => {
  if (n === null) return '';
  if (n === 0) return 'today';
  if (n > 0) return `in ${n}d`;
  return `${-n}d ago`;
};

const dirLabel: Record<string, string> = { hike: 'Hike', cut: 'Cut', hold: 'Hold' };

const PricedInHelp: React.FC<{ color: string }> = ({ color }) => (
  <Tooltip
    placement="bottomLeft"
    overlayInnerStyle={{ maxWidth: 460, padding: 12, fontSize: 12, lineHeight: 1.55 }}
    title={
      <div>
        <div style={{ fontWeight: 600, marginBottom: 6 }}>怎么用</div>
        <div>
          这不是「美联储会做什么」的预测，而是期货已经按什么结果在交易。Hold / Hike / Cut 是 ±25bp 三种情景，加起来约 100%。哪边高，哪边就是当前定价；高概率那边若落空，意外更大。
        </div>
        <div style={{ marginTop: 6 }}>
          卡片跟着时间轴走。后面某次会的 Hike% 是「那次会上再加一次」的增量概率，不是「到那天为止总共加了几次」。
        </div>
        <div style={{ fontWeight: 600, margin: '10px 0 6px' }}>计算方法（zq_v1）</div>
        <div style={{ fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace', fontSize: 11 }}>
          implied = 100 − P<br />
          hold = base<br />
          cut = (base·d_pre + (base − 0.25)·d_post) / N<br />
          hike = (base·d_pre + (base + 0.25)·d_post) / N<br />
          p = 把 implied 插值在三种月均情景之间
        </div>
        <div style={{ marginTop: 6 }}>
          第一次会议 base = EFFR；之后用上一次会的会后隐含利率 post_rate 递推，避免把 9 月已定价的加息再算进 10 月。
        </div>
        <div style={{ fontWeight: 600, margin: '10px 0 6px' }}>数据源（可点开）</div>
        <div>
          期货价：CME{' '}
          <a href="https://www.cmegroup.com/markets/interest-rates/stirs/30-day-federal-fund.html" target="_blank" rel="noreferrer" style={{ color: 'inherit', textDecoration: 'underline' }}>
            30-Day Fed Funds (ZQ)
          </a>
          ，日收盘来自 Yahoo（如{' '}
          <a href="https://finance.yahoo.com/quote/ZQU26.CBT/" target="_blank" rel="noreferrer" style={{ color: 'inherit', textDecoration: 'underline' }}>
            ZQU26.CBT
          </a>
          ）。
          <br />
          基准：{' '}
          <a href="https://www.newyorkfed.org/markets/reference-rates/effr" target="_blank" rel="noreferrer" style={{ color: 'inherit', textDecoration: 'underline' }}>
            纽约联储 EFFR
          </a>
          {' / '}
          <a href="https://fred.stlouisfed.org/series/EFFR" target="_blank" rel="noreferrer" style={{ color: 'inherit', textDecoration: 'underline' }}>
            FRED EFFR
          </a>
          ，不是目标区间中值。
          <br />
          会议日历：{' '}
          <a href="https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm" target="_blank" rel="noreferrer" style={{ color: 'inherit', textDecoration: 'underline' }}>
            fomccalendars.htm
          </a>
          。对照（非数据源）：{' '}
          <a href="https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html" target="_blank" rel="noreferrer" style={{ color: 'inherit', textDecoration: 'underline' }}>
            CME FedWatch
          </a>
          。
        </div>
      </div>
    }
  >
    <QuestionCircleOutlined style={{ color, cursor: 'help', fontSize: 13 }} />
  </Tooltip>
);

const splitMarkdownSections = (md: string): { title: string; body: string }[] => {
  if (!md.trim()) return [];
  const chunks = md.split(/^##\s+/m).map((s) => s.trim()).filter(Boolean);
  return chunks.map((chunk) => {
    const nl = chunk.indexOf('\n');
    if (nl < 0) return { title: chunk, body: '' };
    return { title: chunk.slice(0, nl).trim(), body: chunk.slice(nl + 1).trim() };
  });
};

const isStanceRetread = (title: string) =>
  /决议|投票与分歧|^vote\b|^decision\b/i.test(title);

const sectionRank = (title: string) => {
  if (/措辞|wording/i.test(title)) return 0;
  if (/前瞻|forward guidance/i.test(title)) return 1;
  if (/经济|econom/i.test(title)) return 2;
  return 3;
};

const leadingOutcome = (
  p?: { p_cut?: number | null; p_hold?: number | null; p_hike?: number | null } | null,
): { dir: 'cut' | 'hold' | 'hike'; p: number } | null => {
  if (!p) return null;
  const rows: { dir: 'cut' | 'hold' | 'hike'; p: number }[] = [
    { dir: 'hike', p: p.p_hike ?? NaN },
    { dir: 'hold', p: p.p_hold ?? NaN },
    { dir: 'cut', p: p.p_cut ?? NaN },
  ];
  const finite = rows.filter((r) => Number.isFinite(r.p));
  if (!finite.length) return null;
  finite.sort((a, b) => b.p - a.p);
  return finite[0];
};

const decisionFromRecent = (
  meetings: RecentMeeting[],
  date: string,
): DecisionCard | null => {
  const target = isoDate(date);
  const sorted = [...meetings].sort((a, b) =>
    isoDate(a.meeting_date).localeCompare(isoDate(b.meeting_date)),
  );
  const i = sorted.findIndex((m) => isoDate(m.meeting_date) === target);
  if (i < 0) return null;
  const m = sorted[i];
  const prev = i > 0 ? sorted[i - 1] : undefined;
  const high = m.rate_high_bp;
  const low = m.rate_low_bp ?? (high != null ? high - 25 : null);
  return {
    meeting_date: target,
    rate_low_bp: low,
    rate_high_bp: high,
    decision_direction: (m.decision_direction as DecisionCard['decision_direction']) ?? null,
    change_bp: m.change_bp,
    vote_for: m.vote_for,
    vote_against: m.vote_against,
    dissenters: m.dissenters || [],
    source_url: m.source_url,
    previous: prev && prev.rate_high_bp != null ? {
      meeting_date: isoDate(prev.meeting_date),
      rate_high_bp: prev.rate_high_bp,
      vote_for: prev.vote_for,
      vote_against: prev.vote_against,
      decision_direction: prev.decision_direction,
    } : null,
  };
};

const summaryMatches = (s: SummaryInfo | null | undefined, date: string) =>
  Boolean(s && isoDate(s.meeting_date) === isoDate(date));

const defaultSummaryTab = (stmt?: SummaryInfo | null, mins?: SummaryInfo | null): 'statement' | 'minutes' => {
  if (!mins && stmt) return 'statement';
  if (!stmt && mins) return 'minutes';
  if (!stmt && !mins) return 'statement';
  const sm = stmt?.meeting_date || '';
  const mm = mins?.meeting_date || '';
  if (mm > sm) return 'minutes';
  if (mm < sm) return 'statement';
  return 'minutes';
};

const CentralBankDashboardPage: React.FC = () => {
  const { isAuthenticated } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const colors = THEME_COLORS[themeMode];

  const [loading, setLoading] = useState(false);
  const [latest, setLatest] = useState<DashboardData | null>(null);
  const [slice, setSlice] = useState<{
    decision: DecisionCard | null;
    statement_summary: SummaryInfo | null;
    minutes_summary: SummaryInfo | null;
  } | null>(null);
  const [selectedMeeting, setSelectedMeeting] = useState<string | undefined>(undefined);
  const [sliceLoading, setSliceLoading] = useState(false);

  const [monetaryOnly, setMonetaryOnly] = useState(true);
  const [updates, setUpdates] = useState<UpdateItem[]>([]);
  const [updatesTotal, setUpdatesTotal] = useState(0);
  const [updatesPage, setUpdatesPage] = useState(1);
  const [updatesLoading, setUpdatesLoading] = useState(false);

  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pollCount = useRef(0);

  const fetchLatest = useCallback(async () => {
    setLoading(true);
    try {
      const d = await fomcAPI.getDashboard();
      setLatest(d);
      setSelectedMeeting(
        d.probabilities?.next_meetings?.[0]?.meeting_date
        || d.decision?.meeting_date
        || undefined,
      );
      setSlice(null);
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
      const r = await fomcAPI.getUpdates({ page, page_size: 10, monetary_only: monetary });
      setUpdates(r.items);
      setUpdatesTotal(r.total);
    } catch (error) {
      console.error('Failed to load updates:', error);
    } finally {
      setUpdatesLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!isAuthenticated) return;
    fetchLatest();
    fetchUpdates(1, true);
  }, [isAuthenticated, fetchLatest, fetchUpdates]);

  const selectedIso = isoDate(selectedMeeting);
  const pastHit = (latest?.recent_meetings || []).find(
    (m) => isoDate(m.meeting_date) === selectedIso,
  );
  const sliceDecisionMatches = isoDate(slice?.decision?.meeting_date) === selectedIso
    && Boolean(slice?.decision);
  const displayDecision = pastHit
    ? (sliceDecisionMatches ? slice!.decision : decisionFromRecent(latest!.recent_meetings, selectedIso))
    : (latest?.decision ?? null);
  const displayStmt = pastHit
    ? (summaryMatches(slice?.statement_summary, selectedIso)
      ? slice!.statement_summary
      : (summaryMatches(latest?.statement_summary, selectedIso) ? latest!.statement_summary : null))
    : (slice?.statement_summary ?? latest?.statement_summary ?? null);
  const displayMins = pastHit
    ? (summaryMatches(slice?.minutes_summary, selectedIso)
      ? slice!.minutes_summary
      : (summaryMatches(latest?.minutes_summary, selectedIso) ? latest!.minutes_summary : null))
    : (slice?.minutes_summary ?? latest?.minutes_summary ?? null);

  useEffect(() => {
    const anyGenerating =
      displayStmt?.status === 'generating' || displayMins?.status === 'generating';
    if (!anyGenerating) {
      pollCount.current = 0;
      return;
    }
    if (pollCount.current > 60) return;
    pollRef.current = setTimeout(() => {
      pollCount.current += 1;
      fomcAPI.getDashboard(selectedMeeting).then((d) => {
        if (!selectedMeeting || selectedMeeting === d.decision?.meeting_date) {
          setLatest((prev) => prev ? { ...prev, ...d, recent_meetings: prev.recent_meetings, probabilities: prev.probabilities } : d);
        }
        setSlice({
          decision: d.decision,
          statement_summary: d.statement_summary,
          minutes_summary: d.minutes_summary,
        });
      }).catch(() => {});
    }, 5000);
    return () => {
      if (pollRef.current) clearTimeout(pollRef.current);
    };
  }, [displayStmt?.status, displayMins?.status, selectedMeeting]);

  const pastDates = useMemo(
    () => new Set((latest?.recent_meetings || []).map((m) => isoDate(m.meeting_date))),
    [latest],
  );

  const onSelectMeeting = async (date: string) => {
    const picked = isoDate(date);
    setSelectedMeeting(picked);
    if (!pastDates.has(picked)) {
      setSlice(null);
      return;
    }
    if (picked === isoDate(latest?.decision?.meeting_date)) {
      setSlice(null);
      return;
    }
    setSliceLoading(true);
    try {
      const d = await fomcAPI.getDashboard(picked);
      const matches = isoDate(d.decision?.meeting_date) === picked
        || isoDate(d.statement_summary?.meeting_date) === picked
        || isoDate(d.minutes_summary?.meeting_date) === picked;
      if (matches) {
        setSlice({
          decision: d.decision,
          statement_summary: d.statement_summary,
          minutes_summary: d.minutes_summary,
        });
      } else {
        setSlice(null);
      }
    } catch (error) {
      message.error('Failed to load that meeting');
      setSlice(null);
    } finally {
      setSliceLoading(false);
    }
  };

  return (
    <div style={{ padding: '24px', background: colors.pageBg, minHeight: '100vh' }}>
      <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center', gap: 12 }}>
        <Space>
          <BankOutlined style={{ fontSize: 22, color: colors.text }} />
          <Title level={4} style={{ color: colors.text, margin: 0 }}>
            Fed
          </Title>
        </Space>
      </div>

      {loading && !latest ? (
        <Skeleton active paragraph={{ rows: 8 }} />
      ) : !latest ? (
        <Empty description="No Fed data yet — the scheduler will poll the Fed RSS feed" />
      ) : (
        <FedDesk
          colors={colors}
          latest={latest}
          decision={displayDecision}
          statement={displayStmt}
          minutes={displayMins}
          selectedMeeting={selectedMeeting}
          onSelectMeeting={onSelectMeeting}
          sliceLoading={sliceLoading}
          monetaryOnly={monetaryOnly}
          onMonetaryOnly={(v) => {
            setMonetaryOnly(v);
            setUpdatesPage(1);
            fetchUpdates(1, v);
          }}
          updates={updates}
          updatesTotal={updatesTotal}
          updatesPage={updatesPage}
          updatesLoading={updatesLoading}
          onUpdatesPage={(p) => {
            setUpdatesPage(p);
            fetchUpdates(p, monetaryOnly);
          }}
        />
      )}
    </div>
  );
};

const FedDesk: React.FC<{
  colors: ThemeColors;
  latest: DashboardData;
  decision: DecisionCard | null;
  statement?: SummaryInfo | null;
  minutes?: SummaryInfo | null;
  selectedMeeting?: string;
  onSelectMeeting: (date: string) => void;
  sliceLoading: boolean;
  monetaryOnly: boolean;
  onMonetaryOnly: (v: boolean) => void;
  updates: UpdateItem[];
  updatesTotal: number;
  updatesPage: number;
  updatesLoading: boolean;
  onUpdatesPage: (p: number) => void;
}> = ({
  colors, latest, decision, statement, minutes, selectedMeeting, onSelectMeeting,
  sliceLoading, monetaryOnly, onMonetaryOnly, updates, updatesTotal, updatesPage,
  updatesLoading, onUpdatesPage,
}) => (
    <Spin spinning={sliceLoading}>
      <Grid2 colors={colors} decision={decision} probabilities={latest.probabilities} selectedMeeting={selectedMeeting} />

      <MeetingStrip
        colors={colors}
        recent={latest.recent_meetings || []}
        upcoming={latest.probabilities?.next_meetings || []}
        selected={selectedMeeting}
        onSelect={onSelectMeeting}
      />

      <Card
        size="small"
        style={{ marginBottom: 16, background: colors.cardBg, borderColor: colors.cardBorder }}
        title={
          <Space>
            <RobotOutlined style={{ color: colors.text }} />
            <Text strong style={{ color: colors.text }}>What changed in the text</Text>
          </Space>
        }
      >
        <GuidanceTabs
          colors={colors}
          statement={statement}
          minutes={minutes}
          stmtNewerThanMins={Boolean(
            statement?.meeting_date && minutes?.meeting_date
            && statement.meeting_date > minutes.meeting_date,
          )}
        />
      </Card>

      <div style={{ marginBottom: 16 }}>
        <ProbabilitiesDetails colors={colors} probabilities={latest.probabilities} selectedMeeting={selectedMeeting} />
      </div>

      <Card
          size="small"
          style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
          title={
            <Space>
              <FileTextOutlined />
              <Text strong style={{ color: colors.text }}>Fed updates</Text>
            </Space>
          }
          extra={
            <Space>
              <Text type="secondary" style={{ fontSize: 12 }}>Monetary Policy</Text>
              <Switch size="small" checked={monetaryOnly} onChange={onMonetaryOnly} />
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
                  style={{ padding: '6px 0' }}
                  actions={[
                    <Button key="open" type="link" size="small" icon={<LinkOutlined />} href={item.link} target="_blank">
                      Fed
                    </Button>,
                  ]}
                >
                  <List.Item.Meta
                    title={<Text style={{ color: colors.text, fontSize: 13 }}>{item.title}</Text>}
                    description={
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        {item.pub_date ? fmtDate(item.pub_date.slice(0, 10), true) : ''}
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
                onChange={onUpdatesPage}
              />
            </div>
          </Spin>
        </Card>
    </Spin>
);

const Grid2: React.FC<{
  colors: ThemeColors;
  decision: DecisionCard | null;
  probabilities: ProbabilitiesPayload | null;
  selectedMeeting?: string;
}> = ({ colors, decision, probabilities, selectedMeeting }) => {
  const dirIcon = (dir?: string | null) =>
    dir === 'hike' ? <RiseOutlined style={{ color: colors.hawkColor }} />
    : dir === 'cut' ? <FallOutlined style={{ color: colors.doveColor }} />
    : <MinusOutlined style={{ color: colors.neutralTagColor }} />;

  const next = probabilities?.next_meetings?.[0];
  const upcomingHit = probabilities?.next_meetings?.find(
    (m) => isoDate(m.meeting_date) === isoDate(selectedMeeting),
  );
  const hist = probabilities?.expectations_vs_actual?.find(
    (e) => isoDate(e.meeting_date) === isoDate(selectedMeeting),
  );
  const isHistorical = Boolean(
    selectedMeeting
    && !upcomingHit
    && (daysFromToday(selectedMeeting) ?? 1) < 0,
  );
  const liveFocus = upcomingHit || (!isHistorical ? next : undefined);
  const l = liveFocus?.latest;
  const hp = hist?.implied_p;
  const delta = delta7d(liveFocus);

  const prevVote = decision?.previous;
  const prevVoteText = prevVote && prevVote.vote_for != null
    ? `was ${prevVote.vote_for}–${prevVote.vote_against ?? 0} in ${fmtDate(prevVote.meeting_date)}`
    : prevVote ? `prev ${fmtDate(prevVote.meeting_date)} ${fmtBp(prevVote.rate_high_bp)}` : null;

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: 16, marginBottom: 16 }}>
      <Card
        size="small"
        style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
        title={<Text strong style={{ color: colors.text }}>Stance</Text>}
          extra={
            <Space size={8}>
              {decision?.meeting_date && (
                <Tag style={{ background: colors.neutralTagBg, color: colors.neutralTagColor, border: 'none' }}>
                  {fmtDate(decision.meeting_date, true)}
                </Tag>
              )}
              {decision?.source_url ? (
                <Button type="link" size="small" icon={<LinkOutlined />} href={decision.source_url} target="_blank">
                  Statement
                </Button>
              ) : null}
            </Space>
          }
      >
        {decision ? (
          <div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
              <div>
                <Text type="secondary" style={{ fontSize: 12 }}>Fed Funds Target Range</Text>
                <div style={{ fontSize: 26, fontWeight: 700, color: colors.text, lineHeight: 1.2 }}>
                  {fmtBp(decision.rate_low_bp)} – {fmtBp(decision.rate_high_bp)}
                </div>
                <Space size={4} style={{ marginTop: 4 }}>
                  {dirIcon(decision.decision_direction)}
                  <Text style={{ color: colors.textSecondary, fontSize: 13 }}>
                    {dirLabel[decision.decision_direction ?? ''] ?? ''}
                    {decision.change_bp ? ` ${decision.change_bp > 0 ? '+' : ''}${decision.change_bp}bp` : ' 0bp'}
                  </Text>
                </Space>
              </div>
              <div>
                <Text type="secondary" style={{ fontSize: 12 }}>Vote</Text>
                <div style={{ fontSize: 26, fontWeight: 700, color: colors.text, lineHeight: 1.2 }}>
                  {decision.vote_for ?? '–'} – {decision.vote_against ?? '–'}
                </div>
                {prevVoteText && (
                  <Text type="secondary" style={{ fontSize: 12 }}>{prevVoteText}</Text>
                )}
              </div>
            </div>
            <div style={{ marginTop: 12 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>Dissents ({decision.dissenters.length})</Text>
              <div style={{ marginTop: 6, display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                {decision.dissenters.length === 0 ? (
                  <Text type="secondary" style={{ fontSize: 13 }}>None — unanimous</Text>
                ) : decision.dissenters.map((x) => (
                  <DissenterTag key={x.name} d={x} colors={colors} />
                ))}
              </div>
            </div>
          </div>
        ) : (
          <Empty description="No decision for this date" />
        )}
      </Card>

      <Card
        size="small"
        style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
        title={
          <Space size={6}>
            <Text strong style={{ color: colors.text }}>Priced in</Text>
            <PricedInHelp color={colors.textSecondary} />
          </Space>
        }
        extra={
          isHistorical && hist ? (
            <Tag style={{ background: colors.neutralTagBg, color: colors.neutralTagColor, border: 'none' }}>
              pre-meeting{hist.asof_date ? ` · as of ${fmtDate(hist.asof_date)}` : ''}
            </Tag>
          ) : liveFocus ? (
            <Tag style={{ background: colors.neutralTagBg, color: colors.neutralTagColor, border: 'none' }}>
              {fmtDate(liveFocus.meeting_date)}{liveFocus.is_sep ? ' SEP' : ''}
            </Tag>
          ) : null
        }
      >
        {isHistorical ? (
          hp ? (
            <>
              <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap' }}>
                <div>
                  <div style={{ fontSize: 26, fontWeight: 700, color: colors.text }}>{fmtPct(hp.hold)}</div>
                  <Text type="secondary" style={{ fontSize: 12 }}>Hold</Text>
                </div>
                <div>
                  <div style={{ fontSize: 26, fontWeight: 700, color: colors.hawkColor }}>{fmtPct(hp.hike)}</div>
                  <Text type="secondary" style={{ fontSize: 12 }}>Hike</Text>
                </div>
                <div>
                  <div style={{ fontSize: 26, fontWeight: 700, color: colors.doveColor }}>{fmtPct(hp.cut)}</div>
                  <Text type="secondary" style={{ fontSize: 12 }}>Cut</Text>
                </div>
              </div>
              <Tooltip title={`Cut ${fmtPct(hp.cut)} · Hold ${fmtPct(hp.hold)} · Hike ${fmtPct(hp.hike)}`}>
                <div style={{ display: 'flex', height: 10, borderRadius: 5, overflow: 'hidden', background: colors.neutralTagBg, marginTop: 12 }}>
                  <div style={{ width: `${(hp.cut ?? 0) * 100}%`, background: colors.doveColor }} />
                  <div style={{ width: `${(hp.hold ?? 0) * 100}%`, background: colors.neutralTagColor }} />
                  <div style={{ width: `${(hp.hike ?? 0) * 100}%`, background: colors.hawkColor }} />
                </div>
              </Tooltip>
              {hist?.method === 'zq_front_v1' && (
                <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 8 }}>
                  front-month approx.
                </Text>
              )}
            </>
          ) : (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description="No pre-meeting probability snapshot for this date"
            />
          )
        ) : l ? (
          <>
            <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap' }}>
              <div>
                <div style={{ fontSize: 26, fontWeight: 700, color: colors.text }}>{fmtPct(l.p_hold)}</div>
                <Text type="secondary" style={{ fontSize: 12 }}>Hold</Text>
              </div>
              <div>
                <div style={{ fontSize: 26, fontWeight: 700, color: colors.hawkColor }}>{fmtPct(l.p_hike)}</div>
                <Text type="secondary" style={{ fontSize: 12 }}>Hike</Text>
              </div>
              <div>
                <div style={{ fontSize: 26, fontWeight: 700, color: colors.doveColor }}>{fmtPct(l.p_cut)}</div>
                <Text type="secondary" style={{ fontSize: 12 }}>Cut</Text>
              </div>
            </div>
            <Tooltip title={`Cut ${fmtPct(l.p_cut)} · Hold ${fmtPct(l.p_hold)} · Hike ${fmtPct(l.p_hike)}`}>
              <div style={{ display: 'flex', height: 10, borderRadius: 5, overflow: 'hidden', background: colors.neutralTagBg, marginTop: 12 }}>
                <div style={{ width: `${(l.p_cut ?? 0) * 100}%`, background: colors.doveColor }} />
                <div style={{ width: `${(l.p_hold ?? 0) * 100}%`, background: colors.neutralTagColor }} />
                <div style={{ width: `${(l.p_hike ?? 0) * 100}%`, background: colors.hawkColor }} />
              </div>
            </Tooltip>
            {delta && (
              <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 8 }}>
                7d {dirLabel[delta.dir]} {delta.pp >= 0 ? '+' : ''}{delta.pp.toFixed(0)}pp
              </Text>
            )}
            {(l.futures_contract || l.effr != null) && (
              <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 8 }}>
                {l.futures_contract ? (
                  <a
                    href={`https://finance.yahoo.com/quote/${encodeURIComponent(l.futures_contract)}/`}
                    target="_blank"
                    rel="noreferrer"
                  >
                    {l.futures_contract}
                  </a>
                ) : null}
                {l.futures_price != null ? ` · P ${l.futures_price}` : ''}
                {l.effr != null ? (
                  <>
                    {' · '}
                    <a
                      href="https://fred.stlouisfed.org/series/EFFR"
                      target="_blank"
                      rel="noreferrer"
                    >
                      EFFR {l.effr}
                    </a>
                  </>
                ) : null}
                {l.asof_date ? ` · as of ${fmtDate(l.asof_date)}` : ''}
              </Text>
            )}
          </>
        ) : (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No snapshot yet" />
        )}
      </Card>
    </div>
  );
};

const DissenterTag: React.FC<{ d: Dissenter; colors: ThemeColors }> = ({ d, colors }) => (
  <Tooltip title={d.direction === 'hawkish' ? 'Preferred to raise rates' : d.direction === 'dovish' ? 'Preferred to lower rates' : 'Direction unclear'}>
    <Tag
      style={{
        background: d.direction === 'hawkish' ? colors.hawkBg : d.direction === 'dovish' ? colors.doveBg : colors.neutralTagBg,
        color: d.direction === 'hawkish' ? colors.hawkColor : d.direction === 'dovish' ? colors.doveColor : colors.neutralTagColor,
        border: 'none',
      }}
    >
      {d.name} {d.direction === 'hawkish' ? 'hawk' : d.direction === 'dovish' ? 'dove' : ''}
    </Tag>
  </Tooltip>
);

const MeetingStrip: React.FC<{
  colors: ThemeColors;
  recent: RecentMeeting[];
  upcoming: NextMeetingProb[];
  selected?: string;
  onSelect: (date: string) => void;
}> = ({ colors, recent, upcoming, selected, onSelect }) => {
  const past = [...recent].sort((a, b) => a.meeting_date.localeCompare(b.meeting_date));
  const future = upcoming.filter((m) => !past.some((p) => p.meeting_date === m.meeting_date));
  return (
    <Card
      size="small"
      style={{ marginBottom: 16, background: colors.cardBg, borderColor: colors.cardBorder }}
      title={<Text strong style={{ color: colors.text }}>FOMC cycle</Text>}
    >
      <div style={{ display: 'flex', gap: 8, overflowX: 'auto', paddingBottom: 4 }}>
        {past.map((m) => {
          const active = isoDate(selected) === isoDate(m.meeting_date);
          return (
            <button
              key={m.meeting_date}
              type="button"
              onClick={() => onSelect(m.meeting_date)}
              style={{
                minWidth: 88, textAlign: 'left', cursor: 'pointer',
                border: `1px solid ${active ? colors.doveColor : colors.cardBorder}`,
                background: active ? colors.stripActive : 'transparent',
                borderRadius: 8, padding: '8px 10px', color: colors.text,
              }}
            >
              <div style={{ fontWeight: 600, fontSize: 13 }}>{fmtDate(m.meeting_date)}</div>
              <div style={{ fontSize: 12, color: colors.textSecondary }}>
                {dirLabel[m.decision_direction || ''] || '—'}
              </div>
            </button>
          );
        })}
        {future.map((m) => {
          const active = isoDate(selected) === isoDate(m.meeting_date);
          const lead = leadingOutcome(m.latest);
          const leadColor = lead?.dir === 'hike' ? colors.hawkColor
            : lead?.dir === 'cut' ? colors.doveColor
            : colors.textSecondary;
          return (
            <button
              key={m.meeting_date}
              type="button"
              onClick={() => onSelect(m.meeting_date)}
              style={{
                minWidth: 88, textAlign: 'left', cursor: 'pointer',
                border: `1px dashed ${active ? colors.doveColor : colors.cardBorder}`,
                background: active ? colors.stripActive : 'transparent',
                borderRadius: 8, padding: '8px 10px', color: colors.text,
              }}
            >
              <div style={{ fontWeight: 600, fontSize: 13 }}>
                {fmtDate(m.meeting_date)}{m.is_sep ? ' SEP' : ''}
              </div>
              <div style={{ fontSize: 12, color: leadColor }}>
                {lead ? `${dirLabel[lead.dir]} ${fmtPct(lead.p)}` : '—'}
              </div>
              <div style={{ fontSize: 11, color: colors.textSecondary }}>{daysLabel(m.days_away)}</div>
            </button>
          );
        })}
      </div>
    </Card>
  );
};

const GuidanceTabs: React.FC<{
  colors: ThemeColors;
  statement?: SummaryInfo | null;
  minutes?: SummaryInfo | null;
  stmtNewerThanMins: boolean;
}> = ({ colors, statement, minutes, stmtNewerThanMins }) => {
  const defaultKey = defaultSummaryTab(statement, minutes);
  const [active, setActive] = useState(defaultKey);
  useEffect(() => { setActive(defaultKey); }, [defaultKey, statement?.document_id, minutes?.document_id]);

  const label = (kind: 'statement' | 'minutes', s?: SummaryInfo | null) => {
    const name = kind === 'statement' ? 'Statement' : 'Minutes';
    if (!s?.meeting_date) return name;
    const stale = kind === 'minutes' && stmtNewerThanMins;
    return `${name} · ${fmtDate(s.meeting_date)}${stale ? ' (prior)' : ''}`;
  };

  return (
    <Tabs
      activeKey={active}
      onChange={(k) => setActive(k as 'statement' | 'minutes')}
      items={[
        { key: 'statement', label: label('statement', statement), children: <SummarySection summary={statement} colors={colors} /> },
        { key: 'minutes', label: label('minutes', minutes), children: <SummarySection summary={minutes} colors={colors} /> },
      ]}
    />
  );
};

const SummarySection: React.FC<{
  summary?: SummaryInfo | null;
  colors: ThemeColors;
}> = ({ summary, colors }) => {
  const { themeMode } = useDashboardSettings();
  const [doc, setDoc] = useState<{ raw_content?: string | null; word_count?: number | null } | null>(null);
  const [docLoading, setDocLoading] = useState(false);
  const [originalOpen, setOriginalOpen] = useState(false);

  useEffect(() => { setDoc(null); setOriginalOpen(false); }, [summary?.document_id]);

  if (!summary) {
    return <Empty description="No document yet — will appear after the Fed publishes it" />;
  }
  if (summary.status === 'generating') {
    return <div style={{ padding: '24px 0', textAlign: 'center' }}><Spin tip="AI summary generating…" /></div>;
  }
  if (summary.status === 'failed') {
    return <Empty description="AI summary failed — will retry on next poll" />;
  }

  const visible = splitMarkdownSections(summary.summary_content || '')
    .filter((s) => !isStanceRetread(s.title))
    .sort((a, b) => sectionRank(a.title) - sectionRank(b.title));

  const openOriginal = async () => {
    setOriginalOpen(true);
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
      {visible.map((s, i) => (
        <div
          key={s.title}
          style={{
            marginBottom: 16,
            paddingBottom: 12,
            borderBottom: i < visible.length - 1 ? `1px solid ${colors.cardBorder}` : 'none',
          }}
        >
          <Text strong style={{ color: colors.text, fontSize: 13 }}>{s.title}</Text>
          <div style={{ marginTop: 4 }}>
            <MarkdownRenderer content={s.body} variant={themeMode} />
          </div>
        </div>
      ))}

      <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 12 }}>
        <Button type="link" size="small" icon={<FileTextOutlined />} onClick={openOriginal} style={{ padding: 0 }}>
          Read original
        </Button>
        {summary.source_url && (
          <Button type="link" size="small" icon={<LinkOutlined />} href={summary.source_url} target="_blank" style={{ padding: 0 }}>
            Fed.gov
          </Button>
        )}
        <Text type="secondary" style={{ fontSize: 12 }}>
          {summary.model ? <code>{summary.model}</code> : null}
          {summary.generated_at ? ` · ${new Date(summary.generated_at).toLocaleString()}` : ''}
        </Text>
      </div>

      <Modal
        title={summary.title || 'Original text'}
        open={originalOpen}
        onCancel={() => setOriginalOpen(false)}
        footer={null}
        width={720}
      >
        <Spin spinning={docLoading}>
          <Paragraph
            style={{
              color: colors.textSecondary, fontSize: 13, maxHeight: 480, overflow: 'auto',
              whiteSpace: 'pre-wrap', marginBottom: 0,
            }}
          >
            {doc?.raw_content ?? (docLoading ? 'Loading…' : 'No original text')}
          </Paragraph>
        </Spin>
      </Modal>
    </div>
  );
};

function delta7d(m?: NextMeetingProb): { dir: 'cut' | 'hold' | 'hike'; pp: number } | null {
  const lead = leadingOutcome(m?.latest);
  if (!lead || !m?.trend || m.trend.length < 2) return null;
  const end = m.trend[m.trend.length - 1]?.date;
  if (!end) return null;
  const target = Date.parse(`${end}T00:00:00Z`) - 7 * 86400000;
  let best = m.trend[0];
  for (const p of m.trend) {
    const t = Date.parse(`${p.date}T00:00:00Z`);
    if (t <= target) best = p;
  }
  const then = lead.dir === 'hike' ? best.p_hike : lead.dir === 'cut' ? best.p_cut : best.p_hold;
  if (then === null || then === undefined) return null;
  return { dir: lead.dir, pp: (lead.p - then) * 100 };
}

function impliedPathLabel(p: ProbabilitiesPayload | null): string {
  if (!p?.next_meetings?.length) return '—';
  const bits = p.next_meetings
    .map((m) => m.latest?.implied_rate)
    .filter((v): v is number => v !== null && v !== undefined)
    .map((v) => v.toFixed(2));
  return bits.length ? bits.join(' → ') : '—';
}

const ProbabilitiesDetails: React.FC<{
  colors: ThemeColors;
  probabilities: ProbabilitiesPayload | null;
  selectedMeeting?: string;
}> = ({ colors, probabilities, selectedMeeting }) => {
  const { themeMode } = useDashboardSettings();
  const meetings = probabilities?.next_meetings ?? [];
  const [sel, setSel] = useState<string | undefined>(undefined);
  const [trend, setTrend] = useState<{
    meeting_date: string;
    series: { date: string; p_cut: number | null; p_hold: number | null; p_hike: number | null }[];
  } | null>(null);
  const [trendLoading, setTrendLoading] = useState(false);

  const selected = sel
    ?? (meetings.some((m) => m.meeting_date === selectedMeeting) ? selectedMeeting : undefined)
    ?? meetings[0]?.meeting_date;

  useEffect(() => {
    if (!selected) return;
    const inline = meetings.find((m) => m.meeting_date === selected);
    if (inline) {
      setTrend({ meeting_date: selected, series: inline.trend });
      if (inline.trend.length >= 2) return;
    }
    setTrendLoading(true);
    fomcAPI.getProbabilityTrend(selected, 90)
      .then((r) => setTrend({ meeting_date: r.meeting_date, series: r.series }))
      .catch(() => {})
      .finally(() => setTrendLoading(false));
  }, [selected]); // eslint-disable-line react-hooks/exhaustive-deps

  const gridColor = themeMode === 'dark' ? 'rgba(148,163,184,0.15)' : 'rgba(0,0,0,0.06)';
  const axisColor = themeMode === 'dark' ? '#94a3b8' : 'rgba(0,0,0,0.45)';
  const tooltipBg = themeMode === 'dark' ? '#1a2234' : '#fff';

  if (!probabilities) return null;

  return (
    <Card
      size="small"
      style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
      title={<Text strong style={{ color: colors.text }}>Probability trend</Text>}
      extra={
        meetings.length > 0 ? (
          <Select
            size="small"
            value={selected}
            style={{ minWidth: 160 }}
            onChange={setSel}
            options={meetings.map((m) => ({
              value: m.meeting_date,
              label: `${fmtDate(m.meeting_date)}${m.is_sep ? ' SEP' : ''}`,
            }))}
          />
        ) : null
      }
    >
      <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
        Implied average funds rate: {impliedPathLabel(probabilities)}
      </Text>
      <Spin spinning={trendLoading}>
        {trend && trend.series.length >= 2 ? (
          <>
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={trend.series} margin={{ top: 4, right: 12, bottom: 0, left: -12 }}>
                <CartesianGrid stroke={gridColor} />
                <XAxis dataKey="date" tick={{ fontSize: 10, fill: axisColor }} tickFormatter={(d: string) => d.slice(5)} />
                <YAxis domain={[0, 1]} tickFormatter={(v: number) => `${(v * 100).toFixed(0)}%`} tick={{ fontSize: 10, fill: axisColor }} />
                <RTooltip
                  formatter={(v: unknown, name: unknown) => {
                    const num = typeof v === 'number' ? v : Number(v);
                    return [Number.isFinite(num) ? `${(num * 100).toFixed(0)}%` : '—', String(name)];
                  }}
                  contentStyle={{ background: tooltipBg, borderColor: colors.cardBorder, fontSize: 12 }}
                  labelStyle={{ color: axisColor }}
                />
                <Legend wrapperStyle={{ fontSize: 11 }} />
                {trend.series.some((p) => (p.p_hike ?? 0) > 0.001) && (
                  <Line type="monotone" dataKey="p_hike" name="Hike" stroke={colors.hawkColor} dot={false} strokeWidth={2} />
                )}
                {trend.series.some((p) => (p.p_hold ?? 0) > 0.001) && (
                  <Line type="monotone" dataKey="p_hold" name="Hold" stroke={colors.neutralTagColor} dot={false} strokeWidth={1.5} />
                )}
                {trend.series.some((p) => (p.p_cut ?? 0) > 0.001) && (
                  <Line type="monotone" dataKey="p_cut" name="Cut" stroke={colors.doveColor} dot={false} strokeWidth={2} />
                )}
              </LineChart>
            </ResponsiveContainer>
            {trend.series.every((p) => (p.p_cut ?? 0) <= 0.001) && (
              <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 6 }}>
                Cut is 0% on this window: implied funds rate sits at or above hold, so the model splits probability only between Hold and Hike. It never assigns Cut and Hike at the same time.
              </Text>
            )}
          </>
        ) : (
          <Text type="secondary" style={{ fontSize: 12 }}>Trend will build as daily snapshots accumulate.</Text>
        )}
      </Spin>

      {probabilities.expectations_vs_actual.length > 0 && (
        <div style={{ marginTop: 16 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>Expectation vs actual</Text>
          <Table
            size="small"
            style={{ marginTop: 6 }}
            pagination={false}
            dataSource={probabilities.expectations_vs_actual}
            rowKey="meeting_date"
            onRow={(r) => ({
              style: isoDate(r.meeting_date) === isoDate(selectedMeeting)
                ? { background: colors.stripActive }
                : undefined,
            })}
            columns={[
              {
                title: 'Meeting', dataIndex: 'meeting_date', width: 110,
                render: (d: string) => <Text style={{ color: colors.text, fontSize: 12 }}>{d}</Text>,
              },
              {
                title: 'Implied (pre-meeting)', dataIndex: 'implied_direction', width: 140,
                render: (dir: string | null, r) => (
                  <Text style={{
                    fontSize: 12,
                    color: dir === 'hike' ? colors.hawkColor : dir === 'cut' ? colors.doveColor : colors.neutralTagColor,
                  }}>
                    {dir ? `${dir} ${fmtPct(Math.max(r.implied_p?.hike ?? 0, r.implied_p?.cut ?? 0, r.implied_p?.hold ?? 0))}` : 'n/a'}
                  </Text>
                ),
              },
              {
                title: 'Actual', dataIndex: 'actual_direction', width: 90,
                render: (dir: string | null, r) => (
                  <Text style={{
                    fontSize: 12,
                    color: dir === 'hike' ? colors.hawkColor : dir === 'cut' ? colors.doveColor : colors.text,
                  }}>
                    {dir ?? '—'}{r.actual_change_bp ? ` ${r.actual_change_bp > 0 ? '+' : ''}${r.actual_change_bp}bp` : ''}
                  </Text>
                ),
              },
              {
                title: '', dataIndex: 'hit', width: 60,
                render: (hit: boolean | null) =>
                  hit === null ? <Text type="secondary">n/a</Text>
                  : hit ? <Tag color="success">hit</Tag> : <Tag color="error">miss</Tag>,
              },
            ]}
          />
        </div>
      )}
      <Paragraph type="secondary" style={{ fontSize: 11, marginTop: 10, marginBottom: 0 }}>
        {probabilities.method_note || 'Self-computed from 30-Day Fed Funds futures (Yahoo) + EFFR (FRED). May deviate a few pp from CME FedWatch.'}
      </Paragraph>
    </Card>
  );
};

export default CentralBankDashboardPage;

import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
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
  Select,
  Table,
  Segmented,
  Alert,
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
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip as RTooltip,
  ResponsiveContainer, Legend,
} from 'recharts';
import {
  fomcAPI, DashboardData, DecisionCard, SummaryInfo, UpdateItem,
  ProbabilitiesPayload, NextMeetingProb, RecentMeeting, Dissenter,
} from '../services/fomc';
import { marketsAPI, EconomicEvent } from '../services/markets';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import MarkdownRenderer from '../components/MarkdownRenderer';

const { Title, Text, Paragraph } = Typography;

type BankView = 'overview' | 'fed' | 'pboc' | 'boj';

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

const utcToday = () => new Date().toISOString().slice(0, 10);

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

const parseBank = (raw?: string): BankView => {
  if (raw === 'fed' || raw === 'pboc' || raw === 'boj') return raw;
  return 'overview';
};

const CN_EVENT_RE = /loan prime|lpr|mlf|medium[- ]term lending|reserve requirement|pboc|interest rate/i;
const BOJ_EVENT_RE = /boj.*interest rate decision|interest rate decision/i;

type RateHit = { date: string; event: string; actual?: string | number | null; previous?: string | number | null };

const pickLastNext = (events: EconomicEvent[], matcher: (e: EconomicEvent) => boolean): { last?: RateHit; next?: RateHit } => {
  const today = utcToday();
  const matched = events
    .filter((e) => e.date && matcher(e))
    .sort((a, b) => (a.date || '').localeCompare(b.date || ''));
  const toHit = (e: EconomicEvent): RateHit => ({
    date: e.date as string, event: e.event || '', actual: e.actual, previous: e.previous,
  });
  const last = [...matched].reverse().find((e) => (e.date || '') <= today && e.actual !== null && e.actual !== undefined && e.actual !== '');
  const next = matched.find((e) => (e.date || '') > today);
  return { last: last ? toHit(last) : undefined, next: next ? toHit(next) : undefined };
};

const splitMarkdownSections = (md: string): { title: string; body: string }[] => {
  if (!md.trim()) return [];
  const chunks = md.split(/^##\s+/m).map((s) => s.trim()).filter(Boolean);
  return chunks.map((chunk) => {
    const nl = chunk.indexOf('\n');
    if (nl < 0) return { title: chunk, body: '' };
    return { title: chunk.slice(0, nl).trim(), body: chunk.slice(nl + 1).trim() };
  });
};

const isPrimarySection = (title: string) =>
  /前瞻|措辞|forward guidance|wording/i.test(title);

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
  const { bank: bankParam } = useParams<{ bank?: string }>();
  const navigate = useNavigate();
  const view = parseBank(bankParam);
  const { isAuthenticated } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const colors = THEME_COLORS[themeMode];

  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [latest, setLatest] = useState<DashboardData | null>(null);
  const [slice, setSlice] = useState<{
    decision: DecisionCard | null;
    statement_summary: SummaryInfo | null;
    minutes_summary: SummaryInfo | null;
  } | null>(null);
  const [selectedMeeting, setSelectedMeeting] = useState<string | undefined>(undefined);
  const [sliceLoading, setSliceLoading] = useState(false);

  const [cnEvents, setCnEvents] = useState<EconomicEvent[]>([]);
  const [jpEvents, setJpEvents] = useState<EconomicEvent[]>([]);

  const [monetaryOnly, setMonetaryOnly] = useState(true);
  const [updates, setUpdates] = useState<UpdateItem[]>([]);
  const [updatesTotal, setUpdatesTotal] = useState(0);
  const [updatesPage, setUpdatesPage] = useState(1);
  const [updatesLoading, setUpdatesLoading] = useState(false);

  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pollCount = useRef(0);

  useEffect(() => {
    if (bankParam && bankParam !== 'fed' && bankParam !== 'pboc' && bankParam !== 'boj') {
      navigate('/central-bank', { replace: true });
    }
  }, [bankParam, navigate]);

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

  const fetchAsiaEvents = useCallback(async () => {
    const from = new Date();
    from.setUTCDate(from.getUTCDate() - 180);
    const to = new Date();
    to.setUTCDate(to.getUTCDate() + 90);
    const params = {
      from_date: from.toISOString().slice(0, 10),
      to_date: to.toISOString().slice(0, 10),
      comparison: 'index',
      limit: 400,
    };
    try {
      const [cn, jp] = await Promise.all([
        marketsAPI.getEconomicEvents({ ...params, country: 'CN' }),
        marketsAPI.getEconomicEvents({ ...params, country: 'JP' }),
      ]);
      setCnEvents(cn);
      setJpEvents(jp);
    } catch (error) {
      console.error('Failed to load CN/JP economic events:', error);
    }
  }, []);

  useEffect(() => {
    if (!isAuthenticated) return;
    fetchLatest();
    fetchUpdates(1, true);
    fetchAsiaEvents();
  }, [isAuthenticated, fetchLatest, fetchUpdates, fetchAsiaEvents]);

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

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      await Promise.all([
        fomcAPI.refresh('poll'),
        fomcAPI.refresh('probabilities'),
      ]);
      message.success('Refresh dispatched — new data will appear shortly');
      setTimeout(() => {
        fetchLatest();
        fetchUpdates(updatesPage, monetaryOnly);
        fetchAsiaEvents();
      }, 4000);
    } catch (error) {
      message.error('Failed to dispatch refresh');
    } finally {
      setRefreshing(false);
    }
  };

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

  const pboc = useMemo(() => pickLastNext(cnEvents, (e) => CN_EVENT_RE.test(e.event || '')), [cnEvents]);
  const boj = useMemo(() => pickLastNext(
    jpEvents,
    (e) => BOJ_EVENT_RE.test(e.event || '') && /boj/i.test(e.event || ''),
  ), [jpEvents]);

  const go = (v: BankView) => {
    navigate(v === 'overview' ? '/central-bank' : `/central-bank/${v}`);
  };

  return (
    <div style={{ padding: '24px', background: colors.pageBg, minHeight: '100vh' }}>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
        <Space>
          <BankOutlined style={{ fontSize: 22, color: colors.text }} />
          <Title level={4} style={{ color: colors.text, margin: 0 }}>
            Central Banks
          </Title>
        </Space>
        <Button
          icon={<ReloadOutlined spin={refreshing} />}
          onClick={handleRefresh}
          loading={refreshing}
        >
          Refresh
        </Button>
      </div>

      <Segmented
        value={view}
        onChange={(v) => go(v as BankView)}
        options={[
          { label: 'Overview', value: 'overview' },
          { label: 'Fed', value: 'fed' },
          { label: 'PBoC', value: 'pboc' },
          { label: 'BOJ', value: 'boj' },
        ]}
        style={{ marginBottom: 16 }}
      />

      {loading && !latest ? (
        <Skeleton active paragraph={{ rows: 8 }} />
      ) : view === 'overview' ? (
        <OverviewPanel
          colors={colors}
          latest={latest}
          pboc={pboc}
          boj={boj}
          onOpenBank={go}
        />
      ) : view === 'fed' ? (
        !latest ? (
          <Empty description="No Fed data — click Refresh to poll the Fed RSS feed" />
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
        )
      ) : (
        <PendingDesk
          colors={colors}
          bank={view}
          hit={view === 'pboc' ? pboc : boj}
          toolLabel={view === 'pboc' ? 'PBoC policy tools' : 'BOJ policy rate'}
        />
      )}
    </div>
  );
};

const OverviewPanel: React.FC<{
  colors: ThemeColors;
  latest: DashboardData | null;
  pboc: { last?: RateHit; next?: RateHit };
  boj: { last?: RateHit; next?: RateHit };
  onOpenBank: (v: BankView) => void;
}> = ({ colors, latest, pboc, boj, onOpenBank }) => {
  const d = latest?.decision;
  const next = latest?.probabilities?.next_meetings?.[0];
  const hawks = (d?.dissenters || []).filter((x) => x.direction === 'hawkish');
  const doves = (d?.dissenters || []).filter((x) => x.direction === 'dovish');

  const fedStance = d
    ? `${fmtBp(d.rate_low_bp)}–${fmtBp(d.rate_high_bp)} ${dirLabel[d.decision_direction || ''] || ''}`.trim()
    : 'Pending';
  const fedLast = d
    ? `${fmtDate(d.meeting_date)} · ${d.vote_for ?? '–'}–${d.vote_against ?? '–'}${hawks.length ? ' hawkish' : doves.length ? ' dovish' : ''}`
    : '—';
  const fedNext = next
    ? `${fmtDate(next.meeting_date)}${next.is_sep ? ' SEP' : ''} · hike ${fmtPct(next.latest?.p_hike)}`
    : '—';

  const pbocStance = pboc.last?.actual != null ? String(pboc.last.actual) : 'Desk pending';
  const pbocLast = pboc.last ? `${fmtDate(pboc.last.date)} · ${pboc.last.event}` : 'No EODHD print yet';
  const pbocNext = pboc.next ? `${fmtDate(pboc.next.date)} · ${pboc.next.event}` : 'No upcoming print in calendar';

  const bojStance = boj.last?.actual != null ? `${boj.last.actual}%` : 'Desk pending';
  const bojLast = boj.last ? `${fmtDate(boj.last.date)} · ${boj.last.event}` : 'No EODHD print yet';
  const bojNext = boj.next ? `${fmtDate(boj.next.date)} · ${boj.next.event}` : 'No upcoming print in calendar';

  const nextCandidates: { when: string; bank: string; event: string; sort: string }[] = [];
  if (next) {
    nextCandidates.push({
      when: fmtDate(next.meeting_date, true),
      bank: 'Fed',
      event: `FOMC${next.is_sep ? ' + SEP' : ''} · ${daysLabel(next.days_away)}`,
      sort: next.meeting_date,
    });
  }
  const mins = latest?.minutes_summary;
  const stmt = latest?.statement_summary;
  if (mins && stmt && (mins.meeting_date || '') < (stmt.meeting_date || '')) {
    nextCandidates.push({
      when: 'Pending',
      bank: 'Fed',
      event: `Minutes for ${fmtDate(stmt.meeting_date)} not out`,
      sort: stmt.meeting_date || '9999',
    });
  } else if (mins?.meeting_date) {
    const age = daysFromToday(mins.published_at || mins.generated_at || mins.meeting_date);
    if (age !== null && age >= -5 && age <= 0) {
      nextCandidates.push({
        when: fmtDate(mins.published_at || mins.meeting_date, true),
        bank: 'Fed',
        event: `Minutes · ${daysLabel(age)}`,
        sort: (mins.published_at || mins.meeting_date || '').slice(0, 10),
      });
    }
  }
  if (pboc.next) {
    nextCandidates.push({
      when: fmtDate(pboc.next.date, true), bank: 'PBoC', event: pboc.next.event, sort: pboc.next.date,
    });
  }
  if (boj.next) {
    nextCandidates.push({
      when: fmtDate(boj.next.date, true), bank: 'BOJ', event: boj.next.event, sort: boj.next.date,
    });
  }
  nextCandidates.sort((a, b) => a.sort.localeCompare(b.sort));

  const fedLine = d
    ? `Fed on ${(d.decision_direction || 'hold')} at ${fmtBp(d.rate_high_bp)}${hawks.length ? ` with a hawkish minority (${d.vote_for}–${d.vote_against})` : doves.length ? ` with a dovish minority (${d.vote_for}–${d.vote_against})` : ` · vote ${d.vote_for}–${d.vote_against}`}.`
    : 'Fed stance pending.';
  const pbocLine = pboc.last
    ? `PBoC last print ${pboc.last.event} at ${pboc.last.actual} (${fmtDate(pboc.last.date)}).`
    : 'PBoC desk not connected — EODHD has no matching print in this window.';
  const bojLine = boj.last
    ? `BOJ last decision ${boj.last.actual}% (${fmtDate(boj.last.date)}).`
    : 'BOJ desk not connected — EODHD has no matching print in this window.';
  const soonest = nextCandidates[0];

  return (
    <>
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 16, background: colors.cardBg, borderColor: colors.cardBorder }}
        message="Divergence"
        description={
          <Text style={{ color: colors.text }}>
            {fedLine} {pbocLine} {bojLine}
            {soonest ? ` Next dated event: ${soonest.bank} — ${soonest.event} (${soonest.when}).` : ''}
          </Text>
        }
      />

      <Card
        size="small"
        style={{ marginBottom: 16, background: colors.cardBg, borderColor: colors.cardBorder }}
        title={<Text strong style={{ color: colors.text }}>Stance across G3</Text>}
      >
        <Table
          size="small"
          pagination={false}
          rowKey="bank"
          onRow={(row) => ({
            onClick: () => onOpenBank(row.key),
            style: { cursor: 'pointer' },
          })}
          dataSource={[
            { key: 'fed' as BankView, bank: 'Fed', stance: fedStance, last: fedLast, next: fedNext },
            { key: 'pboc' as BankView, bank: 'PBoC', stance: pbocStance, last: pbocLast, next: pbocNext },
            { key: 'boj' as BankView, bank: 'BOJ', stance: bojStance, last: bojLast, next: bojNext },
          ]}
          columns={[
            {
              title: 'Bank', dataIndex: 'bank', width: 80,
              render: (v: string) => <Text strong style={{ color: colors.text }}>{v}</Text>,
            },
            {
              title: 'Stance', dataIndex: 'stance',
              render: (v: string) => <Text style={{ color: colors.text }}>{v}</Text>,
            },
            {
              title: 'Last official', dataIndex: 'last',
              render: (v: string) => <Text type="secondary">{v}</Text>,
            },
            {
              title: 'Next / priced', dataIndex: 'next',
              render: (v: string) => <Text type="secondary">{v}</Text>,
            },
          ]}
        />
        <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 8 }}>
          Click a row to open that desk. PBoC / BOJ rows use EODHD economic events until their official-text pipelines exist.
        </Text>
      </Card>

      <Card
        size="small"
        style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
        title={<Text strong style={{ color: colors.text }}>Catalysts · next ~90 days</Text>}
      >
        {nextCandidates.length === 0 ? (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No dated catalysts in this window" />
        ) : (
          <Table
            size="small"
            pagination={false}
            rowKey={(r) => `${r.bank}-${r.sort}-${r.event}`}
            dataSource={nextCandidates}
            columns={[
              { title: 'When', dataIndex: 'when', width: 140, render: (v: string) => <Text style={{ color: colors.text }}>{v}</Text> },
              { title: 'Bank', dataIndex: 'bank', width: 80, render: (v: string) => <Tag>{v}</Tag> },
              { title: 'Event', dataIndex: 'event', render: (v: string) => <Text style={{ color: colors.text }}>{v}</Text> },
            ]}
          />
        )}
      </Card>
    </>
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
}) => {
  const next = latest.probabilities?.next_meetings?.[0];

  return (
    <Spin spinning={sliceLoading}>
      <ClockRow colors={colors} next={next} decision={latest.decision} minutes={latest.minutes_summary} statement={latest.statement_summary} />

      <Grid2 colors={colors} decision={decision} probabilities={latest.probabilities} selectedMeeting={selectedMeeting} />

      <MeetingStrip
        colors={colors}
        recent={latest.recent_meetings || []}
        upcoming={latest.probabilities?.next_meetings || []}
        selected={selectedMeeting}
        onSelect={onSelectMeeting}
      />

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: 16, marginBottom: 16 }}>
        <Card
          size="small"
          style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
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

        <Card
          size="small"
          style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
          title={
            <Space>
              <FileTextOutlined />
              <Text strong style={{ color: colors.text }}>Catalysts & feed</Text>
            </Space>
          }
          extra={
            <Space>
              <Text type="secondary" style={{ fontSize: 12 }}>Monetary Policy</Text>
              <Switch size="small" checked={monetaryOnly} onChange={onMonetaryOnly} />
            </Space>
          }
        >
          <CatalystList colors={colors} next={next} minutes={latest.minutes_summary} statement={latest.statement_summary} />
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
      </div>

      <ProbabilitiesDetails colors={colors} probabilities={latest.probabilities} selectedMeeting={selectedMeeting} />
    </Spin>
  );
};

const ClockRow: React.FC<{
  colors: ThemeColors;
  next?: NextMeetingProb;
  decision?: DecisionCard | null;
  minutes?: SummaryInfo | null;
  statement?: SummaryInfo | null;
}> = ({ colors, next, decision, minutes, statement }) => {
  const minsLag = Boolean(
    statement?.meeting_date && minutes?.meeting_date
    && minutes.meeting_date < statement.meeting_date,
  );
  return (
    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 12 }}>
      {next && (
        <Tag color="blue" style={{ margin: 0 }}>
          Next FOMC {fmtDate(next.meeting_date)} · {daysLabel(next.days_away)}
          {next.is_sep ? ' · SEP' : ''}
        </Tag>
      )}
      {decision && (
        <Tag style={{ margin: 0, background: colors.neutralTagBg, color: colors.neutralTagColor, border: 'none' }}>
          Last: {fmtDate(decision.meeting_date)} {dirLabel[decision.decision_direction || ''] || ''}
          {decision.change_bp ? ` ${decision.change_bp > 0 ? '+' : ''}${decision.change_bp}bp` : ' 0bp'}
        </Tag>
      )}
      {minsLag ? (
        <Tag style={{ margin: 0, background: colors.hawkBg, color: colors.hawkColor, border: 'none' }}>
          Minutes for {fmtDate(statement?.meeting_date)} not out
        </Tag>
      ) : minutes?.meeting_date ? (
        <Tag style={{ margin: 0, background: colors.doveBg, color: colors.doveColor, border: 'none' }}>
          Minutes {fmtDate(minutes.meeting_date)} · {daysLabel(daysFromToday(minutes.published_at || minutes.generated_at))}
        </Tag>
      ) : null}
    </div>
  );
};

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
  const delta = hikeDelta7d(liveFocus);

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
        title={<Text strong style={{ color: colors.text }}>Priced in</Text>}
        extra={
          isHistorical && hist ? (
            <Tag style={{ background: colors.neutralTagBg, color: colors.neutralTagColor, border: 'none' }}>
              pre-meeting{hist.asof_date ? ` · as of ${fmtDate(hist.asof_date)}` : ''}
            </Tag>
          ) : liveFocus ? (
            <Tag style={{ background: colors.neutralTagBg, color: colors.neutralTagColor, border: 'none' }}>
              {fmtDate(liveFocus.meeting_date)}{liveFocus.is_sep ? ' SEP' : ''} · {daysLabel(liveFocus.days_away)}
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
              <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 8 }}>
                Market implied {hist?.implied_direction ?? 'n/a'}
                {hist?.actual_direction ? ` · actual ${hist.actual_direction}` : ''}
                {hist?.actual_change_bp ? ` ${hist.actual_change_bp > 0 ? '+' : ''}${hist.actual_change_bp}bp` : ''}
                {hist?.hit === true ? ' · hit' : hist?.hit === false ? ' · miss' : ''}
                {hist?.method === 'zq_front_v1' ? ' · front-month approx.' : ''}
              </Text>
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
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 8 }}>
              {delta !== null && (
                <>7d hike {delta >= 0 ? '+' : ''}{delta.toFixed(0)}pp · </>
              )}
              implied path {impliedPathLabel(probabilities)}
            </Text>
          </>
        ) : (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No snapshot yet — click Refresh" />
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
          const hawks = (m.dissenters || []).filter((x) => x.direction === 'hawkish').length;
          const doves = (m.dissenters || []).filter((x) => x.direction === 'dovish').length;
          const active = isoDate(selected) === isoDate(m.meeting_date);
          return (
            <button
              key={m.meeting_date}
              type="button"
              onClick={() => onSelect(m.meeting_date)}
              style={{
                minWidth: 108, textAlign: 'left', cursor: 'pointer',
                border: `1px solid ${active ? colors.doveColor : colors.cardBorder}`,
                background: active ? colors.stripActive : 'transparent',
                borderRadius: 8, padding: '8px 10px', color: colors.text,
              }}
            >
              <div style={{ fontWeight: 600, fontSize: 13 }}>{fmtDate(m.meeting_date)}</div>
              <div style={{ fontSize: 12, color: colors.textSecondary }}>
                {dirLabel[m.decision_direction || ''] || '—'} {m.vote_for ?? '–'}–{m.vote_against ?? '–'}
              </div>
              <div style={{ fontSize: 11, color: hawks ? colors.hawkColor : doves ? colors.doveColor : colors.textSecondary }}>
                {hawks ? `${hawks} hawk` : doves ? `${doves} dove` : 'unanimous'}
              </div>
            </button>
          );
        })}
        {future.map((m) => {
          const active = isoDate(selected) === isoDate(m.meeting_date);
          return (
            <button
              key={m.meeting_date}
              type="button"
              onClick={() => onSelect(m.meeting_date)}
              style={{
                minWidth: 108, textAlign: 'left', cursor: 'pointer',
                border: `1px dashed ${active ? colors.doveColor : colors.cardBorder}`,
                background: active ? colors.stripActive : 'transparent',
                borderRadius: 8, padding: '8px 10px', color: colors.text,
              }}
            >
              <div style={{ fontWeight: 600, fontSize: 13 }}>
                {fmtDate(m.meeting_date)}{m.is_sep ? ' SEP' : ''}
              </div>
              <div style={{ fontSize: 12, color: colors.textSecondary }}>{daysLabel(m.days_away)}</div>
              <div style={{ fontSize: 11, color: colors.hawkColor }}>hike {fmtPct(m.latest?.p_hike)}</div>
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

const CatalystList: React.FC<{
  colors: ThemeColors;
  next?: NextMeetingProb;
  minutes?: SummaryInfo | null;
  statement?: SummaryInfo | null;
}> = ({ next, minutes, statement }) => {
  const minsLag = Boolean(
    statement?.meeting_date && minutes?.meeting_date
    && minutes.meeting_date < statement.meeting_date,
  );
  return (
    <div style={{ marginBottom: 8 }}>
      {next && (
        <div style={{ marginBottom: 6 }}>
          <Text style={{ fontSize: 13 }}>
            {fmtDate(next.meeting_date, true)} · FOMC{next.is_sep ? ' + SEP' : ''} · {daysLabel(next.days_away)}
          </Text>
        </div>
      )}
      {minsLag ? (
        <div style={{ marginBottom: 6 }}>
          <Text style={{ fontSize: 13 }}>Minutes for {fmtDate(statement?.meeting_date)} still pending</Text>
        </div>
      ) : minutes?.meeting_date ? (
        <div style={{ marginBottom: 6 }}>
          <Text style={{ fontSize: 13 }}>
            Minutes {fmtDate(minutes.meeting_date, true)} · {daysLabel(daysFromToday(minutes.published_at || minutes.generated_at))}
          </Text>
        </div>
      ) : null}
    </div>
  );
};

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
    return <div style={{ padding: '24px 0', textAlign: 'center' }}><Spin tip="AI summary generating…" /></div>;
  }
  if (summary.status === 'failed') {
    return <Empty description="AI summary failed — will retry on next poll" />;
  }

  const sections = splitMarkdownSections(summary.summary_content || '');
  const primary = sections.filter((s) => isPrimarySection(s.title));
  const rest = sections.filter((s) => !isPrimarySection(s.title));
  const shown = primary.length ? primary : sections.slice(0, 2);

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
      {shown.map((s) => (
        <div key={s.title} style={{ marginBottom: 12 }}>
          <Text strong style={{ color: colors.text }}>{s.title}</Text>
          <div style={{ marginTop: 4 }}>
            <MarkdownRenderer content={s.body} variant={themeMode} />
          </div>
        </div>
      ))}

      {(rest.length > 0 || true) && (
        <Collapse
          ghost
          size="small"
          onChange={loadDoc}
          items={[
            ...(rest.length ? [{
              key: 'rest',
              label: <Text type="secondary" style={{ fontSize: 12 }}>More sections (决议 / 投票 / 经济评估)</Text>,
              children: (
                <div>
                  {rest.map((s) => (
                    <div key={s.title} style={{ marginBottom: 12 }}>
                      <Text strong style={{ color: colors.text }}>{s.title}</Text>
                      <MarkdownRenderer content={s.body} variant={themeMode} />
                    </div>
                  ))}
                </div>
              ),
            }] : []),
            {
              key: 'original',
              label: <Text type="secondary" style={{ fontSize: 12 }}>
                Original text {doc?.word_count ? `(${doc.word_count} words)` : ''}
              </Text>,
              children: (
                <Spin spinning={docLoading}>
                  <Paragraph
                    style={{
                      color: colors.textSecondary, fontSize: 12, maxHeight: 300, overflow: 'auto',
                      whiteSpace: 'pre-wrap', background: colors.neutralTagBg, padding: 12, borderRadius: 6,
                    }}
                  >
                    {doc?.raw_content ?? 'Loading…'}
                  </Paragraph>
                </Spin>
              ),
            },
            {
              key: 'methods',
              label: <Text type="secondary" style={{ fontSize: 12 }}>Data & methods</Text>,
              children: (
                <Paragraph type="secondary" style={{ fontSize: 12, marginBottom: 0 }}>
                  <RobotOutlined /> template <code>{summary.template_id}</code>
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
              ),
            },
          ]}
        />
      )}
    </div>
  );
};

function hikeDelta7d(m?: NextMeetingProb): number | null {
  if (!m?.latest || !m.trend || m.trend.length < 2) return null;
  const latestP = m.latest.p_hike;
  if (latestP === null || latestP === undefined) return null;
  const end = m.trend[m.trend.length - 1]?.date;
  if (!end) return null;
  const target = Date.parse(`${end}T00:00:00Z`) - 7 * 86400000;
  let best = m.trend[0];
  for (const p of m.trend) {
    const t = Date.parse(`${p.date}T00:00:00Z`);
    if (t <= target) best = p;
  }
  if (best.p_hike === null || best.p_hike === undefined) return null;
  return (latestP - best.p_hike) * 100;
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

  const selected = sel ?? selectedMeeting ?? meetings[0]?.meeting_date;

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
    <Collapse
      size="small"
      style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
      items={[{
        key: 'more',
        label: <Text style={{ color: colors.text }}>Probability trend, path, and model trust</Text>,
        children: (
          <div>
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
              Implied average funds rate: {impliedPathLabel(probabilities)}
            </Text>
            <Space style={{ marginBottom: 8 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>Trend</Text>
              <Select
                size="small"
                value={selected}
                style={{ minWidth: 160 }}
                onChange={setSel}
                options={meetings.map((m) => ({
                  value: m.meeting_date,
                  label: `${m.meeting_date}${m.is_sep ? ' (SEP)' : ''}`,
                }))}
              />
            </Space>
            <Spin spinning={trendLoading}>
              {trend && trend.series.length >= 2 ? (
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
                    <Line type="monotone" dataKey="p_hike" name="Hike" stroke={colors.hawkColor} dot={false} strokeWidth={2} />
                    <Line type="monotone" dataKey="p_hold" name="Hold" stroke={colors.neutralTagColor} dot={false} strokeWidth={1.5} />
                    <Line type="monotone" dataKey="p_cut" name="Cut" stroke={colors.doveColor} dot={false} strokeWidth={2} />
                  </LineChart>
                </ResponsiveContainer>
              ) : (
                <Text type="secondary" style={{ fontSize: 12 }}>Trend will build as daily snapshots accumulate.</Text>
              )}
            </Spin>

            {probabilities.expectations_vs_actual.length > 0 && (
              <div style={{ marginTop: 16 }}>
                <Text type="secondary" style={{ fontSize: 12 }}>Expectation vs actual (past meetings)</Text>
                <Table
                  size="small"
                  style={{ marginTop: 6 }}
                  pagination={false}
                  dataSource={probabilities.expectations_vs_actual}
                  rowKey="meeting_date"
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
          </div>
        ),
      }]}
    />
  );
};

const PendingDesk: React.FC<{
  colors: ThemeColors;
  bank: 'pboc' | 'boj';
  hit: { last?: RateHit; next?: RateHit };
  toolLabel: string;
}> = ({ colors, bank, hit, toolLabel }) => {
  const name = bank === 'pboc' ? 'PBoC' : 'BOJ';
  const slots = bank === 'pboc'
    ? ['7-day OMO', '1Y LPR', '5Y LPR']
    : ['Policy rate', 'Board vote', 'YCC / JGB ops'];
  return (
    <>
      <Alert
        type="warning"
        showIcon
        style={{ marginBottom: 16 }}
        message={`${name} official-text pipeline is not connected yet`}
        description="This desk uses the same slot layout as Fed. Stance below is from EODHD economic events when a matching print exists. Guidance, minutes/outlook, and the updates feed will fill in once the crawler lands — the chrome is locked so we do not redesign later."
      />
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 12 }}>
        {hit.next ? (
          <Tag color="blue">Next {fmtDate(hit.next.date, true)} · {hit.next.event}</Tag>
        ) : (
          <Tag>Next event · pending calendar</Tag>
        )}
        {hit.last && (
          <Tag style={{ background: colors.neutralTagBg, color: colors.neutralTagColor, border: 'none' }}>
            Last: {fmtDate(hit.last.date)} · {hit.last.actual ?? '—'}
          </Tag>
        )}
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: 16, marginBottom: 16 }}>
        <Card
          size="small"
          style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
          title={<Text strong style={{ color: colors.text }}>Stance</Text>}
        >
          <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>{toolLabel}</Text>
          {hit.last ? (
            <div>
              <div style={{ fontSize: 26, fontWeight: 700, color: colors.text }}>{String(hit.last.actual)}</div>
              <Text type="secondary" style={{ fontSize: 13 }}>{hit.last.event} · {fmtDate(hit.last.date, true)}</Text>
              {hit.last.previous != null && (
                <div><Text type="secondary" style={{ fontSize: 12 }}>previous {String(hit.last.previous)}</Text></div>
              )}
            </div>
          ) : (
            <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No matching EODHD print in the last 180 days" />
          )}
          <div style={{ marginTop: 12, display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            {slots.map((s) => (
              <Tag key={s} style={{ background: colors.neutralTagBg, color: colors.neutralTagColor, border: 'none' }}>{s}</Tag>
            ))}
          </div>
        </Card>
        <Card
          size="small"
          style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
          title={<Text strong style={{ color: colors.text }}>Priced in</Text>}
        >
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description={bank === 'pboc'
              ? 'No implied-probability source — slot omitted on purpose (not a fake FedWatch).'
              : 'No JGB/OIS implied path yet. Slot stays empty until a real source is chosen.'}
          />
        </Card>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: 16 }}>
        <Card
          size="small"
          style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
          title={<Text strong style={{ color: colors.text }}>What changed in the text</Text>}
        >
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description={bank === 'pboc'
              ? 'Will hold monetary vs fiscal releases (PBoC news + 执行报告 / MoF).'
              : 'Will hold Statement vs Outlook (Jan/Apr/Jul/Oct), defaulting to whichever is newer.'}
          />
        </Card>
        <Card
          size="small"
          style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
          title={<Text strong style={{ color: colors.text }}>Feed</Text>}
        >
          {hit.next || hit.last ? (
            <List
              size="small"
              dataSource={[hit.next, hit.last].filter(Boolean) as RateHit[]}
              renderItem={(item) => (
                <List.Item>
                  <List.Item.Meta
                    title={<Text style={{ color: colors.text, fontSize: 13 }}>{item.event}</Text>}
                    description={<Text type="secondary" style={{ fontSize: 12 }}>{fmtDate(item.date, true)}{item.actual != null ? ` · ${item.actual}` : ''}</Text>}
                  />
                </List.Item>
              )}
            />
          ) : (
            <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="Updates feed pending crawler" />
          )}
        </Card>
      </div>
    </>
  );
};

export default CentralBankDashboardPage;

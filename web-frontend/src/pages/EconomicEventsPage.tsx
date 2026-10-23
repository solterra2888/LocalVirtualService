import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import {
  Card,
  Space,
  Input,
  Select,
  Button,
  ConfigProvider,
  theme as antdTheme,
  Segmented,
  Spin,
  Empty,
  message,
  Tag,
} from 'antd';
import {
  DownOutlined,
} from '@ant-design/icons';
import dayjs, { Dayjs } from 'dayjs';
import utc from 'dayjs/plugin/utc';
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip as ChartTooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  marketsAPI,
  EconomicEvent,
  CountryInfo,
  EconomicEventCategory,
} from '../services/markets';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { aiTagChipStyle } from '../components/shared/aiTagChipStyles';
import './EconomicCalendar.css';

dayjs.extend(utc);

const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
  },
  dark: {
    pageBg: '#1a202c',
    cardBg: '#1a2234',
    cardBorder: '#364152',
  },
} as const;

type Preset = 'today' | 'thisWeek' | 'nextWeek' | 'thisMonth' | 'nextMonth';
type CountryGroup = 'common' | 'major' | 'g7' | 'all';

/** 常用经济体(高频关注: 美国/中国/日本/德国/英国) */
const COMMON_COUNTRIES = ['US', 'CN', 'JP', 'DE', 'UK'];

/** 主要经济体（对齐 Trading Economics 默认 Major 集合, EODHD 代码: UK=英国 EU=欧元区） */
const MAJOR_COUNTRIES = [
  'US', 'EU', 'UK', 'JP', 'DE', 'FR', 'IT', 'ES', 'CN', 'IN', 'CA', 'AU',
  'BR', 'MX', 'KR', 'ID', 'TR', 'ZA', 'RU', 'SG', 'NZ', 'CH', 'NL', 'SE',
  'PL', 'TH', 'MY', 'PH', 'VN', 'HK',
];
const G7_COUNTRIES = ['US', 'UK', 'JP', 'DE', 'FR', 'IT', 'CA'];

/** ISO2 → 国旗 emoji（EU 等非国家代码同样适用; UK 是 EODHD 别名, 旗帜 emoji 须按 ISO 的 GB 生成, 🇺🇰 不存在） */
const flagEmoji = (code: string): string => {
  const c = (code || '').toUpperCase();
  const iso = c === 'UK' ? 'GB' : c;
  return iso.replace(/./g, (ch) => String.fromCodePoint(127397 + ch.charCodeAt(0)));
};

/** 指标值字符串 → 数值(折线图用): 支持 1,234 / 2.5% / 236K / 1.2M 等; 解析不了返回 null */
const parseNum = (v: string | number | null | undefined): number | null => {
  if (v === null || v === undefined || v === '') return null;
  const s = String(v).trim().replace(/,/g, '');
  const m = s.match(/^(-?[\d.]+)\s*([KMBT])?\s*%?$/i);
  if (!m) return null;
  let n = parseFloat(m[1]);
  if (Number.isNaN(n)) return null;
  const suf = (m[2] || '').toUpperCase();
  if (suf === 'K') n *= 1e3;
  else if (suf === 'M') n *= 1e6;
  else if (suf === 'B') n *= 1e9;
  else if (suf === 'T') n *= 1e12;
  return n;
};

/** Y 轴刻度紧凑格式(220000 → 220K) */
const fmtAxisNum = (n: number): string => {
  const a = Math.abs(n);
  if (a >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (a >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (a >= 1e3) return `${(n / 1e3).toFixed(a >= 1e4 ? 0 : 1)}K`;
  return `${n}`;
};

/** 取某日期所在周的周一 */
const getMonday = (d: Dayjs): Dayjs => {
  const dow = d.day(); // 0=周日
  return d.subtract(dow === 0 ? 6 : dow - 1, 'day').startOf('day');
};

const getWindow = (preset: Preset, anchor: Dayjs): [Dayjs, Dayjs] => {
  const monday = getMonday(anchor);
  switch (preset) {
    case 'today':
      return [anchor.startOf('day'), anchor.startOf('day')];
    case 'thisWeek':
      return [monday, monday.add(6, 'day')];
    case 'nextWeek':
      return [monday.add(7, 'day'), monday.add(13, 'day')];
    case 'thisMonth': {
      const m = anchor.startOf('month');
      return [m, m.endOf('month')];
    }
    case 'nextMonth': {
      const m = anchor.startOf('month').add(1, 'month');
      return [m, m.endOf('month')];
    }
  }
};

const EconomicEventsPage: React.FC = () => {
  const { isAuthenticated } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const tagMode = isDark ? 'dark' : 'light';
  const colors = THEME_COLORS[themeMode];

  const [preset, setPreset] = useState<Preset>('thisWeek');
  const [anchor, setAnchor] = useState<Dayjs>(dayjs().startOf('day'));
  const [countryGroup, setCountryGroup] = useState<CountryGroup>('common');
  const [countries, setCountries] = useState<string[]>(COMMON_COUNTRIES);
  const [impacts, setImpacts] = useState<number[]>([2, 3]);
  const [category, setCategory] = useState<string | undefined>(undefined);
  const [searchText, setSearchText] = useState('');

  const [allCountries, setAllCountries] = useState<CountryInfo[]>([]);
  const [categories, setCategories] = useState<EconomicEventCategory[]>([]);
  const [events, setEvents] = useState<EconomicEvent[]>([]);
  const [loading, setLoading] = useState(false);

  // 行展开: 历史序列（key = country|event|comparison）
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const [historyLoading, setHistoryLoading] = useState(false);
  const historyCache = useRef<Map<string, EconomicEvent[]>>(new Map());
  const [historyVersion, setHistoryVersion] = useState(0);

  const [winFrom, winTo] = useMemo(() => getWindow(preset, anchor), [preset, anchor]);

  /** UTC 存储(date+time) → 本地时区(中国用户即北京时间, 无时区选择器) */
  const toTz = useCallback(
    (date: string, time?: string): Dayjs => {
      const t = time && /^\d{2}:\d{2}/.test(time) ? time.slice(0, 8) : '00:00:00';
      return dayjs.utc(`${date}T${t}`).local();
    },
    []
  );

  const todayKey = useMemo(() => dayjs().format('YYYY-MM-DD'), []);

  const fetchEvents = useCallback(async () => {
    if (!isAuthenticated) return;
    setLoading(true);
    try {
      const result = await marketsAPI.getEconomicEvents({
        from_date: winFrom.format('YYYY-MM-DD'),
        to_date: winTo.format('YYYY-MM-DD'),
        country: countries.length ? countries.join(',') : undefined,
        comparison: 'all',
        impacts: impacts.join(','),
        categories: category,
        order: 'asc',
        limit: 5000,
      });
      setEvents(result);
    } catch (error) {
      console.error('获取经济日历数据失败:', error);
      message.error('Failed to retrieve economic calendar');
    } finally {
      setLoading(false);
    }
  }, [isAuthenticated, winFrom, winTo, countries, impacts, category]);

  // 行点击 → 拉取该事件历史序列（同口径）
  const toggleRow = useCallback(
    async (ev: EconomicEvent) => {
      const key = `${ev.country}|${ev.event}|${ev.comparison ?? ''}`;
      if (expandedKey === key) {
        setExpandedKey(null);
        return;
      }
      setExpandedKey(key);
      if (historyCache.current.has(key)) return;
      setHistoryLoading(true);
      try {
        const rows = await marketsAPI.getEconomicEvents({
          country: ev.country,
          event: ev.event,
          comparison: ev.comparison || 'index',
          order: 'desc',
          limit: 30,
        });
        historyCache.current.set(key, rows);
        setHistoryVersion((v) => v + 1);
      } catch (error) {
        console.error('获取事件历史序列失败:', error);
      } finally {
        setHistoryLoading(false);
      }
    },
    [expandedKey]
  );

  useEffect(() => {
    fetchEvents();
  }, [fetchEvents]);

  useEffect(() => {
    if (!isAuthenticated) return;
    marketsAPI
      .getEconomicEventsCountries()
      .then(setAllCountries)
      .catch(() => undefined);
    marketsAPI
      .getEconomicEventCategories()
      .then(setCategories)
      .catch(() => undefined);
  }, [isAuthenticated]);

  const applyGroup = (g: CountryGroup) => {
    setCountryGroup(g);
    if (g === 'common') setCountries(COMMON_COUNTRIES);
    else if (g === 'major') setCountries(MAJOR_COUNTRIES);
    else if (g === 'g7') setCountries(G7_COUNTRIES);
    else setCountries(allCountries.map((c) => c.code));
  };

  /** 搜索过滤 + 按所选时区分组 + 组内排序 */
  const dayGroups = useMemo(() => {
    void historyVersion; // 历史缓存更新后触发重渲染
    const kw = searchText.trim().toLowerCase();
    const filtered = kw
      ? events.filter(
          (e) =>
            (e.event || '').toLowerCase().includes(kw) ||
            (e.country || '').toLowerCase().includes(kw)
        )
      : events;
    const groups = new Map<string, EconomicEvent[]>();
    for (const ev of filtered) {
      if (!ev.date) continue;
      const key = toTz(ev.date, ev.time).format('YYYY-MM-DD');
      const list = groups.get(key);
      if (list) list.push(ev);
      else groups.set(key, [ev]);
    }
    // Array.from 而非 [...spread]: 项目 tsconfig target 较低, spread 迭代器需 downlevelIteration
    const sorted = Array.from(groups.entries()).sort((a, b) => a[0].localeCompare(b[0]));
    for (const [, list] of sorted) {
      list.sort((a, b) => {
        const ta = toTz(a.date!, a.time).valueOf();
        const tb = toTz(b.date!, b.time).valueOf();
        if (ta !== tb) return ta - tb;
        return (b.impact ?? 1) - (a.impact ?? 1);
      });
    }
    return sorted;
  }, [events, searchText, toTz, historyVersion]);

  const countryNameMap = useMemo(() => {
    const m = new Map<string, string>();
    for (const c of allCountries) m.set(c.code, c.name);
    return m;
  }, [allCountries]);

  const renderValue = (v: string | number | null | undefined, strong = false) => {
    if (v === null || v === undefined || v === '') return <span className="ee-val">–</span>;
    const s = String(v);
    const neg = s.trimStart().startsWith('-');
    return (
      <span className={`ee-val${strong ? ' ee-val-strong' : ''}${neg ? ' ee-val-neg' : ''}`}>
        {s}
      </span>
    );
  };

  if (!isAuthenticated) {
    return (
      <div style={{ padding: 24 }}>
        <Card>
          <Empty description="Please log in to view the economic calendar" />
        </Card>
      </div>
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
          // 顶部 padding 为 0: 查询卡片顶边与左侧导航栏顶边(viewport y=12px)对齐 —— 同 EarningsCalendarPage
          padding: '0 24px 24px 0',
          height: 'calc(100vh - 24px)',
          backgroundColor: colors.pageBg,
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
        }}
      >
        <Card className="app-controls" style={{ marginBottom: 12 }}>
          <Space size={12} wrap>
            {/* 日期范围预设（TE Recent 班次的常用子集） */}
            <Segmented
              options={[
                { label: 'Today', value: 'today' },
                { label: 'This Week', value: 'thisWeek' },
                { label: 'Next Week', value: 'nextWeek' },
                { label: 'This Month', value: 'thisMonth' },
                { label: 'Next Month', value: 'nextMonth' },
              ]}
              value={preset}
              onChange={(v) => setPreset(v as Preset)}
            />
            {/* 重要度多选（TE Impact: ★/★★/★★★） */}
            <div className="app-ctl-pills">
              {[1, 2, 3].map((n) => (
                <Button
                  key={n}
                  size="small"
                  type={impacts.includes(n) ? 'primary' : 'default'}
                  onClick={() =>
                    setImpacts((prev) =>
                      prev.includes(n) ? prev.filter((i) => i !== n) : [...prev, n]
                    )
                  }
                >
                  {'★'.repeat(n)}
                </Button>
              ))}
            </div>
            {/* 国家组快捷切换 + 多选 */}
            <Segmented
              options={[
                { label: 'Common', value: 'common' },
                { label: 'Major', value: 'major' },
                { label: 'G7', value: 'g7' },
                { label: 'All', value: 'all' },
              ]}
              value={countryGroup}
              onChange={(v) => applyGroup(v as CountryGroup)}
            />
            <Select
              mode="multiple"
              showSearch
              maxTagCount="responsive"
              placeholder="Countries"
              value={countries}
              onChange={(v) => {
                setCountries(v);
                setCountryGroup('major');
              }}
              options={allCountries.map((c) => ({
                value: c.code,
                label: `${flagEmoji(c.code)} ${c.code} · ${c.name}`,
              }))}
              style={{ minWidth: 260 }}
              allowClear
            />
            {/* 类别（TE Category） */}
            <Select
              showSearch
              allowClear
              placeholder="All Categories"
              value={category}
              onChange={(v) => setCategory(v)}
              options={categories.map((c) => ({ value: c.slug, label: c.name }))}
              style={{ minWidth: 160 }}
            />
            {/* 搜索 */}
            <Input.Search
              placeholder="Search event / country"
              allowClear
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              style={{ width: 190 }}
            />
          </Space>
        </Card>

        <Card
          style={{ flex: 1, minHeight: 0, overflow: 'hidden' }}
          styles={{ body: { height: '100%', padding: 12 } }}
        >
          <div className="ee-cal">
            <div className="ee-cal-scroll">
              {loading && events.length === 0 ? (
                <div style={{ textAlign: 'center', padding: 48 }}>
                  <Spin />
                </div>
              ) : dayGroups.length === 0 ? (
                <Empty description="No events in this window" style={{ marginTop: 48 }} />
              ) : (
                dayGroups.map(([dateKey, list]) => (
                  <div className="ee-day" key={dateKey}>
                    <div className="ee-day-header">
                      <div className="ee-day-title">
                        <span>{dayjs(dateKey).format('dddd, MMMM DD YYYY')}</span>
                        {dateKey === todayKey && (
                          <Tag bordered={false} style={aiTagChipStyle('signal-neutral', tagMode)}>
                            Today
                          </Tag>
                        )}
                        <span className="ee-day-count">{list.length} events</span>
                      </div>
                      <div className="ee-cols">
                        <span>Time</span>
                        <span>Impact</span>
                        <span>Country</span>
                        <span>Event</span>
                        <span>Actual</span>
                        <span>Previous</span>
                        <span>Forecast</span>
                        <span />
                      </div>
                    </div>

                    {list.map((ev, idx) => {
                      const key = `${ev.country}|${ev.event}|${ev.comparison ?? ''}`;
                      const open = expandedKey === key;
                      const imp = Math.min(3, Math.max(1, ev.impact ?? 1));
                      return (
                        <React.Fragment key={`${key}|${ev.date}|${idx}`}>
                          <div
                            className={`ee-row${open ? ' ee-row-open' : ''}`}
                            onClick={() => toggleRow(ev)}
                          >
                            <span className={`ee-time ee-imp-${imp}`}>
                              {ev.time ? toTz(ev.date!, ev.time).format('HH:mm') : ''}
                            </span>
                            <span className={`ee-stars ee-imp-${imp}`}>{'★'.repeat(imp)}</span>
                            <span className="ee-country">
                              <span className="ee-country-flag">{flagEmoji(ev.country || '')}</span>
                              {ev.country}
                            </span>
                            <span className="ee-event">
                              <span className="ee-event-name">{ev.event}</span>
                              {ev.period && (
                                <Tag bordered={false} style={aiTagChipStyle('ticker', tagMode)}>
                                  {ev.period}
                                </Tag>
                              )}
                              {ev.comparison && (
                                <Tag bordered={false} style={aiTagChipStyle('signal-meta', tagMode)}>
                                  {ev.comparison}
                                </Tag>
                              )}
                            </span>
                            <span>{renderValue(ev.actual, true)}</span>
                            <span>{renderValue(ev.previous)}</span>
                            <span>{renderValue(ev.forecast)}</span>
                            <span className="ee-chevron">
                              <DownOutlined
                                style={{
                                  transform: open ? 'none' : 'rotate(-90deg)',
                                  transition: 'transform 0.15s',
                                }}
                              />
                            </span>
                          </div>

                          {open && (
                            <div className="ee-history">
                              <div className="ee-history-title">
                                {ev.event}
                                {ev.country
                                  ? ` · ${countryNameMap.get(ev.country) || ev.country}`
                                  : ''}
                                {ev.comparison ? ` (${ev.comparison})` : ''}
                              </div>
                              {historyLoading && !historyCache.current.has(key) ? (
                                <Spin size="small" />
                              ) : (
                                (() => {
                                  const rows = historyCache.current.get(key) || [];
                                  const chartData = rows
                                    .map((h) => ({
                                      label: h.date ? dayjs(h.date).format('YY/MM') : '',
                                      actual: parseNum(h.actual),
                                      fullDate: h.date,
                                      actualRaw: h.actual ?? '–',
                                      previousRaw: h.previous ?? '–',
                                      forecastRaw: h.forecast ?? '–',
                                    }))
                                    .filter((p) => p.actual !== null)
                                    .reverse();
                                  // 兜底: 数值解析不出来(<2 个可画点)时仍展示数据表, 避免空展开
                                  if (chartData.length < 2) {
                                    return (
                                      <table>
                                        <thead>
                                          <tr>
                                            <th>Release</th>
                                            <th>Actual</th>
                                            <th>Previous</th>
                                            <th>Forecast</th>
                                          </tr>
                                        </thead>
                                        <tbody>
                                          {rows.slice(0, 12).map((h, i) => (
                                            <tr key={i}>
                                              <td>{h.date}</td>
                                              <td className={String(h.actual ?? '').startsWith('-') ? 'ee-val-neg' : ''}>
                                                {h.actual ?? '–'}
                                              </td>
                                              <td>{h.previous ?? '–'}</td>
                                              <td>{h.forecast ?? '–'}</td>
                                            </tr>
                                          ))}
                                        </tbody>
                                      </table>
                                    );
                                  }
                                  const axisColor = isDark
                                    ? 'rgba(255,255,255,0.45)'
                                    : 'rgba(0,0,0,0.45)';
                                  return (
                                    <div className="ee-history-chart">
                                      <ResponsiveContainer width="100%" height={150}>
                                        <LineChart
                                          data={chartData}
                                          margin={{ top: 6, right: 12, bottom: 0, left: 0 }}
                                        >
                                          <CartesianGrid
                                            stroke={isDark ? '#364152' : '#f0f0f0'}
                                            vertical={false}
                                          />
                                          <XAxis
                                            dataKey="label"
                                            tick={{ fontSize: 10, fill: axisColor }}
                                            minTickGap={28}
                                          />
                                          <YAxis
                                            tick={{ fontSize: 10, fill: axisColor }}
                                            tickFormatter={fmtAxisNum}
                                            width={48}
                                            domain={['auto', 'auto']}
                                          />
                                          <ChartTooltip
                                            isAnimationActive={false}
                                            content={(props: any) => {
                                              const { active, payload } = props || {};
                                              if (!active || !payload || !payload.length) return null;
                                              const p = payload[0].payload;
                                              return (
                                                <div
                                                  style={{
                                                    background: isDark ? '#1a2234' : '#fff',
                                                    border: `1px solid ${isDark ? '#364152' : '#e5e8ec'}`,
                                                    borderRadius: 6,
                                                    padding: '4px 8px',
                                                    fontSize: 11,
                                                    lineHeight: 1.6,
                                                  }}
                                                >
                                                  <div style={{ fontWeight: 600 }}>{p.fullDate}</div>
                                                  <div>
                                                    Actual: <b>{p.actualRaw}</b>
                                                  </div>
                                                  <div>Previous: {p.previousRaw}</div>
                                                  <div>Forecast: {p.forecastRaw}</div>
                                                </div>
                                              );
                                            }}
                                          />
                                          <Line
                                            type="monotone"
                                            dataKey="actual"
                                            stroke={isDark ? '#4096ff' : '#1890ff'}
                                            strokeWidth={1.5}
                                            dot={false}
                                            activeDot={{ r: 3 }}
                                            isAnimationActive={false}
                                          />
                                        </LineChart>
                                      </ResponsiveContainer>
                                    </div>
                                  );
                                })()
                              )}
                            </div>
                          )}
                        </React.Fragment>
                      );
                    })}
                  </div>
                ))
              )}
            </div>
          </div>
        </Card>
      </div>
    </ConfigProvider>
  );
};

export default EconomicEventsPage;

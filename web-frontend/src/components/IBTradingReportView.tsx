import React, { useMemo, useState } from 'react';
import {
  Card,
  Table,
  Descriptions,
  ConfigProvider,
  theme as antdTheme,
  Tabs,
  Empty,
  Select,
  Space,
  DatePicker,
} from 'antd';
import dayjs from 'dayjs';
import type { Dayjs } from 'dayjs';

const { RangePicker } = DatePicker;

export interface IBReportData {
  _type: 'ib_trading_report';
  account_id: string;
  period: string;
  /** Account base currency from IB Account Information (MTM summary amounts are in this currency). */
  base_currency?: string;
  tables: Record<string, Record<string, string>[]>;
  orders: Record<string, string>[];
  markdown: string;
}

export function isIBReportContent(content: string | null | undefined): IBReportData | null {
  if (!content) return null;
  const trimmed = content.trimStart();
  if (!trimmed.startsWith('{"_type"')) return null;
  try {
    const parsed = JSON.parse(trimmed);
    if (parsed?._type === 'ib_trading_report') return parsed as IBReportData;
  } catch {
    // not JSON
  }
  return null;
}

function fmtNum(val: string | undefined): string {
  if (!val) return '';
  const s = val.replace(/,/g, '');
  const n = Number(s);
  if (isNaN(n) || s.trim() === '') return val;
  return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

const numRender = (v: string) => fmtNum(v);

/** MTM / generic: Symbol and IB code columns must stay plain text (avoid 600230 → 600,230.00). */
function isTickerOrCodeColumn(key: string): boolean {
  if (key === 'Symbol' || key === 'Code' || key === '代码') return true;
  if (key.startsWith('代码')) return true;
  return false;
}

/** Columns to omit from Mark-to-Market tables (EN + 中文 IB 导出). */
const MTM_HIDDEN_COLUMNS = new Set([
  'Prior Quantity',
  '先前 数量',
  '先前数量',
  'Prior Price',
  '先前 价格',
  '先前价格',
  'Mark-to-Market P/L Other',
  '按市值计盈亏 其它',
  '按市值计盈亏其它',
  'Code_1',
  '代码_1',
]);

const ORDER_COLUMNS = [
  { title: '资产分类', dataIndex: 'asset_class', width: 80 },
  { title: '货币', dataIndex: 'currency', width: 60 },
  { title: '代码', dataIndex: 'symbol', width: 100 },
  { title: '日期/时间', dataIndex: 'trade_time', width: 160 },
  { title: '数量', dataIndex: 'quantity', width: 80, align: 'right' as const, render: numRender },
  { title: '交易价格', dataIndex: 'trade_price', width: 100, align: 'right' as const, render: numRender },
  { title: '收盘价格', dataIndex: 'close_price', width: 100, align: 'right' as const, render: numRender },
  { title: '收益', dataIndex: 'proceeds', width: 100, align: 'right' as const, render: numRender },
  { title: '佣金/税', dataIndex: 'commission', width: 90, align: 'right' as const, render: numRender },
  { title: '基础', dataIndex: 'basis', width: 100, align: 'right' as const, render: numRender },
  { title: '已实现损益', dataIndex: 'realized_pnl', width: 100, align: 'right' as const, render: numRender },
  { title: '按市值计损益', dataIndex: 'mtm_pnl', width: 110, align: 'right' as const, render: numRender },
  { title: '交易代码', dataIndex: 'trade_code', width: 80 },
];

interface Props {
  data: IBReportData;
  isDark: boolean;
}

function parseOrderTradeDate(tradeTime: string | undefined): Dayjs | null {
  if (!tradeTime?.trim()) return null;
  const raw = tradeTime.trim();
  const datePart = raw.includes(',') ? raw.split(',')[0].trim() : raw.split(/\s+/)[0] || raw;
  const d = dayjs(datePart);
  return d.isValid() ? d : null;
}

const IBTradingReportView: React.FC<Props> = ({ data, isDark }) => {
  const [tradesTicker, setTradesTicker] = useState<string | undefined>(undefined);
  const [tradesDateRange, setTradesDateRange] = useState<[Dayjs | null, Dayjs | null] | null>(null);
  const cardStyle: React.CSSProperties = {
    marginBottom: 16,
    borderRadius: 8,
    ...(isDark ? { background: '#1a2234', borderColor: 'rgba(255,255,255,0.06)' } : {}),
  };

  const renderAccountInfo = () => {
    const rows = data.tables['账户信息'] || [];
    if (!rows.length) return null;
    return (
      <Card title="账户信息" size="small" style={cardStyle}>
        <Descriptions column={2} size="small" bordered>
          {rows.map((row, i) => (
            <Descriptions.Item key={i} label={row['域名称'] || ''}>
              {fmtNum(row['域值']) || ''}
            </Descriptions.Item>
          ))}
        </Descriptions>
      </Card>
    );
  };

  const renderGenericTable = (tableName: string, cardTitle?: string) => {
    const rows = data.tables[tableName] || [];
    if (!rows.length) return null;
    const keys = Object.keys(rows[0]);
    const columns = keys.map((k) => ({
      title: k,
      dataIndex: k,
      key: k,
      ellipsis: true,
      render: (v: string) => (isTickerOrCodeColumn(k) ? v ?? '' : fmtNum(v)),
    }));
    return (
      <Card title={cardTitle ?? tableName} size="small" style={cardStyle}>
        <Table
          dataSource={rows.map((r, i) => ({ ...r, _key: i }))}
          columns={columns}
          rowKey="_key"
          size="small"
          pagination={false}
          scroll={{ x: 'max-content' }}
        />
      </Card>
    );
  };

  /** Mark-to-Market: text columns for Symbol / 代码; numeric formatting for amounts. */
  const renderMtmTable = (
    rows: Record<string, string>[],
    cardTitle: string,
    hideAssetCategory?: boolean,
  ) => {
    if (!rows.length) return null;
    const skip = new Set<string>(MTM_HIDDEN_COLUMNS);
    if (hideAssetCategory) {
      skip.add('Asset Category');
      skip.add('资产分类');
    }
    const keys = Object.keys(rows[0]).filter((k) => !skip.has(k));
    const columns = keys.map((k) => ({
      title: k,
      dataIndex: k,
      key: k,
      ellipsis: true,
      render: (v: string) => (isTickerOrCodeColumn(k) ? v ?? '' : fmtNum(v)),
    }));
    return (
      <Card title={cardTitle} size="small" style={cardStyle}>
        <Table
          dataSource={rows.map((r, i) => ({ ...r, _key: i }))}
          columns={columns}
          rowKey="_key"
          size="small"
          pagination={false}
          scroll={{ x: 'max-content' }}
        />
      </Card>
    );
  };

  const renderMarkToMarket = () => {
    const stocks = data.tables['按市值计算的表现总结_股票'] || [];
    const forex = data.tables['按市值计算的表现总结_外汇'] || [];
    const legacy = data.tables['按市值计算的表现总结'] || [];

    if (stocks.length > 0 || forex.length > 0) {
      return (
        <>
          {renderMtmTable(stocks, 'Stocks 股票', true)}
          {renderMtmTable(forex, 'Forex 外汇', true)}
        </>
      );
    }
    if (legacy.length > 0) {
      return renderMtmTable(legacy, 'Mark-to-Market Performance', false);
    }
    return <Empty description="暂无按市值计算的表现总结" image={Empty.PRESENTED_IMAGE_SIMPLE} />;
  };

  const renderNavChange = () => {
    const rows = data.tables['净资产值变更'] || [];
    if (!rows.length) return null;
    return (
      <Card title="净资产值变更" size="small" style={cardStyle}>
        <Descriptions column={2} size="small" bordered>
          {rows.map((row, i) => (
            <Descriptions.Item key={i} label={row['域名称'] || ''}>
              {fmtNum(row['域值']) || ''}
            </Descriptions.Item>
          ))}
        </Descriptions>
      </Card>
    );
  };

  const tradesTickerOptions = useMemo(() => {
    const set = new Set<string>();
    for (const o of data.orders) {
      const sym = (o.symbol || '').trim();
      if (sym) set.add(sym);
    }
    return Array.from(set)
      .sort()
      .map((v) => ({ label: v, value: v }));
  }, [data.orders]);

  const tradesFilteredOrders = useMemo(() => {
    let list = data.orders;
    if (tradesTicker) {
      list = list.filter((o) => (o.symbol || '').trim() === tradesTicker);
    }
    const dr = tradesDateRange;
    if (dr?.[0] && dr[1]) {
      const from = dr[0].format('YYYY-MM-DD');
      const to = dr[1].format('YYYY-MM-DD');
      list = list.filter((o) => {
        const d = parseOrderTradeDate(o.trade_time);
        if (!d) return false;
        const ds = d.format('YYYY-MM-DD');
        return ds >= from && ds <= to;
      });
    }
    return list;
  }, [data.orders, tradesTicker, tradesDateRange]);

  const orderDataSource = useMemo(
    () => tradesFilteredOrders.map((o, i) => ({ ...o, _key: i })),
    [tradesFilteredOrders],
  );

  const renderOrders = () => {
    if (!data.orders.length) {
      return <Empty description="暂无交易明细" image={Empty.PRESENTED_IMAGE_SIMPLE} />;
    }
    const total = data.orders.length;
    const shown = tradesFilteredOrders.length;
    const titleSuffix =
      shown === total ? `共 ${shown} 笔` : `显示 ${shown} 笔（全部 ${total} 笔）`;
    return (
      <Card title={`Trades（${titleSuffix}）`} size="small" style={cardStyle}>
        <Space wrap style={{ marginBottom: 12 }} size="small">
          <Select
            allowClear
            placeholder="Ticker"
            style={{ minWidth: 160 }}
            value={tradesTicker}
            onChange={(v) => setTradesTicker(v)}
            options={tradesTickerOptions}
            showSearch
            optionFilterProp="label"
          />
          <RangePicker
            value={tradesDateRange}
            onChange={(v) => setTradesDateRange(v)}
            allowClear
            placeholder={['开始日期', '结束日期']}
          />
        </Space>
        <Table
          dataSource={orderDataSource}
          columns={ORDER_COLUMNS}
          rowKey="_key"
          size="small"
          pagination={{ pageSize: 50, showSizeChanger: true, showTotal: (t) => `共 ${t} 条` }}
          scroll={{ x: 'max-content' }}
        />
      </Card>
    );
  };

  const overviewContent = (
    <>
      {renderAccountInfo()}
      {renderGenericTable('净资产值')}
      {renderNavChange()}
    </>
  );

  const tabItems = [
    {
      key: 'overview',
      label: 'Overview',
      children: <div style={{ paddingTop: 8 }}>{overviewContent}</div>,
    },
    {
      key: 'mtm',
      label: 'Mark-to-Market Performance',
      children: <div style={{ paddingTop: 8 }}>{renderMarkToMarket()}</div>,
    },
    {
      key: 'trades',
      label: 'Trades',
      children: <div style={{ paddingTop: 8 }}>{renderOrders()}</div>,
    },
  ];

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        token: {
          fontSize: 10,
          ...(isDark ? {
            colorBgContainer: '#1a2234',
            colorBorderSecondary: 'rgba(255,255,255,0.06)',
          } : {}),
        },
      }}
    >
      <div style={{ padding: '16px 20px', overflowY: 'auto', height: '100%', fontSize: 10 }}>
        <div style={{ marginBottom: 16 }}>
          <h2 style={{ margin: 0, fontSize: 18, fontWeight: 600 }}>
            Interactive Brokers 活动报表{data.account_id ? ` — ${data.account_id}` : ''}
          </h2>
          {data.period && (
            <div style={{ marginTop: 4, fontSize: 13, opacity: 0.65 }}>
              报表期间: {data.period}
            </div>
          )}
        </div>

        <Tabs defaultActiveKey="overview" items={tabItems} size="small" />
      </div>
    </ConfigProvider>
  );
};

export default IBTradingReportView;

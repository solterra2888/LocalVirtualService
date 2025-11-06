import React from 'react';
import { Table, Tag, Empty } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { EconomicEvent, EarningsEvent, IPOEvent } from '../../types/marketWeekly';

// 经济日历表格
export const EconomicCalendarTable: React.FC<{ data: EconomicEvent[] }> = ({ data }) => {
  const columns: ColumnsType<EconomicEvent> = [
    {
      title: 'Date',
      dataIndex: 'date',
      key: 'date',
      width: 120,
      render: (date: string) => {
        const d = new Date(date);
        return d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
      }
    },
    {
      title: 'Event',
      dataIndex: 'event',
      key: 'event',
      ellipsis: true
    },
    {
      title: 'Country',
      dataIndex: 'country',
      key: 'country',
      width: 80,
      render: (country: string) => <Tag>{country}</Tag>
    },
    {
      title: 'Actual',
      dataIndex: 'actual',
      key: 'actual',
      width: 80,
      align: 'center'
    },
    {
      title: 'Forecast',
      dataIndex: 'forecast',
      key: 'forecast',
      width: 80,
      align: 'center'
    },
    {
      title: 'Previous',
      dataIndex: 'previous',
      key: 'previous',
      width: 80,
      align: 'center'
    }
  ];

  if (!data || data.length === 0) {
    return <Empty description="No high/medium-impact US economic events scheduled for next week" />;
  }

  return (
    <Table
      columns={columns}
      dataSource={data}
      rowKey={(record, index) => `${record.date}-${index}`}
      pagination={false}
      size="small"
      scroll={{ x: 800 }}
      className="calendar-table"
    />
  );
};

// 财报日历表格
export const EarningsCalendarTable: React.FC<{ data: EarningsEvent[] }> = ({ data }) => {
  const columns: ColumnsType<EarningsEvent> = [
    {
      title: 'Date',
      dataIndex: 'date',
      key: 'date',
      width: 120,
      render: (date: string) => {
        const d = new Date(date);
        return d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
      }
    },
    {
      title: 'Time',
      dataIndex: 'time',
      key: 'time',
      width: 120,
      render: (time: string) => {
        let color = 'default';
        if (time.includes('Before')) color = 'blue';
        else if (time.includes('After')) color = 'orange';
        return <Tag color={color}>{time}</Tag>;
      }
    },
    {
      title: 'Ticker',
      dataIndex: 'ticker',
      key: 'ticker',
      width: 100,
      render: (ticker: string) => <strong>{ticker}</strong>
    },
    {
      title: 'Company',
      dataIndex: 'company',
      key: 'company',
      ellipsis: true
    }
  ];

  if (!data || data.length === 0) {
    return <Empty description="No S&P 500 earnings scheduled for next week" />;
  }

  return (
    <Table
      columns={columns}
      dataSource={data}
      rowKey={(record, index) => `${record.ticker}-${index}`}
      pagination={{ pageSize: 20, showSizeChanger: false }}
      size="small"
      scroll={{ x: 600 }}
      className="calendar-table"
    />
  );
};

// IPO日历表格
export const IPOCalendarTable: React.FC<{ data: IPOEvent[] }> = ({ data }) => {
  const columns: ColumnsType<IPOEvent> = [
    {
      title: 'Date',
      dataIndex: 'date',
      key: 'date',
      width: 120,
      render: (date: string) => {
        const d = new Date(date);
        return d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
      }
    },
    {
      title: 'Ticker',
      dataIndex: 'ticker',
      key: 'ticker',
      width: 100,
      render: (ticker: string) => <strong>{ticker}</strong>
    },
    {
      title: 'Company',
      dataIndex: 'company',
      key: 'company',
      ellipsis: true
    },
    {
      title: 'Exchange',
      dataIndex: 'exchange',
      key: 'exchange',
      width: 120,
      render: (exchange: string) => <Tag>{exchange}</Tag>
    },
    {
      title: 'Deal Type',
      dataIndex: 'deal_type',
      key: 'deal_type',
      width: 100
    },
    {
      title: 'Price Range',
      dataIndex: 'price_range',
      key: 'price_range',
      width: 120
    },
    {
      title: 'Shares',
      dataIndex: 'shares',
      key: 'shares',
      width: 120,
      align: 'right'
    }
  ];

  if (!data || data.length === 0) {
    return <Empty description="No IPOs scheduled for US or Hong Kong exchanges next week" />;
  }

  return (
    <Table
      columns={columns}
      dataSource={data}
      rowKey={(record, index) => `${record.ticker}-${index}`}
      pagination={false}
      size="small"
      scroll={{ x: 900 }}
      className="calendar-table"
    />
  );
};


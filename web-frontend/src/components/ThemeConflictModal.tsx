/**
 * Theme Conflict Resolution Modal
 * 
 * 当创建主题时发现theme_id冲突，显示此Modal让用户选择要保留的tickers
 */

import React, { useState, useEffect } from 'react';
import { Modal, Checkbox, Table, Alert, Space, Typography, Tag, Divider, Button } from 'antd';
import { CheckCircleOutlined, PlusOutlined, MinusCircleOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { getModalStyles } from '../utils/modalStyles';

const { Text, Title } = Typography;

interface ThemeConflictModalProps {
  visible: boolean;
  onCancel: () => void;
  onConfirm: (selectedTickers: string[]) => void;
  existingTheme: {
    theme_id: string;
    theme_name: string;
    category: string;
    description: string;
    group?: string;
    created_by: string;
    tickers: string[];
  };
  newTickers: string[];
  loading?: boolean;
}

interface TickerItem {
  ticker: string;
  status: 'new' | 'existing' | 'duplicate';
  statusText: string;
  statusColor: string;
}

const ThemeConflictModal: React.FC<ThemeConflictModalProps> = ({
  visible,
  onCancel,
  onConfirm,
  existingTheme,
  newTickers,
  loading = false
}) => {
  const { themeMode } = useDashboardSettings();
  const [selectedTickers, setSelectedTickers] = useState<string[]>([]);
  
  // 计算ticker分类
  const tickerAnalysis = React.useMemo(() => {
    const existingSet = new Set(existingTheme.tickers);
    const newSet = new Set(newTickers);
    
    // 新增的 tickers（新的有，旧的没有）
    const addedTickers = newTickers.filter(t => !existingSet.has(t));
    
    // 重复的 tickers（新的有，旧的也有）
    const duplicateTickers = newTickers.filter(t => existingSet.has(t));
    
    // 原有的 tickers（旧的有，新的没有）
    const existingOnlyTickers = existingTheme.tickers.filter(t => !newSet.has(t));
    
    // 合并并去重所有 tickers
    const combinedTickers = existingTheme.tickers.concat(newTickers);
    const uniqueSet = new Set(combinedTickers);
    const allTickers = Array.from(uniqueSet);
    
    return {
      added: addedTickers,
      duplicate: duplicateTickers,
      existingOnly: existingOnlyTickers,
      allTickers: allTickers
    };
  }, [existingTheme.tickers, newTickers]);
  
  // 构建表格数据
  const tableData: TickerItem[] = React.useMemo(() => {
    const data: TickerItem[] = [];
    
    // 新增的
    tickerAnalysis.added.forEach(ticker => {
      data.push({
        ticker,
        status: 'new',
        statusText: '新增',
        statusColor: 'green'
      });
    });
    
    // 重复的
    tickerAnalysis.duplicate.forEach(ticker => {
      data.push({
        ticker,
        status: 'duplicate',
        statusText: '重复',
        statusColor: 'blue'
      });
    });
    
    // 原有的
    tickerAnalysis.existingOnly.forEach(ticker => {
      data.push({
        ticker,
        status: 'existing',
        statusText: '原有',
        statusColor: 'default'
      });
    });
    
    return data;
  }, [tickerAnalysis]);
  
  // 默认选中：新增的 + 重复的 + 原有的（全选）
  useEffect(() => {
    if (visible) {
      setSelectedTickers(tickerAnalysis.allTickers);
    }
  }, [visible, tickerAnalysis.allTickers]);
  
  // 表格列定义
  const columns: ColumnsType<TickerItem> = [
    {
      title: 'Ticker',
      dataIndex: 'ticker',
      key: 'ticker',
      width: '40%',
      render: (ticker: string) => <Text strong code>{ticker}</Text>
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      width: '30%',
      render: (_: any, record: TickerItem) => (
        <Tag 
          color={record.statusColor}
          icon={
            record.status === 'new' ? <PlusOutlined /> :
            record.status === 'duplicate' ? <CheckCircleOutlined /> :
            <MinusCircleOutlined />
          }
        >
          {record.statusText}
        </Tag>
      ),
      filters: [
        { text: '新增', value: 'new' },
        { text: '重复', value: 'duplicate' },
        { text: '原有', value: 'existing' }
      ],
      onFilter: (value, record) => record.status === value
    },
    {
      title: 'Select',
      key: 'select',
      width: '30%',
      render: (_: any, record: TickerItem) => (
        <Checkbox
          checked={selectedTickers.includes(record.ticker)}
          onChange={(e) => {
            if (e.target.checked) {
              setSelectedTickers([...selectedTickers, record.ticker]);
            } else {
              setSelectedTickers(selectedTickers.filter(t => t !== record.ticker));
            }
          }}
        />
      )
    }
  ];
  
  // 快捷选择
  const handleSelectAll = () => {
    setSelectedTickers(tickerAnalysis.allTickers);
  };
  
  const handleSelectNone = () => {
    setSelectedTickers([]);
  };
  
  const handleSelectNew = () => {
    setSelectedTickers([...tickerAnalysis.added, ...tickerAnalysis.duplicate]);
  };
  
  const handleSelectExisting = () => {
    setSelectedTickers([...tickerAnalysis.existingOnly, ...tickerAnalysis.duplicate]);
  };
  
  const handleConfirm = () => {
    onConfirm(selectedTickers);
  };
  
  return (
    <Modal
      title={
        <Space direction="vertical" size={0}>
          <Title level={4} style={{ margin: 0 }}>
            Theme Conflict Detected
          </Title>
          <Text type="secondary">
            Theme "{existingTheme.theme_name}" - {existingTheme.category} already exists
          </Text>
        </Space>
      }
      open={visible}
      onCancel={onCancel}
      onOk={handleConfirm}
      okText={`Update with ${selectedTickers.length} Ticker(s)`}
      cancelText="Cancel"
      width={800}
      confirmLoading={loading}
      okButtonProps={{ disabled: selectedTickers.length === 0 }}
      styles={getModalStyles(themeMode)}
    >
      <Space direction="vertical" style={{ width: '100%' }} size="large">
        {/* 冲突信息 */}

        
        {/* 统计信息 */}
        <Space size="large" wrap>
          <Space>
            <Tag color="green" icon={<PlusOutlined />}>New</Tag>
            <Text strong>{tickerAnalysis.added.length}</Text>
          </Space>
          <Space>
            <Tag color="blue" icon={<CheckCircleOutlined />}>Duplicate</Tag>
            <Text strong>{tickerAnalysis.duplicate.length}</Text>
          </Space>
          <Space>
            <Tag color="default" icon={<MinusCircleOutlined />}>Existing Only</Tag>
            <Text strong>{tickerAnalysis.existingOnly.length}</Text>
          </Space>
          <Divider type="vertical" />
          <Space>
            <Text type="secondary">Selected:</Text>
            <Text strong>{selectedTickers.length}</Text>
            <Text type="secondary">/ {tickerAnalysis.allTickers.length}</Text>
          </Space>
        </Space>
        
        {/* 快捷选择按钮 */}
        <Space wrap>
          <Text type="secondary">Quick Select:</Text>
          <Button type="link" onClick={handleSelectAll} style={{ padding: 0, height: 'auto' }}>
            All
          </Button>
          <Button type="link" onClick={handleSelectNone} style={{ padding: 0, height: 'auto' }}>
            None
          </Button>
          <Button type="link" onClick={handleSelectNew} style={{ padding: 0, height: 'auto' }}>
            New + Duplicate
          </Button>
          <Button type="link" onClick={handleSelectExisting} style={{ padding: 0, height: 'auto' }}>
            Existing + Duplicate
          </Button>
        </Space>
        
        {/* Ticker列表 */}
        <Table
          columns={columns}
          dataSource={tableData}
          rowKey="ticker"
          size="small"
          pagination={false}
          scroll={{ y: 300 }}
        />
        
        {/* 提示信息 */}
        <Alert
          type="info"
          showIcon
          message="After confirmation"
          description={
            <ul style={{ marginBottom: 0, paddingLeft: 20 }}>
              <li>Old ticker associations will be removed</li>
              <li>Selected tickers will be associated with this theme</li>
              <li>Theme description will be regenerated using LLM</li>
              <li>Theme vector will be updated</li>
            </ul>
          }
        />
      </Space>
    </Modal>
  );
};

export default ThemeConflictModal;


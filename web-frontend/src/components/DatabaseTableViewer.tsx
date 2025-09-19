import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Table,
  Button,
  Space,
  Typography,
  Tag,
  Tooltip,
  Row,
  Col,
  Statistic,
  Drawer,
  Descriptions,
  message,
  Empty,
  Spin
} from 'antd';
import {
  ReloadOutlined,
  InfoCircleOutlined,
  EyeOutlined,
  TableOutlined,
  CheckCircleOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { schedulerAPI } from '../services/scheduler';

const { Text } = Typography;

interface TableData {
  table_name: string;
  columns: string[];
  rows: any[];
  total_count: number;
}

interface DatabaseTableViewerProps {
  // Remove unused props
  refreshTrigger?: number; // Add refresh trigger prop
}

const DatabaseTableViewer: React.FC<DatabaseTableViewerProps> = ({ refreshTrigger }) => {
  const [tableData, setTableData] = useState<TableData | null>(null);
  const [loading, setLoading] = useState(false);
  const [selectedTable] = useState('task_scheduling');

  const [detailVisible, setDetailVisible] = useState(false);
  const [selectedRecord, setSelectedRecord] = useState<any>(null);

  // Table configuration - only keep scheduled task configuration table
  const tableConfigs = {
    task_scheduling: {
      name: 'Scheduled Task Configuration',
      keyColumn: 'id',
      statusColumn: 'enabled',
      color: '#1890ff',
      icon: <TableOutlined />
    }
  };

  const currentConfig = tableConfigs[selectedTable as keyof typeof tableConfigs];

  const loadTableData = useCallback(async (tableName: string) => {
    try {
      setLoading(true);
      const response = await schedulerAPI.getDatabaseTable(tableName, 100);
      
      if (response.success && response.data) {
        setTableData(response.data);
      } else {
        message.error(response.message || 'Failed to load table data');
        setTableData(null);
      }
    } catch (error) {
      console.error('Failed to load table data:', error);
      message.error(`Failed to load table data: ${error instanceof Error ? error.message : 'Unknown error'}`);
      setTableData(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadTableData(selectedTable);
  }, [selectedTable, loadTableData]);

  // Refresh data when refreshTrigger changes
  useEffect(() => {
    if (refreshTrigger !== undefined) {
      loadTableData(selectedTable);
    }
  }, [refreshTrigger, selectedTable, loadTableData]);



  const handleViewRecord = (record: any) => {
    setSelectedRecord(record);
    setDetailVisible(true);
  };

  const renderStatusTag = (value: any, column: string) => {
    if (column === 'enabled') {
      return <Tag color={value ? 'green' : 'red'}>{value ? 'Enabled' : 'Disabled'}</Tag>;
    }
    if (column === 'status') {
      const statusColors = {
        'success': 'green',
        'running': 'blue',
        'failed': 'red',
        'error': 'red',
        'completed': 'green',
        'pending': 'orange'
      };
      return <Tag color={statusColors[value as keyof typeof statusColors] || 'default'}>{value || '-'}</Tag>;
    }
    return value;
  };

  const renderCellContent = (value: any, column: string) => {
    if (value === null || value === undefined) {
      return <Text type="secondary">-</Text>;
    }

    // Special handling for status column
    if (column === currentConfig?.statusColumn) {
      return renderStatusTag(value, column);
    }

    // JSON object handling
    if (typeof value === 'object') {
      return (
        <Tooltip title={<pre style={{ fontSize: '11px' }}>{JSON.stringify(value, null, 2)}</pre>}>
          <Text code style={{ fontSize: '10px', cursor: 'pointer' }}>
            {JSON.stringify(value).slice(0, 50)}...
          </Text>
        </Tooltip>
      );
    }

    // Boolean value handling
    if (typeof value === 'boolean') {
      return <Tag color={value ? 'green' : 'red'}>{value ? 'true' : 'false'}</Tag>;
    }

    // Timestamp handling
    if (column.includes('_at') && typeof value === 'string') {
      return (
        <Tooltip title={new Date(value).toLocaleString()}>
          <Text style={{ fontSize: '11px' }}>
            {dayjs(value).format('MM-DD HH:mm')}
          </Text>
        </Tooltip>
      );
    }

    // ID column handling
    if (column === 'id' && typeof value === 'string') {
      return (
        <Text code style={{ fontSize: '10px' }}>
          {value.length > 16 ? `${value.slice(0, 8)}...${value.slice(-8)}` : value}
        </Text>
      );
    }

    // Long text handling
    const stringValue = String(value);
    if (stringValue.length > 50) {
      return (
        <Tooltip title={stringValue}>
          <Text style={{ fontSize: '11px' }}>
            {stringValue.slice(0, 50)}...
          </Text>
        </Tooltip>
      );
    }

    return (
      <Text style={{ fontSize: '11px' }}>
        {stringValue}
      </Text>
    );
  };

  const getTableColumns = () => {
    if (!tableData || !tableData.columns) return [];

    // Create normalized column configuration for scheduled task configuration table
    if (selectedTable === 'task_scheduling') {
      return [
        {
          title: 'Name',
          dataIndex: 'name',
          key: 'name',
          width: 200,
          render: (value: any) => renderCellContent(value, 'name'),
          sorter: (a: any, b: any) => {
            const aVal = a.name;
            const bVal = b.name;
            if (aVal === null || aVal === undefined) return -1;
            if (bVal === null || bVal === undefined) return 1;
            return aVal.localeCompare(bVal);
          }
        },
        {
          title: 'Type',
          dataIndex: 'type',
          key: 'type',
          width: 150,
          render: (value: any) => {
            const typeMap: Record<string, string> = {
              'eodhd_news': 'EODHD News',
              'economic_events': 'Economic Events',
              'macro_indicators': 'Macro Indicators'
            };
            return (
              <Tag color="blue" style={{ fontSize: '11px' }}>
                {typeMap[value] || value}
              </Tag>
            );
          },
          sorter: (a: any, b: any) => {
            const aVal = a.type;
            const bVal = b.type;
            if (aVal === null || aVal === undefined) return -1;
            if (bVal === null || bVal === undefined) return 1;
            return aVal.localeCompare(bVal);
          }
        },
        {
          title: 'Timezone',
          dataIndex: 'timezone',
          key: 'timezone',
          width: 150,
          render: (value: any) => renderCellContent(value, 'timezone'),
          sorter: (a: any, b: any) => {
            const aVal = a.timezone;
            const bVal = b.timezone;
            if (aVal === null || aVal === undefined) return -1;
            if (bVal === null || bVal === undefined) return 1;
            return aVal.localeCompare(bVal);
          }
        },
        {
          title: 'Enabled',
          dataIndex: 'enabled',
          key: 'enabled',
          width: 100,
          render: (value: any) => renderStatusTag(value, 'enabled'),
          sorter: (a: any, b: any) => {
            const aVal = a.enabled;
            const bVal = b.enabled;
            if (aVal === bVal) return 0;
            return aVal ? 1 : -1;
          }
        },
        {
          title: 'Success Rate',
          dataIndex: 'success_rate',
          key: 'success_rate',
          width: 120,
          render: (value: any, record: any) => {
            const successCount = record.success_count || 0;
            const failCount = record.fail_count || 0;
            const total = successCount + failCount;
            
            if (total === 0) {
              return <Text type="secondary" style={{ fontSize: '11px' }}>No Data</Text>;
            }
            
            const rate = (successCount / total * 100).toFixed(1);
            const color = parseFloat(rate) >= 80 ? 'green' : parseFloat(rate) >= 60 ? 'orange' : 'red';
            
            return (
              <Space direction="vertical" size={0}>
                <Tag color={color} style={{ fontSize: '11px' }}>
                  {rate}%
                </Tag>
                <Text type="secondary" style={{ fontSize: '10px' }}>
                  {successCount}/{total}
                </Text>
              </Space>
            );
          },
          sorter: (a: any, b: any) => {
            const aSuccess = a.success_count || 0;
            const aFail = a.fail_count || 0;
            const aTotal = aSuccess + aFail;
            const aRate = aTotal > 0 ? aSuccess / aTotal : 0;
            
            const bSuccess = b.success_count || 0;
            const bFail = b.fail_count || 0;
            const bTotal = bSuccess + bFail;
            const bRate = bTotal > 0 ? bSuccess / bTotal : 0;
            
            return aRate - bRate;
          }
        }
      ];
    }

    // Other tables maintain original dynamic column configuration
    return tableData.columns.map((col: string) => ({
      title: (
        <Space>
          <Text strong style={{ fontSize: '12px' }}>{col}</Text>
          {col === currentConfig?.keyColumn && <Tag color="blue" style={{ fontSize: '10px' }}>Primary Key</Tag>}
          {col === currentConfig?.statusColumn && <Tag color="green" style={{ fontSize: '10px' }}>Status</Tag>}
        </Space>
      ),
      dataIndex: col,
      key: col,
      width: getColumnWidth(col),
      render: (value: any) => renderCellContent(value, col),
      sorter: (a: any, b: any) => {
        const aVal = a[col];
        const bVal = b[col];
        if (aVal === null || aVal === undefined) return -1;
        if (bVal === null || bVal === undefined) return 1;
        if (typeof aVal === 'string' && typeof bVal === 'string') {
          return aVal.localeCompare(bVal);
        }
        return aVal > bVal ? 1 : -1;
      }
    }));
  };

  const getColumnWidth = (column: string): number => {
    if (column === 'id') return 200;
    if (column.includes('params') || column.includes('data') || column.includes('message')) return 250;
    if (column.includes('_at')) return 150;
    if (column === 'type' || column === 'status') return 120;
    return 180;
  };

  const getTableStats = () => {
    if (!tableData || !tableData.rows) return null;

    const data = tableData.rows;
    
    if (selectedTable === 'task_scheduling') {
      const enabled = data.filter((r: any) => r.enabled).length;
      const totalSuccess = data.reduce((sum: number, r: any) => sum + (r.success_count || 0), 0);
      const totalFail = data.reduce((sum: number, r: any) => sum + (r.fail_count || 0), 0);
      const avgSuccessRate = totalSuccess + totalFail > 0 ? (totalSuccess / (totalSuccess + totalFail) * 100).toFixed(1) : '0.0';
      
      return { 
        enabled, 
        total: data.length,
        avgSuccessRate
      };
    }

    return { total: data.length };
  };

  const stats = getTableStats();

  return (
    <div>
      {/* Table selection and statistics */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24}>
          <Card size="small">
            <Space direction="vertical" style={{ width: '100%' }}>
              <Space>
                <Text strong>Scheduled Task Configuration</Text>
                <Button
                  icon={<ReloadOutlined />}
                  onClick={() => loadTableData(selectedTable)}
                  loading={loading}
                  size="small"
                >
                  Refresh
                </Button>
              </Space>
            </Space>
          </Card>
        </Col>
        

      </Row>

      {/* Statistics cards */}
      {stats && (
        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          <Col xs={12} sm={6}>
            <Card>
              <Statistic
                title="Total Records"
                value={stats.total}
                prefix={<TableOutlined />}
                valueStyle={{ color: currentConfig?.color }}
              />
            </Card>
          </Col>
          
          {selectedTable === 'task_scheduling' && stats.enabled !== undefined && (
            <>
              <Col xs={12} sm={6}>
                <Card>
                  <Statistic
                    title="Enabled"
                    value={stats.enabled}
                    prefix={<CheckCircleOutlined />}
                    valueStyle={{ color: '#52c41a' }}
                  />
                </Card>
              </Col>
              <Col xs={12} sm={6}>
                <Card>
                  <Statistic
                    title="Average Success Rate"
                    value={stats.avgSuccessRate}
                    suffix="%"
                    prefix={<CheckCircleOutlined />}
                    valueStyle={{ color: '#1890ff' }}
                  />
                </Card>
              </Col>
            </>
          )}
          

        </Row>
      )}

      {/* Data table */}
      <Card
        title={
          <Space>
            {currentConfig?.icon}
            <span>{currentConfig?.name}</span>
          </Space>
        }
        extra={
          tableData && (
            <Text type="secondary" style={{ fontSize: '12px' }}>
              Total {tableData.total_count} records, {tableData.columns.length} fields
            </Text>
          )
        }
      >
        {loading ? (
          <div style={{ textAlign: 'center', padding: '60px 0' }}>
            <Spin size="large" />
            <div style={{ marginTop: 16 }}>
              <Text type="secondary">Loading table data...</Text>
            </div>
          </div>
        ) : tableData && tableData.rows ? (
          <Table
            columns={[
              ...getTableColumns(),
              {
                title: 'Actions',
                key: 'actions',
                width: 80,
                fixed: 'right',
                render: (record) => (
                  <Button
                    type="text"
                    icon={<EyeOutlined />}
                    size="small"
                    onClick={() => handleViewRecord(record)}
                  />
                )
              }
            ]}
            dataSource={tableData.rows}
            rowKey={(record, index) => `${selectedTable}_${record[currentConfig?.keyColumn || 'id'] || index}`}
            pagination={{
              pageSize: 20,
              showSizeChanger: true,
              showQuickJumper: true,
              showTotal: (total, range) => `${range[0]}-${range[1]} records, total ${total}`
            }}
            scroll={{ x: true }}
            size="small"
          />
        ) : (
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description={
              <Space direction="vertical">
                <Text type="secondary">No Data</Text>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  Click refresh button to load data
                </Text>
              </Space>
            }
          />
        )}
      </Card>

      {/* Record details drawer */}
      <Drawer
        title={
          <Space>
            <InfoCircleOutlined />
            Record Details
          </Space>
        }
        open={detailVisible}
        onClose={() => setDetailVisible(false)}
        width={600}
      >
        {selectedRecord && (
          <Descriptions column={1} bordered size="small">
            {Object.entries(selectedRecord).map(([key, value]) => (
              <Descriptions.Item key={key} label={key}>
                {typeof value === 'object' ? (
                  <pre style={{ fontSize: '11px', margin: 0, whiteSpace: 'pre-wrap' }}>
                    {JSON.stringify(value, null, 2)}
                  </pre>
                ) : key.includes('_at') && typeof value === 'string' ? (
                  <div>
                    <Text>{new Date(value).toLocaleString()}</Text>
                    <br />
                    <Text type="secondary" style={{ fontSize: '11px' }}>
                      {dayjs(value).fromNow()}
                    </Text>
                  </div>
                ) : (
                  renderCellContent(value, key)
                )}
              </Descriptions.Item>
            ))}
          </Descriptions>
        )}
      </Drawer>
    </div>
  );
};

export default DatabaseTableViewer;

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Button,
  Space,
  Typography,
  Alert,
  Table,
  Tag,
  Descriptions,
  Divider,
  Select,
  Switch,
  Row,
  Col,
  message
} from 'antd';
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  ExclamationCircleOutlined,
  ReloadOutlined,
  BugOutlined
} from '@ant-design/icons';
import { useAuthStore } from '../store/authStore';
import { useGlobalLogger } from '../contexts/LoggerContext';
import { LogLevel } from '../types/logger';
import { tasksAPI } from '../services/tasks';
import { authAPI } from '../services/auth';
import { api } from '../services/api';

const { Title, Text } = Typography;

interface DiagnosticResult {
  test: string;
  status: 'success' | 'error' | 'warning';
  message: string;
  details?: any;
  timestamp: string;
}

const DiagnosticPage: React.FC = () => {
  const { user, isAuthenticated } = useAuthStore();
  const { actions, logs } = useGlobalLogger();
  const [results, setResults] = useState<DiagnosticResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [systemInfo, setSystemInfo] = useState<any>({});
  // 本地日志级别过滤状态（仅用于诊断页面）
  const [logLevelFilter, setLogLevelFilter] = useState<LogLevel | undefined>(undefined);

  const addResult = (test: string, status: 'success' | 'error' | 'warning', message: string, details?: any) => {
    const result: DiagnosticResult = {
      test,
      status,
      message,
      details,
      timestamp: new Date().toLocaleString()
    };
    setResults(prev => [...prev, result]);
  };

  const runDiagnostics = useCallback(async () => {
    setLoading(true);
    setResults([]);
    
    try {
      // 1. 测试用户认证状态
      addResult(
        '用户认证状态',
        isAuthenticated ? 'success' : 'error',
        isAuthenticated ? `已登录用户: ${user?.username}` : '未登录',
        { user, isAuthenticated }
      );

      // 2. 测试API基础连接
      try {
        const response = await api.get('/health');
        addResult('API健康检查', 'success', 'API服务运行正常', response);
      } catch (error: any) {
        addResult('API健康检查', 'error', `API连接失败: ${error.message}`, {
          error: error.response?.data,
          status: error.response?.status,
          config: error.config
        });
      }

      // 3. 测试认证API
      if (isAuthenticated) {
        try {
          const authResult = await authAPI.verifyToken();
          addResult('认证令牌验证', 'success', '认证令牌有效', authResult);
        } catch (error: any) {
          addResult('认证令牌验证', 'error', `认证验证失败: ${error.message}`, {
            error: error.response?.data,
            status: error.response?.status
          });
        }
      }

      // 4. 测试RC Agent API
      try {
        const taskTypes = await tasksAPI.getTaskTypes();
        addResult(
          'RC Agent API',
          'success',
          `成功获取 ${taskTypes.length} 个RC Agent`,
          taskTypes
        );
      } catch (error: any) {
        addResult('RC Agent API', 'error', `获取RC Agent失败: ${error.message}`, {
          error: error.response?.data,
          status: error.response?.status,
          config: error.config
        });
      }

      // 5. 测试任务执行历史API
      try {
        const executions = await tasksAPI.getTaskExecutions();
        addResult(
          '任务执行历史API',
          'success',
          `成功获取 ${executions.length} 条执行记录`,
          executions
        );
      } catch (error: any) {
        addResult('任务执行历史API', 'error', `获取执行历史失败: ${error.message}`, {
          error: error.response?.data,
          status: error.response?.status
        });
      }

      // 6. 收集系统信息
      const sysInfo = {
        userAgent: navigator.userAgent,
        url: window.location.href,
        apiBaseUrl: process.env.REACT_APP_API_BASE_URL || '/api',
        localStorage: {
          hasToken: !!localStorage.getItem('access_token'),
          tokenPreview: localStorage.getItem('access_token')?.substring(0, 20) + '...',
          hasUser: !!localStorage.getItem('user')
        },
        timestamp: new Date().toISOString()
      };
      setSystemInfo(sysInfo);
      
      addResult('系统信息收集', 'success', '系统信息收集完成', sysInfo);

    } catch (error: any) {
      addResult('诊断过程', 'error', `诊断过程中发生错误: ${error.message}`, error);
    } finally {
      setLoading(false);
    }
  }, [user, isAuthenticated]);

  useEffect(() => {
    runDiagnostics();
  }, [runDiagnostics]);

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'success':
        return <CheckCircleOutlined style={{ color: '#52c41a' }} />;
      case 'error':
        return <CloseCircleOutlined style={{ color: '#ff4d4f' }} />;
      case 'warning':
        return <ExclamationCircleOutlined style={{ color: '#faad14' }} />;
      default:
        return null;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'success':
        return 'success';
      case 'error':
        return 'error';
      case 'warning':
        return 'warning';
      default:
        return 'default';
    }
  };

  const columns = [
    {
      title: 'Test Item',
      dataIndex: 'test',
      key: 'test',
      width: '200px'
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      width: '100px',
      render: (status: string) => (
        <Tag icon={getStatusIcon(status)} color={getStatusColor(status)}>
          {status.toUpperCase()}
        </Tag>
      )
    },
    {
      title: 'Message',
      dataIndex: 'message',
      key: 'message',
      render: (message: string) => (
        <Text style={{ wordBreak: 'break-word' }}>{message}</Text>
      )
    },
    {
      title: 'Time',
      dataIndex: 'timestamp',
      key: 'timestamp',
      width: '150px'
    },
    {
      title: 'Details',
      key: 'details',
      width: '120px',
      render: (_: any, record: DiagnosticResult) => (
        record.details && (
          <Button
            size="small"
            type="link"
            onClick={() => {
              console.log(`${record.test} details:`, record.details);
              message.info('Details have been output to console');
            }}
          >
            View Details
          </Button>
        )
      )
    }
  ];

  const errorCount = results.filter(r => r.status === 'error').length;
  const warningCount = results.filter(r => r.status === 'warning').length;
  const successCount = results.filter(r => r.status === 'success').length;

  return (
    <div>
      <div className="page-header">
        <div className="flex-between">
          <div>
            <Title level={3} className="page-title">
              <BugOutlined /> System Diagnostics
            </Title>
            <div className="page-description">
              Diagnose API connections and system status to help locate issues
            </div>
          </div>
          <Space>
            <Button
              icon={<ReloadOutlined />}
              onClick={runDiagnostics}
              loading={loading}
            >
              Re-diagnose
            </Button>
          </Space>
        </div>
      </div>

      <div className="page-content">
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          {/* 诊断概览 */}
          <Card title="Diagnostic Overview" loading={loading}>
            <Space size="large">
              <div>
                <Text strong>Total Tests: </Text>
                <Text>{results.length}</Text>
              </div>
              <div>
                <Text strong>Successful: </Text>
                <Text style={{ color: '#52c41a' }}>{successCount}</Text>
              </div>
              <div>
                <Text strong>Warnings: </Text>
                <Text style={{ color: '#faad14' }}>{warningCount}</Text>
              </div>
              <div>
                <Text strong>Errors: </Text>
                <Text style={{ color: '#ff4d4f' }}>{errorCount}</Text>
              </div>
            </Space>
          </Card>

          {/* 系统信息 */}
          <Card title="System Information">
            <Descriptions bordered column={2}>
              <Descriptions.Item label="User Agent">
                {systemInfo.userAgent}
              </Descriptions.Item>
              <Descriptions.Item label="Current URL">
                {systemInfo.url}
              </Descriptions.Item>
              <Descriptions.Item label="API Base URL">
                {systemInfo.apiBaseUrl}
              </Descriptions.Item>
              <Descriptions.Item label="Auth Token">
                {systemInfo.localStorage?.hasToken ? 'Exists' : 'Not Found'}
              </Descriptions.Item>
              <Descriptions.Item label="User Info">
                {systemInfo.localStorage?.hasUser ? 'Exists' : 'Not Found'}
              </Descriptions.Item>
              <Descriptions.Item label="Diagnostic Time">
                {systemInfo.timestamp}
              </Descriptions.Item>
            </Descriptions>
          </Card>

          {/* 日志控制 */}
          <Card title="Log Control">
            <Row gutter={[16, 16]}>
              <Col span={12}>
                <Space direction="vertical" style={{ width: '100%' }}>
                  <div>
                    <Text strong>Log Level Filter:</Text>
                  </div>
                  <Select
                    placeholder="Select log level"
                    value={logLevelFilter}
                    onChange={setLogLevelFilter}
                    allowClear
                    style={{ width: '100%' }}
                  >
                    <Select.Option value="debug">DEBUG - Debug information</Select.Option>
                    <Select.Option value="info">INFO - General information</Select.Option>
                    <Select.Option value="warning">WARNING - Warning information</Select.Option>
                    <Select.Option value="error">ERROR - Error information</Select.Option>
                    <Select.Option value="success">SUCCESS - Success information</Select.Option>
                  </Select>
                </Space>
              </Col>
              <Col span={12}>
                <Space direction="vertical" style={{ width: '100%' }}>
                  <div>
                    <Text strong>Log Operations:</Text>
                  </div>
                  <Space wrap>
                    <Button 
                      onClick={() => actions.setShowModal(true)}
                      type="default"
                    >
                      View Detailed Logs
                    </Button>
                    <Button 
                      onClick={() => actions.setShowFloating(true)}
                      type="default"
                    >
                      Show Floating Logs
                    </Button>
                    <Button 
                      onClick={actions.clearLogs}
                      type="default"
                      danger
                    >
                      Clear Logs
                    </Button>
                    <Button 
                      onClick={() => actions.downloadLogs()}
                      type="default"
                    >
                      Download Logs
                    </Button>
                  </Space>
                  <div style={{ marginTop: 8, fontSize: '12px', color: '#666' }}>
                    <Text type="secondary">
                      Current log count: {logs.length} entries
                      {logs.length > 0 && (
                        <>
                          {' '}(Errors: {logs.filter(log => log.level === 'error').length}, 
                          Warnings: {logs.filter(log => log.level === 'warning').length}, 
                          Info: {logs.filter(log => log.level === 'info').length})
                        </>
                      )}
                    </Text>
                    <br />
                    <Text type="secondary">
                      Performance tip: Floating window shows latest 50 entries, detailed modal shows all logs
                    </Text>
                  </div>
                </Space>
              </Col>
            </Row>
          </Card>

          {/* 错误提示 */}
          {errorCount > 0 && (
            <Alert
              message="Errors Found"
              description={`Detected ${errorCount} errors, please check details and contact technical support.`}
              type="error"
              showIcon
              closable
            />
          )}

          {/* 详细结果 */}
          <Card title="Detailed Diagnostic Results">
            <Table
              columns={columns}
              dataSource={results}
              rowKey="test"
              pagination={false}
              size="small"
              loading={loading}
            />
          </Card>

          {/* 故障排除建议 */}
          <Card title="Troubleshooting Suggestions">
            <div>
              <Title level={5}>Common Issues and Solutions:</Title>
              <ul>
                <li><strong>API Connection Failed:</strong> Check if the backend service is running properly and ports are correct</li>
                <li><strong>Authentication Failed:</strong> Try logging in again and check user permissions</li>
                <li><strong>Database Connection Error:</strong> Check database configuration and connection status</li>
                <li><strong>Network Error:</strong> Check network connection and firewall settings</li>
              </ul>
              
              <Divider />
              
              <Title level={5}>Debug Information:</Title>
              <Text>
                Open browser developer tools (F12), check console and network tabs for detailed error information.
                Click "View Details" buttons above to see more technical details in console.
              </Text>
            </div>
          </Card>
        </Space>
      </div>
    </div>
  );
};

export default DiagnosticPage; 
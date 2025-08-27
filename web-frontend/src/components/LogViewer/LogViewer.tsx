import React, { useEffect, useRef, useMemo, useCallback } from 'react';
import { Modal, Button, Space, Typography, Badge, Tooltip, Select } from 'antd';
import {
  ClearOutlined,
  CloseOutlined,
  DownloadOutlined,
  UpOutlined,
  DownOutlined,
  ExclamationCircleOutlined,
  InfoCircleOutlined,
  WarningOutlined,
  CheckCircleOutlined,
  BugOutlined,
  EyeOutlined
} from '@ant-design/icons';
import { LogEntry, LogLevel } from '../../types/logger';

const { Text } = Typography;
const { Option } = Select;

interface LogViewerProps {
  logs: LogEntry[];
  autoScroll: boolean;
  showModal: boolean;
  showFloating: boolean;
  minimizedFloating: boolean;
  onAutoScrollChange: (enabled: boolean) => void;
  onModalClose: () => void;
  onFloatingClose: () => void;
  onMinimizeToggle: () => void;
  onClearLogs: () => void;
  onDownloadLogs: (filename?: string) => void;
  filterSource?: string;
  filterLevel?: LogLevel;
  onFilterSourceChange?: (source: string | undefined) => void;
  onFilterLevelChange?: (level: LogLevel | undefined) => void;
}

// 获取日志级别图标
const getLogLevelIcon = (level: LogLevel) => {
  switch (level) {
    case 'error':
      return <ExclamationCircleOutlined style={{ color: '#ff4d4f' }} />;
    case 'warning':
      return <WarningOutlined style={{ color: '#faad14' }} />;
    case 'info':
      return <InfoCircleOutlined style={{ color: '#1890ff' }} />;
    case 'success':
      return <CheckCircleOutlined style={{ color: '#52c41a' }} />;
    case 'debug':
      return <BugOutlined style={{ color: '#8c8c8c' }} />;
    default:
      return <InfoCircleOutlined style={{ color: '#1890ff' }} />;
  }
};

// 获取日志级别背景色
const getLogLevelColor = (level: LogLevel) => {
  switch (level) {
    case 'error':
      return '#ffebe6';
    case 'warning':
      return '#fff7e6';
    case 'info':
      return '#e6f7ff';
    case 'success':
      return '#f6ffed';
    case 'debug':
      return '#f5f5f5';
    default:
      return '#f5f5f5';
  }
};

// 获取日志级别边框色
const getLogLevelBorderColor = (level: LogLevel) => {
  switch (level) {
    case 'error':
      return '#ff4d4f';
    case 'warning':
      return '#faad14';
    case 'info':
      return '#1890ff';
    case 'success':
      return '#52c41a';
    case 'debug':
      return '#8c8c8c';
    default:
      return '#1890ff';
  }
};

const LogViewer: React.FC<LogViewerProps> = ({
  logs,
  autoScroll,
  showModal,
  showFloating,
  minimizedFloating,
  onAutoScrollChange,
  onModalClose,
  onFloatingClose,
  onMinimizeToggle,
  onClearLogs,
  onDownloadLogs,
  filterSource,
  filterLevel,
  onFilterSourceChange,
  onFilterLevelChange,
}) => {
  const logContainerRef = useRef<HTMLDivElement>(null);

  // 过滤日志并限制显示数量
  const filteredLogs = useMemo(() => {
    const filtered = logs.filter(log => {
      if (filterSource && log.source !== filterSource) return false;
      if (filterLevel && log.level !== filterLevel) return false;
      return true;
    });
    
    // 限制最大显示数量，避免性能问题
    const maxDisplayLogs = 500; // 最多显示500条日志
    if (filtered.length > maxDisplayLogs) {
      return filtered.slice(-maxDisplayLogs); // 取最新的500条
    }
    return filtered;
  }, [logs, filterSource, filterLevel]);

  // 防抖自动滚动到底部
  useEffect(() => {
    if (!autoScroll || !logContainerRef.current) return;
    
    const timeoutId = setTimeout(() => {
      if (logContainerRef.current) {
        logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
      }
    }, 100); // 100ms防抖
    
    return () => clearTimeout(timeoutId);
  }, [filteredLogs, autoScroll]);

  // 获取所有可用的日志源
  const availableSources = Array.from(new Set(logs.map(log => log.source).filter(Boolean)));

  // 优化的日志条目渲染组件
  const LogEntryItem = React.memo<{
    log: LogEntry;
    index: number;
    isCompact?: boolean;
  }>(({ log, index, isCompact = false }) => {
    // 超级简化版本用于浮动窗口
    if (isCompact) {
      return (
        <div 
          key={log.id} 
          style={{ 
            marginBottom: 1,
            padding: '2px 4px',
            backgroundColor: getLogLevelColor(log.level),
            borderRadius: '2px',
            borderLeft: `2px solid ${getLogLevelBorderColor(log.level)}`,
            fontSize: '9px',
            fontFamily: 'monospace',
            lineHeight: '1.2',
            overflow: 'hidden'
          }}
        >
          <span style={{ 
            color: getLogLevelBorderColor(log.level),
            fontWeight: 'bold',
            marginRight: '4px'
          }}>
            {log.level.charAt(0).toUpperCase()}
          </span>
          <span style={{ color: '#666', marginRight: '4px' }}>
            {new Date(log.timestamp).toLocaleTimeString('zh-CN', { 
              hour12: false, 
              hour: '2-digit', 
              minute: '2-digit', 
              second: '2-digit' 
            })}
          </span>
          <span style={{ 
            color: '#333',
            wordBreak: 'break-word',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap'
          }}>
            {log.message}
          </span>
        </div>
      );
    }

    // 详细版本用于模态框
    return (
      <div 
        key={log.id} 
        style={{ 
          marginBottom: 6,
          padding: '8px 10px',
          backgroundColor: getLogLevelColor(log.level),
          borderRadius: '4px',
          borderLeft: `3px solid ${getLogLevelBorderColor(log.level)}`,
          fontSize: '13px',
          fontFamily: 'monospace',
          wordBreak: 'break-word',
          lineHeight: '1.4'
        }}
      >
        <div style={{ 
          display: 'flex', 
          alignItems: 'center', 
          gap: '6px',
          marginBottom: 3
        }}>
          {getLogLevelIcon(log.level)}
          <Text strong style={{ fontSize: 11 }}>
            {log.level.toUpperCase()}
          </Text>
          {log.source && (
            <Text type="secondary" style={{ fontSize: 10 }}>
              [{log.source}]
            </Text>
          )}
          <Text type="secondary" style={{ fontSize: 10 }}>
            {new Date(log.timestamp).toLocaleTimeString()}
          </Text>
        </div>
        <div style={{ 
          color: '#333', 
          fontSize: '12px',
          lineHeight: '1.3',
          wordBreak: 'break-word',
          wordWrap: 'break-word',
          whiteSpace: 'pre-wrap'
        }}>
          {log.message}
        </div>
        {log.details && (
          <div style={{ 
            marginTop: 4,
            padding: '4px 8px',
            backgroundColor: 'rgba(0,0,0,0.05)',
            borderRadius: '2px',
            fontSize: '11px',
            color: '#666'
          }}>
            <details>
              <summary style={{ cursor: 'pointer' }}>详细信息</summary>
              <pre style={{ 
                margin: '4px 0 0 0', 
                fontSize: '10px',
                wordBreak: 'break-word',
                wordWrap: 'break-word',
                whiteSpace: 'pre-wrap',
                overflowWrap: 'break-word'
              }}>
                {JSON.stringify(log.details, null, 2)}
              </pre>
            </details>
          </div>
        )}
      </div>
    );
  });

  // 渲染过滤器
  const renderFilters = () => (
    <Space style={{ marginBottom: 16 }}>
      <Select
        placeholder="按来源过滤"
        value={filterSource}
        onChange={onFilterSourceChange}
        allowClear
        style={{ width: 120 }}
        size="small"
      >
        {availableSources.map(source => (
          <Option key={source} value={source}>{source}</Option>
        ))}
      </Select>
      <Select
        placeholder="按级别过滤"
        value={filterLevel}
        onChange={onFilterLevelChange}
        allowClear
        style={{ width: 100 }}
        size="small"
      >
        <Option value="error">ERROR</Option>
        <Option value="warning">WARNING</Option>
        <Option value="info">INFO</Option>
        <Option value="success">SUCCESS</Option>
        <Option value="debug">DEBUG</Option>
      </Select>
    </Space>
  );

  // 模态框
  const renderModal = () => (
    <Modal
      title={
        <Space>
          <EyeOutlined />
          详细日志
          {filteredLogs.length > 0 && (
            <Badge count={filteredLogs.length} style={{ backgroundColor: '#1890ff' }} />
          )}
        </Space>
      }
      open={showModal}
      onCancel={onModalClose}
      footer={[
        <Button key="autoScroll" onClick={() => onAutoScrollChange(!autoScroll)}>
          自动滚动: {autoScroll ? '开' : '关'}
        </Button>,
        <Button key="clear" onClick={onClearLogs} disabled={logs.length === 0}>
          清空日志
        </Button>,
        <Button key="download" onClick={() => onDownloadLogs()} disabled={logs.length === 0}>
          下载日志
        </Button>,
        <Button key="close" onClick={onModalClose}>
          关闭
        </Button>
      ]}
      width={1000}
    >
      {(onFilterSourceChange || onFilterLevelChange) && renderFilters()}
      <div style={{ maxHeight: 600, overflowY: 'auto' }} ref={logContainerRef}>
        {filteredLogs.length > 0 ? (
          filteredLogs.map((log, index) => (
            <LogEntryItem key={log.id} log={log} index={index} />
          ))
        ) : (
          <div style={{ textAlign: 'center', color: '#999', padding: '40px' }}>
            暂无日志
          </div>
        )}
      </div>
    </Modal>
  );

  // 浮动窗口
  const renderFloatingWindow = () => (
    <div
      style={{
        position: 'fixed',
        right: 20,
        bottom: 20,
        width: 420,
        height: minimizedFloating ? 60 : 320,
        backgroundColor: '#fff',
        border: '1px solid #d9d9d9',
        borderRadius: '6px',
        boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)',
        zIndex: 1050,
        display: 'flex',
        flexDirection: 'column'
      }}
    >
      {/* 标题栏 */}
      <div style={{
        padding: '12px 16px',
        borderBottom: '1px solid #f0f0f0',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        minHeight: '48px',
        backgroundColor: '#fafafa',
        borderRadius: '6px 6px 0 0'
      }}>
        <span style={{ fontSize: '14px', fontWeight: 500 }}>
          实时日志
          {filteredLogs.length > 0 && (
            <Badge 
              count={filteredLogs.length > 50 ? '50+' : filteredLogs.length} 
              style={{ marginLeft: 6 }} 
              size="small" 
            />
          )}
        </span>
        <Space size="small">
          {!minimizedFloating && (
            <>
              <Tooltip title="自动滚动">
                <Button
                  type="text"
                  size="small"
                  icon={<DownOutlined />}
                  onClick={() => onAutoScrollChange(!autoScroll)}
                  style={{ 
                    color: autoScroll ? '#1890ff' : '#d9d9d9',
                    width: '24px',
                    height: '24px',
                    fontSize: '12px'
                  }}
                />
              </Tooltip>
              <Tooltip title="清空">
                <Button
                  type="text"
                  size="small"
                  icon={<ClearOutlined />}
                  onClick={onClearLogs}
                  disabled={logs.length === 0}
                  style={{ width: '24px', height: '24px', fontSize: '12px' }}
                />
              </Tooltip>
              <Tooltip title="下载">
                <Button
                  type="text"
                  size="small"
                  icon={<DownloadOutlined />}
                  onClick={() => onDownloadLogs()}
                  disabled={logs.length === 0}
                  style={{ width: '24px', height: '24px', fontSize: '12px' }}
                />
              </Tooltip>
            </>
          )}
          <Tooltip title={minimizedFloating ? "展开" : "最小化"}>
            <Button
              type="text"
              size="small"
              icon={minimizedFloating ? <UpOutlined /> : <DownOutlined />}
              onClick={onMinimizeToggle}
              style={{ width: '24px', height: '24px', fontSize: '12px' }}
            />
          </Tooltip>
          <Tooltip title="关闭">
            <Button
              type="text"
              size="small"
              icon={<CloseOutlined />}
              onClick={onFloatingClose}
              style={{ width: '24px', height: '24px', fontSize: '12px' }}
            />
          </Tooltip>
        </Space>
      </div>

      {/* 日志内容区域 */}
      {!minimizedFloating && (
        <div style={{ 
          flex: 1, 
          padding: '8px',
          minHeight: 0, // 重要：允许flex子元素缩小
          display: 'flex',
          flexDirection: 'column'
        }}>
          <div 
            ref={logContainerRef}
            style={{ 
              flex: 1,
              overflowY: 'auto', 
              overflowX: 'hidden',
              fontSize: '11px',
              fontFamily: 'monospace',
              backgroundColor: '#fafafa',
              padding: '6px',
              border: '1px solid #e8e8e8',
              borderRadius: '4px',
              minHeight: 0, // 重要：允许滚动
              wordWrap: 'break-word', // 防止长文本溢出
              wordBreak: 'break-word'
            }}
          >
            {filteredLogs.length > 0 ? (
              // 浮动窗口只显示最新的50条日志以提升性能
              filteredLogs.slice(-50).map((log, index) => (
                <LogEntryItem key={log.id} log={log} index={index} isCompact={true} />
              ))
            ) : (
              <div style={{ textAlign: 'center', color: '#999', padding: '20px', fontSize: '12px' }}>
                暂无日志
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );

  return (
    <>
      {showModal && renderModal()}
      {showFloating && renderFloatingWindow()}
    </>
  );
};

export default LogViewer; 
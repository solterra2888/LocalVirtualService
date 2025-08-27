import React, { useMemo, useRef, useEffect, useState } from 'react';
import { Typography } from 'antd';
import { LogEntry, LogLevel } from '../../types/logger';
import {
  ExclamationCircleOutlined,
  InfoCircleOutlined,
  WarningOutlined,
  CheckCircleOutlined,
  BugOutlined,
} from '@ant-design/icons';

const { Text } = Typography;

interface VirtualizedLogViewerProps {
  logs: LogEntry[];
  height: number;
  itemHeight: number;
  filterSource?: string;
  filterLevel?: LogLevel;
  isCompact?: boolean;
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

// 虚拟化日志条目组件
const VirtualLogItem = React.memo<{
  log: LogEntry;
  style: React.CSSProperties;
  isCompact?: boolean;
}>(({ log, style, isCompact = false }) => (
  <div 
    style={{
      ...style,
      marginBottom: isCompact ? 2 : 6,
      padding: isCompact ? '3px 5px' : '8px 10px',
      backgroundColor: getLogLevelColor(log.level),
      borderRadius: isCompact ? '2px' : '4px',
      borderLeft: `${isCompact ? 2 : 3}px solid ${getLogLevelBorderColor(log.level)}`,
      fontSize: isCompact ? '10px' : '13px',
      fontFamily: 'monospace',
      wordBreak: 'break-word',
      lineHeight: isCompact ? '1.2' : '1.4',
      boxSizing: 'border-box'
    }}
  >
    <div style={{ 
      display: 'flex', 
      alignItems: 'center', 
      gap: isCompact ? '3px' : '6px',
      marginBottom: isCompact ? 1 : 3
    }}>
      {getLogLevelIcon(log.level)}
      <Text strong style={{ fontSize: isCompact ? 8 : 11 }}>
        {log.level.toUpperCase()}
      </Text>
      {log.source && (
        <Text type="secondary" style={{ fontSize: isCompact ? 8 : 10 }}>
          [{log.source}]
        </Text>
      )}
      <Text type="secondary" style={{ fontSize: isCompact ? 8 : 10 }}>
        {new Date(log.timestamp).toLocaleTimeString()}
      </Text>
    </div>
    <div style={{ 
      color: '#333', 
      fontSize: isCompact ? '10px' : '12px',
      lineHeight: isCompact ? '1.2' : '1.3',
      wordBreak: 'break-word',
      wordWrap: 'break-word',
      whiteSpace: 'pre-wrap'
    }}>
      {log.message}
    </div>
  </div>
));

export const VirtualizedLogViewer: React.FC<VirtualizedLogViewerProps> = ({
  logs,
  height,
  itemHeight,
  filterSource,
  filterLevel,
  isCompact = false
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [containerHeight, setContainerHeight] = useState(height);

  // 过滤日志
  const filteredLogs = useMemo(() => {
    return logs.filter(log => {
      if (filterSource && log.source !== filterSource) return false;
      if (filterLevel && log.level !== filterLevel) return false;
      return true;
    });
  }, [logs, filterSource, filterLevel]);

  // 计算可见范围
  const visibleRange = useMemo(() => {
    const start = Math.floor(scrollTop / itemHeight);
    const visibleCount = Math.ceil(containerHeight / itemHeight);
    const end = Math.min(start + visibleCount + 5, filteredLogs.length); // 多渲染5个减少滚动时的空白
    
    return {
      start: Math.max(0, start - 2), // 预渲染2个
      end
    };
  }, [scrollTop, containerHeight, itemHeight, filteredLogs.length]);

  // 可见的日志条目
  const visibleLogs = useMemo(() => {
    return filteredLogs.slice(visibleRange.start, visibleRange.end);
  }, [filteredLogs, visibleRange]);

  // 滚动事件处理
  const handleScroll = (e: React.UIEvent<HTMLDivElement>) => {
    setScrollTop(e.currentTarget.scrollTop);
  };

  // 监听容器高度变化
  useEffect(() => {
    if (containerRef.current) {
      const resizeObserver = new ResizeObserver((entries) => {
        setContainerHeight(entries[0].contentRect.height);
      });
      resizeObserver.observe(containerRef.current);
      return () => resizeObserver.disconnect();
    }
  }, []);

  // 自动滚动到底部
  useEffect(() => {
    if (containerRef.current) {
      const container = containerRef.current;
      const isAtBottom = container.scrollTop + container.clientHeight >= container.scrollHeight - 50;
      
      if (isAtBottom) {
        container.scrollTop = container.scrollHeight;
      }
    }
  }, [filteredLogs.length]);

  const totalHeight = filteredLogs.length * itemHeight;
  const offsetY = visibleRange.start * itemHeight;

  return (
    <div
      ref={containerRef}
      style={{
        height,
        overflowY: 'auto',
        overflowX: 'hidden',
        border: '1px solid #e8e8e8',
        borderRadius: '4px',
        backgroundColor: '#fafafa'
      }}
      onScroll={handleScroll}
    >
      <div style={{ height: totalHeight, position: 'relative' }}>
        <div style={{ transform: `translateY(${offsetY}px)` }}>
          {visibleLogs.map((log, index) => (
            <VirtualLogItem
              key={log.id}
              log={log}
              isCompact={isCompact}
              style={{
                height: itemHeight,
                position: 'relative'
              }}
            />
          ))}
        </div>
      </div>
      
      {filteredLogs.length === 0 && (
        <div style={{ 
          textAlign: 'center', 
          color: '#999', 
          padding: '40px',
          fontSize: isCompact ? '12px' : '14px'
        }}>
          暂无日志
        </div>
      )}
    </div>
  );
}; 
import { useState, useCallback, useRef, useEffect } from 'react';
import { message } from 'antd';
import { LogEntry, LogLevel, LoggerConfig, LoggerActions, LoggerState } from '../types/logger';

const defaultConfig: Required<LoggerConfig> = {
  maxLogs: 800, // 减少默认最大日志数，提升性能
  defaultSource: 'application',
  enableFloating: true,
  enableModal: true,
  autoScroll: true,
};

export const useLogger = (config?: LoggerConfig) => {
  const finalConfig = { ...defaultConfig, ...config };
  
  const [state, setState] = useState<LoggerState>({
    logs: [],
    maxLogs: finalConfig.maxLogs,
    autoScroll: finalConfig.autoScroll,
    showModal: false,
    showFloating: false,
    minimizedFloating: false,
  });

  // 定期清理过期日志
  useEffect(() => {
    const cleanupInterval = setInterval(() => {
      setState(prev => {
        const now = Date.now();
        const tenMinutesAgo = now - 10 * 60 * 1000;
        
        const cleanedLogs = prev.logs.filter(log => {
          const logTime = new Date(log.timestamp).getTime();
          // 保留重要日志和最近的日志
          return log.level === 'error' || 
                 log.level === 'warning' || 
                 log.level === 'success' ||
                 logTime > tenMinutesAgo;
        });
        
        // 只有在日志数量发生变化时才更新状态
        if (cleanedLogs.length !== prev.logs.length) {
          return { ...prev, logs: cleanedLogs };
        }
        return prev;
      });
    }, 2 * 60 * 1000); // 每2分钟清理一次
    
    return () => clearInterval(cleanupInterval);
  }, []);

  const logIdCounterRef = useRef(0);

  const addLog = useCallback((log: Omit<LogEntry, 'id' | 'timestamp'>) => {
    const newLog: LogEntry = {
      ...log,
      id: `log-${Date.now()}-${logIdCounterRef.current++}`,
      timestamp: new Date().toISOString(),
      source: log.source || finalConfig.defaultSource,
    };

    setState(prev => {
      let newLogs = [...prev.logs, newLog];
      
      // 智能清理策略
      if (newLogs.length > prev.maxLogs) {
        // 1. 分离重要日志和普通日志
        const importantLogs = newLogs.filter(log => 
          log.level === 'error' || log.level === 'warning'
        );
        const regularLogs = newLogs.filter(log => 
          log.level !== 'error' && log.level !== 'warning'
        );
        
        // 2. 保留策略：40%重要日志，60%普通日志
        const maxImportant = Math.min(importantLogs.length, Math.floor(prev.maxLogs * 0.4));
        const maxRegular = prev.maxLogs - maxImportant;
        
        const keptImportantLogs = importantLogs.slice(-maxImportant);
        const keptRegularLogs = regularLogs.slice(-maxRegular);
        
        // 3. 合并并按时间排序
        newLogs = [...keptImportantLogs, ...keptRegularLogs]
          .sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());
      }
      
      // 4. 定期清理过期的debug日志（超过10分钟）
      const now = Date.now();
      const tenMinutesAgo = now - 10 * 60 * 1000;
      
      newLogs = newLogs.filter(log => {
        const logTime = new Date(log.timestamp).getTime();
        // 保留所有错误、警告和成功日志，以及10分钟内的debug和info日志
        return log.level === 'error' || 
               log.level === 'warning' || 
               log.level === 'success' ||
               logTime > tenMinutesAgo;
      });
      
      return {
        ...prev,
        logs: newLogs,
      };
    });
  }, [finalConfig.defaultSource]);

  const clearLogs = useCallback(() => {
    setState(prev => ({
      ...prev,
      logs: [],
    }));
  }, []);

  const downloadLogs = useCallback((filename?: string) => {
    if (state.logs.length === 0) {
      message.warning('没有日志可下载');
      return;
    }

    const logContent = state.logs.map(log => 
      `[${new Date(log.timestamp).toLocaleString()}] ${log.level.toUpperCase()}${log.source ? ` [${log.source}]` : ''}: ${log.message}${log.details ? `\n详情: ${JSON.stringify(log.details, null, 2)}` : ''}`
    ).join('\n');
    
    const blob = new Blob([logContent], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename || `logs-${new Date().toISOString().split('T')[0]}.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    message.success('日志下载成功');
  }, [state.logs]);

  const setAutoScroll = useCallback((enabled: boolean) => {
    setState(prev => ({
      ...prev,
      autoScroll: enabled,
    }));
  }, []);

  const setShowModal = useCallback((show: boolean) => {
    setState(prev => ({
      ...prev,
      showModal: show,
    }));
  }, []);

  const setShowFloating = useCallback((show: boolean) => {
    setState(prev => ({
      ...prev,
      showFloating: show,
    }));
  }, []);

  const setMinimizedFloating = useCallback((minimized: boolean) => {
    setState(prev => ({
      ...prev,
      minimizedFloating: minimized,
    }));
  }, []);

  const getLogsBySource = useCallback((source: string) => {
    return state.logs.filter(log => log.source === source);
  }, [state.logs]);

  const getLogsByLevel = useCallback((level: LogLevel) => {
    return state.logs.filter(log => log.level === level);
  }, [state.logs]);

  // 便捷的日志添加方法
  const logInfo = useCallback((message: string, source?: string, details?: any) => {
    addLog({ level: 'info', message, source, details });
  }, [addLog]);

  const logWarning = useCallback((message: string, source?: string, details?: any) => {
    addLog({ level: 'warning', message, source, details });
  }, [addLog]);

  const logError = useCallback((message: string, source?: string, details?: any) => {
    addLog({ level: 'error', message, source, details });
  }, [addLog]);

  const logDebug = useCallback((message: string, source?: string, details?: any) => {
    addLog({ level: 'debug', message, source, details });
  }, [addLog]);

  const logSuccess = useCallback((message: string, source?: string, details?: any) => {
    addLog({ level: 'success', message, source, details });
  }, [addLog]);



  const actions: LoggerActions = {
    addLog,
    clearLogs,
    downloadLogs,
    setAutoScroll,
    setShowModal,
    setShowFloating,
    setMinimizedFloating,
    getLogsBySource,
    getLogsByLevel,
  };

  // 从外部任务状态批量添加日志的方法
  const addTaskLogs = useCallback((taskLogs: Array<{ timestamp: string; level: string; message: string; }>, source: string = 'task') => {
    taskLogs.forEach(log => {
      addLog({
        level: log.level as LogLevel,
        message: log.message,
        source,
      });
    });
  }, [addLog]);

  return {
    state,
    actions,
    // 便捷方法
    logInfo,
    logWarning,
    logError,
    logDebug,
    logSuccess,
    addTaskLogs,
    // 状态
    logs: state.logs,
    autoScroll: state.autoScroll,
    showModal: state.showModal,
    showFloating: state.showFloating,
    minimizedFloating: state.minimizedFloating,
  };
}; 
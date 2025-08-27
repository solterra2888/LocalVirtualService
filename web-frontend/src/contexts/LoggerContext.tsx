import React, { createContext, useContext, ReactNode } from 'react';
import { useLogger } from '../hooks/useLogger';
import { LogEntry, LogLevel } from '../types/logger';

// 定义Context类型
interface LoggerContextType {
  // 日志数据和状态
  logs: LogEntry[];
  autoScroll: boolean;
  showModal: boolean;
  showFloating: boolean;
  minimizedFloating: boolean;
  
  // 便捷方法
  logInfo: (message: string, source?: string, details?: any) => void;
  logWarning: (message: string, source?: string, details?: any) => void;
  logError: (message: string, source?: string, details?: any) => void;
  logSuccess: (message: string, source?: string, details?: any) => void;
  logDebug: (message: string, source?: string, details?: any) => void;
  addTaskLogs: (taskLogs: Array<{ timestamp: string; level: string; message: string; }>, source?: string) => void;
  
  // 操作方法
  actions: {
    addLog: (log: Omit<LogEntry, 'id' | 'timestamp'>) => void;
    clearLogs: () => void;
    downloadLogs: (filename?: string) => void;
    setAutoScroll: (enabled: boolean) => void;
    setShowModal: (show: boolean) => void;
    setShowFloating: (show: boolean) => void;
    setMinimizedFloating: (minimized: boolean) => void;
    getLogsBySource: (source: string) => LogEntry[];
    getLogsByLevel: (level: LogLevel) => LogEntry[];
  };
}

// 创建Context
const LoggerContext = createContext<LoggerContextType | undefined>(undefined);

// Provider组件
interface LoggerProviderProps {
  children: ReactNode;
}

export const LoggerProvider: React.FC<LoggerProviderProps> = ({ children }) => {
  const logger = useLogger({
    defaultSource: 'system',
    maxLogs: 300, // 进一步减少以提升性能
    autoScroll: true,
    enableFloating: true,
    enableModal: true
  });

  const contextValue: LoggerContextType = {
    // 状态
    logs: logger.logs,
    autoScroll: logger.autoScroll,
    showModal: logger.showModal,
    showFloating: logger.showFloating,
    minimizedFloating: logger.minimizedFloating,
    
    // 便捷方法
    logInfo: logger.logInfo,
    logWarning: logger.logWarning,
    logError: logger.logError,
    logSuccess: logger.logSuccess,
    logDebug: logger.logDebug,
    addTaskLogs: logger.addTaskLogs,
    
    // 操作方法
    actions: logger.actions
  };

  return (
    <LoggerContext.Provider value={contextValue}>
      {children}
    </LoggerContext.Provider>
  );
};

// Hook用于使用Context
export const useGlobalLogger = (): LoggerContextType => {
  const context = useContext(LoggerContext);
  if (context === undefined) {
    throw new Error('useGlobalLogger must be used within a LoggerProvider');
  }
  return context;
};

// 可选的Hook，允许在没有Provider的情况下返回undefined
export const useOptionalGlobalLogger = (): LoggerContextType | undefined => {
  return useContext(LoggerContext);
}; 
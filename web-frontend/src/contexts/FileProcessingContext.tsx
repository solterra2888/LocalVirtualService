import React, { createContext, useContext, useState, useCallback, useEffect, useRef, ReactNode, useMemo } from 'react';
import { filesAPI } from '../services/files';
import type { TaskStatusResponse } from '../services/files';

export interface TaskInfo {
  taskId: string;
  fileName: string;
  fileType: 'document' | 'audio';
  status: TaskStatusResponse;
  startTime: number;
}

interface FileProcessingContextType {
  // 新增：多任务管理
  tasks: Map<string, TaskInfo>;
  addTask: (taskId: string, fileName: string, fileType: 'document' | 'audio') => void;
  updateTaskStatus: (taskId: string, status: Partial<TaskStatusResponse> & { status: string; progress: number; files_processed: number; total_files: number; logs: Array<{ timestamp: string; level: string; message: string; }> }) => void;
  removeTask: (taskId: string) => void;
  clearAllTasks: () => void;
  
  /** 注册 Source 列表刷新回调（如 Files 页左侧列表），在弹窗启动任务或任务完成时会触发刷新 */
  registerSourceListRefresh: (fn: () => void) => () => void;
  
  // 兼容旧代码：返回最新任务
  currentTaskId: string | null;
  taskStatus: TaskStatusResponse | null;
  setCurrentTaskId: (id: string | null) => void;
  setTaskStatus: (status: TaskStatusResponse | null) => void;
  clearTask: () => void;
}

const FileProcessingContext = createContext<FileProcessingContextType | undefined>(undefined);

const POLL_INTERVAL_MS = 8000;
const AUTO_REMOVE_DELAY_MS = 3000;

const SOURCE_LIST_REFRESH_DELAY_MS = 1500;

export const FileProcessingProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [tasks, setTasks] = useState<Map<string, TaskInfo>>(new Map());
  const pollingRef = useRef<NodeJS.Timeout | null>(null);
  const tasksRef = useRef<Map<string, TaskInfo>>(new Map());
  const onSourceListRefreshRef = useRef<(() => void) | null>(null);

  // 同步 tasks 到 tasksRef（用于轮询，避免频繁重启 interval）
  useEffect(() => {
    tasksRef.current = tasks;
  }, [tasks]);

  const registerSourceListRefresh = useCallback((fn: () => void) => {
    onSourceListRefreshRef.current = fn;
    return () => {
      if (onSourceListRefreshRef.current === fn) {
        onSourceListRefreshRef.current = null;
      }
    };
  }, []);

  // 新增：添加任务
  const addTask = useCallback((taskId: string, fileName: string, fileType: 'document' | 'audio') => {
    setTasks(prev => {
      const newTasks = new Map(prev);
      newTasks.set(taskId, {
        taskId,
        fileName,
        fileType,
        status: {
          task_id: taskId,
          status: 'pending',
          progress: 0,
          message: 'Task pending',
          files_processed: 0,
          files_failed: 0,
          total_files: 1,
          logs: []
        } as TaskStatusResponse,
        startTime: Date.now()
      });
      return newTasks;
    });
    // 弹窗关闭后延迟刷新 Source 列表，使新任务尽快出现在列表中
    setTimeout(() => {
      onSourceListRefreshRef.current?.();
    }, SOURCE_LIST_REFRESH_DELAY_MS);
  }, []);

  // 新增：更新任务状态
  const updateTaskStatus = useCallback((
    taskId: string, 
    status: Partial<TaskStatusResponse> & { 
      status: string; 
      progress: number; 
      files_processed: number; 
      total_files: number; 
      logs: Array<{ timestamp: string; level: string; message: string; }> 
    }
  ) => {
    const isCompleted = status.status === 'completed' || status.status === 'failed';
    setTasks(prev => {
      const newTasks = new Map(prev);
      const task = newTasks.get(taskId);
      if (task) {
        newTasks.set(taskId, { ...task, status: status as TaskStatusResponse });
        
        // 如果任务完成或失败，3秒后自动移除
        if (isCompleted) {
          setTimeout(() => {
            setTasks(current => {
              const updated = new Map(current);
              updated.delete(taskId);
              return updated;
            });
          }, AUTO_REMOVE_DELAY_MS);
        }
      }
      return newTasks;
    });
    // 任务完成或失败时立即刷新 Source 列表
    if (isCompleted) {
      onSourceListRefreshRef.current?.();
    }
  }, []);

  // 新增：移除任务
  const removeTask = useCallback((taskId: string) => {
    setTasks(prev => {
      const newTasks = new Map(prev);
      newTasks.delete(taskId);
      return newTasks;
    });
  }, []);

  // 新增：清除所有任务
  const clearAllTasks = useCallback(() => {
    setTasks(new Map());
  }, []);

  // 兼容旧代码：setCurrentTaskId 现在添加任务
  const setCurrentTaskId = useCallback((id: string | null) => {
    if (id) {
      // 如果任务不存在，添加一个临时任务
      setTasks(prev => {
        if (!prev.has(id)) {
          const newTasks = new Map(prev);
          newTasks.set(id, {
            taskId: id,
            fileName: 'Processing...',
            fileType: id.startsWith('audio-') ? 'audio' : 'document',
            status: {
              task_id: id,
              status: 'pending',
              progress: 0,
              message: 'Task pending',
              files_processed: 0,
              files_failed: 0,
              total_files: 1,
              logs: []
            } as TaskStatusResponse,
            startTime: Date.now()
          });
          return newTasks;
        }
        return prev;
      });
    }
  }, []);

  // 兼容旧代码：setTaskStatus 更新最新任务的状态
  const setTaskStatus = useCallback((status: TaskStatusResponse | null) => {
    if (!status) return;
    
    setTasks(prev => {
      if (prev.size === 0) return prev;
      const sortedTasks = Array.from(prev.values()).sort((a, b) => b.startTime - a.startTime);
      const latestTaskId = sortedTasks[0].taskId;
      return new Map(prev).set(latestTaskId, {
        ...sortedTasks[0],
        status
      });
    });
  }, []);

  // 兼容旧代码：clearTask 清除最新任务
  const clearTask = useCallback(() => {
    setTasks(prev => {
      if (prev.size === 0) return prev;
      const sortedTasks = Array.from(prev.values()).sort((a, b) => b.startTime - a.startTime);
      const newTasks = new Map(prev);
      newTasks.delete(sortedTasks[0].taskId);
      return newTasks;
    });
  }, []);

  // 轮询所有任务（只在任务数量变化时重启）
  useEffect(() => {
    const taskCount = tasks.size;
    
    if (taskCount === 0) {
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
        pollingRef.current = null;
      }
      return;
    }

    // 如果已经在轮询，不要重启
    if (pollingRef.current) {
      return;
    }

    const poll = async () => {
      // 使用 tasksRef.current 而不是 tasks，避免闭包问题
      const taskEntries = Array.from(tasksRef.current.entries());
      
      for (const [taskId, task] of taskEntries) {
        // 跳过音频任务（手动更新）
        if (taskId.startsWith('audio-')) continue;
        
        // 跳过已完成/失败的任务
        if (task.status.status === 'completed' || task.status.status === 'failed') continue;

        try {
          const status = await filesAPI.getTaskStatus(taskId);
          updateTaskStatus(taskId, status);
        } catch (error) {
          console.error(`Failed to poll task ${taskId}:`, error);
        }
      }
    };

    // 立即执行一次
    poll();
    // 设置定时轮询
    pollingRef.current = setInterval(poll, POLL_INTERVAL_MS);
    
    return () => {
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
        pollingRef.current = null;
      }
    };
  }, [tasks.size, updateTaskStatus]);

  // 兼容旧代码：返回最新任务
  const latestTask = useMemo(() => {
    if (tasks.size === 0) return { taskId: null, status: null };
    const sortedTasks = Array.from(tasks.values()).sort((a, b) => b.startTime - a.startTime);
    return {
      taskId: sortedTasks[0].taskId,
      status: sortedTasks[0].status
    };
  }, [tasks]);

  const value: FileProcessingContextType = {
    tasks,
    addTask,
    updateTaskStatus,
    removeTask,
    clearAllTasks,
    registerSourceListRefresh,
    currentTaskId: latestTask.taskId,
    taskStatus: latestTask.status,
    setCurrentTaskId,
    setTaskStatus,
    clearTask,
  };

  return (
    <FileProcessingContext.Provider value={value}>
      {children}
    </FileProcessingContext.Provider>
  );
};

export function useFileProcessing(): FileProcessingContextType {
  const ctx = useContext(FileProcessingContext);
  if (ctx === undefined) {
    throw new Error('useFileProcessing must be used within FileProcessingProvider');
  }
  return ctx;
}

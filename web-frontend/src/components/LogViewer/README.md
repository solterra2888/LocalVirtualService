# 日志模块使用说明

## 概述

日志模块是一个独立的、可复用的日志系统，提供了完整的日志管理功能，包括日志记录、查看、过滤、导出等。

## 组件结构

```
components/LogViewer/
├── LogViewer.tsx          # 日志查看器组件
├── index.ts               # 导出文件
└── README.md             # 使用说明

hooks/
└── useLogger.ts          # 日志管理Hook

types/
└── logger.ts             # 日志类型定义
```

## 基本使用

### 1. 在组件中使用日志系统

```tsx
import { useLogger, LogViewer } from '../components/LogViewer';

const MyComponent = () => {
  // 初始化日志系统
  const logger = useLogger({
    defaultSource: 'my-module',  // 默认日志来源
    maxLogs: 500,               // 最大日志数量
    autoScroll: true            // 自动滚动
  });

  // 记录日志
  const handleSomeAction = () => {
    logger.logInfo('开始执行操作', 'my-module');
    
    try {
      // 执行某些操作
      logger.logSuccess('操作成功完成', 'my-module');
    } catch (error) {
      logger.logError('操作失败', 'my-module', error);
    }
  };

  return (
    <div>
      {/* 你的组件内容 */}
      
      <button onClick={() => logger.actions.setShowModal(true)}>
        查看日志
      </button>
      
      <button onClick={() => logger.actions.setShowFloating(true)}>
        显示浮动日志
      </button>
      
      {/* 日志查看器 */}
      <LogViewer
        logs={logger.logs}
        autoScroll={logger.autoScroll}
        showModal={logger.showModal}
        showFloating={logger.showFloating}
        minimizedFloating={logger.minimizedFloating}
        onAutoScrollChange={logger.actions.setAutoScroll}
        onModalClose={() => logger.actions.setShowModal(false)}
        onFloatingClose={() => logger.actions.setShowFloating(false)}
        onMinimizeToggle={() => logger.actions.setMinimizedFloating(!logger.minimizedFloating)}
        onClearLogs={logger.actions.clearLogs}
        onDownloadLogs={logger.actions.downloadLogs}
      />
    </div>
  );
};
```

### 2. 日志记录方法

```tsx
// 便捷方法
logger.logInfo('信息日志', 'module-name');
logger.logWarning('警告日志', 'module-name');
logger.logError('错误日志', 'module-name', errorObject);
logger.logSuccess('成功日志', 'module-name');
logger.logDebug('调试日志', 'module-name');

// 通用方法
logger.actions.addLog({
  level: 'info',
  message: '自定义日志',
  source: 'module-name',
  details: { key: 'value' }
});

// 批量添加外部日志
const externalLogs = [
  { timestamp: '2023-01-01T00:00:00Z', level: 'info', message: '外部日志1' },
  { timestamp: '2023-01-01T00:01:00Z', level: 'error', message: '外部日志2' }
];
logger.addTaskLogs(externalLogs, 'external-system');
```

### 3. 日志过滤

```tsx
// 按来源过滤
const moduleALogs = logger.actions.getLogsBySource('module-a');

// 按级别过滤
const errorLogs = logger.actions.getLogsByLevel('error');
```

### 4. 日志管理

```tsx
// 清空日志
logger.actions.clearLogs();

// 下载日志
logger.actions.downloadLogs('my-logs.txt');

// 控制显示
logger.actions.setShowModal(true);
logger.actions.setShowFloating(true);
logger.actions.setMinimizedFloating(false);
logger.actions.setAutoScroll(true);
```

## 配置选项

```tsx
const config = {
  maxLogs: 1000,              // 最大日志数量，超过会删除最旧的
  defaultSource: 'app',       // 默认日志来源
  enableFloating: true,       // 启用浮动窗口
  enableModal: true,          // 启用模态框
  autoScroll: true           // 自动滚动到最新日志
};

const logger = useLogger(config);
```

## 日志级别

- `info`: 一般信息日志（蓝色）
- `warning`: 警告日志（橙色）
- `error`: 错误日志（红色）
- `success`: 成功日志（绿色）
- `debug`: 调试日志（灰色）

## 特性

1. **多模块支持**: 通过 `source` 参数区分不同模块的日志
2. **多种显示模式**: 支持模态框和浮动窗口两种显示方式
3. **实时更新**: 日志实时显示，支持自动滚动
4. **过滤功能**: 支持按来源和级别过滤日志
5. **导出功能**: 支持将日志导出为文本文件
6. **内存管理**: 自动限制日志数量，防止内存溢出
7. **响应式设计**: 适配不同屏幕尺寸

## 在 FilesPage 中的应用

日志模块已经在 `FilesPage` 中实现，用于显示文件处理过程中的日志：

- 文件处理开始时自动清空日志并显示浮动窗口
- 实时同步任务状态中的日志到新的日志系统
- 支持查看历史日志和实时日志
- 提供日志下载和清空功能

这个实现可以作为在其他页面使用日志模块的参考。 
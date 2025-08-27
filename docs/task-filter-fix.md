# 任务筛选功能修复

## 问题描述

在任务执行历史页面中，任务筛选功能存在以下问题：

1. **硬编码选项**：任务筛选器中的选项是静态硬编码的值（"task-1", "task-2", "task-3"）
2. **不匹配实际数据**：这些硬编码的ID与实际数据库中的任务ID不匹配
3. **功能失效**：用户无法通过筛选器正确筛选特定任务的执行日志

## 修复方案

### 1. 动态获取任务列表
- 添加了 `loadTasks()` 函数来从后端API获取实际的任务列表
- 使用 `useEffect` 在组件加载时自动获取任务列表
- 添加了 `tasks` 状态来存储获取到的任务数据

### 2. 更新筛选器选项
- 将硬编码的选项替换为动态生成的任务选项
- 添加了"所有任务"选项，允许用户查看所有任务的日志
- 使用实际的任务ID和名称作为选项值

### 3. 改进用户体验
- 增加了筛选器的宽度以适应较长的任务名称
- 添加了加载状态指示器
- 改进了错误处理和用户反馈

## 技术实现

### 前端修改

#### 1. 导入类型
```typescript
import { schedulerAPI, ExecutionLog, ScheduledTask } from '../services/scheduler';
```

#### 2. 添加状态
```typescript
const [tasks, setTasks] = useState<ScheduledTask[]>([]);
```

#### 3. 添加任务加载函数
```typescript
const loadTasks = async () => {
  try {
    const response = await schedulerAPI.getTasks();
    if (response.success && response.data) {
      setTasks(response.data);
    } else {
      console.error('获取任务列表失败:', response.message);
      message.error('获取任务列表失败');
    }
  } catch (error) {
    console.error('获取任务列表失败:', error);
    message.error('获取任务列表失败');
  }
};
```

#### 4. 更新筛选器组件
```typescript
<Select
  placeholder="选择任务"
  style={{ width: 200 }}
  allowClear
  value={selectedTaskId}
  onChange={setSelectedTaskId}
  loading={loading}
>
  <Option value="all">所有任务</Option>
  {tasks.map((task) => (
    <Option key={task.id} value={task.id}>
      {task.name}
    </Option>
  ))}
</Select>
```

### 后端支持

后端已经提供了获取任务列表的API：
- **端点**: `GET /scheduler/tasks`
- **功能**: 返回所有定时任务的列表
- **响应**: 包含任务ID、名称、类型等信息的数组

## 修复效果

### 修复前
- 任务筛选器显示硬编码的选项
- 无法正确筛选特定任务的日志
- 用户体验差

### 修复后
- 任务筛选器显示实际的任务列表
- 可以正确筛选特定任务的执行日志
- 支持查看所有任务的日志
- 用户体验显著改善

## 测试方法

1. **启动应用**：确保前后端服务正常运行
2. **访问页面**：导航到数据源管理 -> 任务执行历史
3. **检查筛选器**：确认任务筛选器显示实际的任务名称
4. **测试筛选**：
   - 选择"所有任务"查看所有日志
   - 选择特定任务查看该任务的日志
   - 验证筛选结果是否正确
5. **测试清空**：点击清空按钮确认筛选器重置

## 注意事项

- 确保后端任务API正常工作
- 如果任务列表为空，筛选器将只显示"所有任务"选项
- 任务名称较长时，筛选器会自动调整宽度
- 加载状态会在获取任务列表时显示

## 相关文件

- `web-frontend/src/components/TaskExecutionHistory.tsx`: 主要修改文件
- `web-frontend/src/services/scheduler.ts`: 任务API服务
- `backend/api/scheduler.py`: 后端任务API
- `backend/services/scheduler_service.py`: 后端任务服务

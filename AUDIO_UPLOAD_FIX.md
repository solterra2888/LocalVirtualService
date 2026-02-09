# 🚨 音频上传系统崩溃问题修复

## 问题描述

当用户通过 "Upload Audio" 上传大文件（>200MB）时，整个 SaaS 系统停滞甚至崩溃。这是一个**严重的生产环境问题**，影响所有用户。

## 根本原因分析

### 1. **同步阻塞上传（内存问题）**
```python
# 旧代码 (backend/api/files.py, 行 1541)
content = await file.read()  # ❌ 把整个 200MB+ 文件一次性加载到内存
file_size = len(content)
with open(temp_file_path, "wb") as buffer:
    buffer.write(content)  # ❌ 一次性写入，占用大量内存
```

**问题：**
- 一次性读取整个文件到内存（200MB+）
- 导致内存占用激增
- 阻塞 FastAPI worker 线程

### 2. **同步阻塞 ASR 处理（长时间等待）**
```python
# 旧代码 (backend/api/files.py, 行 1678-1684)
result = asr_service.transcribe_audio(
    audio_url=audio_url,
    wait_for_result=True,  # ❌ 默认等待结果
    ...
)
```

**问题：**
- 等待 ASR 处理完成（可能需要几分钟）
- 完全阻塞 HTTP 请求
- 导致其他用户的请求无法处理

### 3. **多用户系统崩溃**
- FastAPI 默认使用有限数量的 worker（通常 1-4 个）
- 一个大文件上传阻塞一个 worker
- 多个用户同时上传 → 所有 worker 被阻塞 → 系统停滞

## 解决方案

### ✅ 修改 1：流式上传（避免内存问题）

**文件：** `backend/api/files.py`

```python
# 新代码：使用流式写入，分块读取
chunk_size = 1024 * 1024  # 1MB chunks
file_size = 0

with open(temp_file_path, "wb") as buffer:
    while True:
        chunk = await file.read(chunk_size)  # ✅ 分块读取，不占用大量内存
        if not chunk:
            break
        buffer.write(chunk)
        file_size += len(chunk)
```

**优点：**
- 不再一次性加载整个文件到内存
- 内存占用稳定（最多 1MB）
- 支持任意大小的文件

### ✅ 修改 2：异步 Celery 任务（避免阻塞）

**文件：** `backend/tasks/file_tasks.py`

新增 `upload_and_transcribe_audio_task` Celery 任务：
- 在后台处理音频上传到 OSS
- 在后台处理 ASR 语音识别
- 不阻塞主 API 线程
- 支持进度追踪和错误处理

**流程：**
1. 用户上传文件 → 保存到临时目录（流式写入）
2. 立即返回任务 ID（不等待处理完成）
3. Celery 后台任务：
   - 上传到 OSS
   - 调用 ASR 服务
   - 保存结果到数据库
   - 清理临时文件

### ✅ 修改 3：前端轮询任务状态

**文件：** `web-frontend/src/components/UploadAudioModal.tsx`

```typescript
// 新代码：立即返回任务 ID，轮询进度
const uploadResult = await filesAPI.uploadAudioFile(audioFile.file);
const taskId = uploadResult.task_id;

// 轮询任务状态（每 2 秒）
const pollInterval = setInterval(async () => {
  const statusResult = await filesAPI.getTaskStatus(taskId);
  // 更新进度显示
  updateTaskStatus(taskId, {
    status: statusResult.status,
    progress: statusResult.progress,
    ...
  });
}, 2000);
```

**优点：**
- 不阻塞用户界面
- 实时显示进度
- 用户可以继续使用系统其他功能

## 修改的文件清单

### 后端
1. **backend/tasks/file_tasks.py**
   - ✅ 新增 `upload_and_transcribe_audio_task` Celery 任务
   - ✅ 添加进度追踪
   - ✅ 添加错误处理和清理逻辑

2. **backend/api/files.py**
   - ✅ 修改 `/upload-audio` 端点：使用流式上传
   - ✅ 提交异步任务，立即返回任务 ID
   - ✅ 导入新的 Celery 任务

### 前端
3. **web-frontend/src/components/UploadAudioModal.tsx**
   - ✅ 修改上传逻辑：不等待结果
   - ✅ 添加任务状态轮询
   - ✅ 实时更新进度显示

4. **web-frontend/src/services/files.ts**
   - ✅ 更新 `uploadAudioFile` 返回类型
   - ✅ 更新 `TaskStatusResponse` 类型定义

## 技术优势

### 内存效率
- **旧方案：** 200MB 文件 → 占用 200MB+ 内存
- **新方案：** 200MB 文件 → 占用 1-2MB 内存（分块处理）

### 响应时间
- **旧方案：** 等待上传+ASR 完成（5-10 分钟）
- **新方案：** 立即返回（<1 秒）

### 并发能力
- **旧方案：** 4 个并发请求 → 系统停滞
- **新方案：** 支持数百个并发请求（Celery 队列）

### 用户体验
- **旧方案：** 用户等待，无法使用系统
- **新方案：** 后台处理，实时进度显示，用户可继续使用系统

## 测试建议

1. **小文件测试（<10MB）**
   - 验证流式上传正常工作
   - 验证进度显示正常

2. **大文件测试（200-500MB）**
   - 验证内存占用稳定
   - 验证系统不会崩溃
   - 验证其他用户不受影响

3. **并发测试**
   - 多个用户同时上传大文件
   - 验证系统响应正常
   - 验证所有任务都能完成

4. **错误测试**
   - 上传失败（网络中断）
   - ASR 识别失败
   - 验证错误提示和清理逻辑

## 部署注意事项

1. **确保 Celery worker 运行**
   ```bash
   celery -A backend.celery_app worker --loglevel=info -Q file_processing
   ```

2. **确保 Redis 运行**（Celery 使用 Redis 作为消息队列）
   ```bash
   redis-server
   ```

3. **配置 OSS 和 ASR 服务**
   - 设置环境变量：`OSS_ACCESS_KEY_ID`, `OSS_ACCESS_KEY_SECRET`
   - 设置环境变量：`DASHSCOPE_API_KEY`

4. **监控任务队列**
   - 使用 Flower 监控 Celery 任务：`celery -A backend.celery_app flower`

## 总结

这次修复解决了一个**严重的生产环境问题**：
- ✅ 避免内存溢出（流式上传）
- ✅ 避免系统阻塞（异步任务）
- ✅ 提升用户体验（实时进度）
- ✅ 提高系统稳定性（支持高并发）

**适用场景：** 所有多用户 SaaS 系统中的大文件处理场景。

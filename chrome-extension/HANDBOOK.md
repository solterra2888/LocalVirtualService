# WebClipper（GuanHe Web Clipper）使用手册

面向 **GuanHe** 平台的 Chrome 扩展：将当前网页**正文**剪藏为 HTML，图片在本地拉取并转为 **base64 内嵌**，再经登录鉴权上传到平台，供后续处理与归档。

---

## 功能概览

- **一键剪藏**：在弹出窗口中点击 **Capture Page**（需先登录）。
- **智能正文**：使用 Mozilla **[Readability](https://github.com/mozilla/readability)** 解析主文；失败时按选择器回退（`article`、`.post-content`、`[role=main]`、`main` 等），再不行则使用 `body`。
- **图片内嵌**：后台 Service Worker 按页面中的图片 URL 下载，转为 data URL 并写回 HTML，减少外链失效问题。
- **懒加载与常见 CDN**：在正文提取前，对当前页 `img` 尝试解析 `data-src` / `srcset` / Next.js `/_next/image` 等，再交给 Readability。
- **账号与服务器**：可配置 **Server URL**，使用平台账号 **登录**；支持 **Remember password** 与后台 **401 时自动重登**（若已保存密码）。

---

## 环境要求


| 项目   | 说明                                                  |
| ---- | --------------------------------------------------- |
| 浏览器  | 支持 Manifest V3 的 Chromium 系（Chrome、Edge 等）          |
| 后端   | 已部署的 **GuanHe** 后端，且开启扩展所用接口（见下文 [后端 API](#后端-api)） |
| 对象存储 | 服务端剪藏接口依赖 **OSS 已配置**；未配置时上传会返回服务不可用类错误             |


- **线上（GuanHe Capital）**：在扩展里将 Server URL 设为官方站点根地址，例如 `**https://guanhecapital.hk`**（与 [GuanHe Capital](https://guanhecapital.hk/) 使用的后端一致；末尾 `/` 可有可无，扩展会规范化）。
- **本地开发**：扩展默认 `http://localhost:8000`，请与本地后端监听地址一致。

---

## 安装扩展

1. 克隆或复制本目录（含 `manifest.json`、`background.js`、`popup.js`、`Readability.js`、图标等）。
2. 打开 Chrome → 地址栏输入 `chrome://extensions/`。
3. 打开右上角 **开发者模式**。
4. 点击 **加载已解压的扩展程序**，选择本扩展所在文件夹。
5. 建议将扩展**固定到工具栏**，便于点击弹出窗口。

---

## 配置与登录

### 服务器地址

1. 点击扩展图标打开弹出窗口。
2. 点击 **⚙ Server Settings** 展开。
3. 在 **Server URL** 中填写平台根地址（无尾部 `/` 亦可，扩展会规范化）。
4. 点击 **Save Server URL**。

#### 示例：对接 [GuanHe Capital](https://guanhecapital.hk/) 线上站点

本扩展设计为与 **GuanHe Capital** 平台后端联调，典型配置如下。


| 配置项            | 示例值                                                       |
| -------------- | --------------------------------------------------------- |
| **Server URL** | `https://guanhecapital.hk`                                |
| 登录接口（扩展自动调用）   | `POST https://guanhecapital.hk/api/auth/login`            |
| 剪藏上传（扩展自动调用）   | `POST https://guanhecapital.hk/api/files/upload-web-clip` |


**操作顺序示例：**

1. 在弹出窗口 **⚙ Server Settings** 中填入 `**https://guanhecapital.hk`** → **Save Server URL**。
2. 使用你在 **[https://guanhecapital.hk/](https://guanhecapital.hk/)** 上的**同一套**用户名与密码 → **Login**（成功后 **Capture Page** 可点）。
3. 在任意要保存的文章或网页标签页中，点击扩展 → **Capture Page**；剪藏内容会进入该平台账号下的处理流程（需服务端 OSS 等已就绪）。

本地调试时，把 Server URL 改回 `http://localhost:8000`（或你的开发机地址）即可，无需改代码。

### 登录

1. 填写 **Username**、**Password**。
2. **Remember password**（默认勾选）：勾选时会在本地保存密码用于 **401 自动重登**；不勾选则不在扩展存储中保留密码。
3. 点击 **Login**。成功后显示已登录用户名，**Capture Page** 变为可用。

### 登出

点击 **Logout**。若未勾选「记住密码」，登出后会清空已存用户名与密码字段。

---

## 使用步骤

1. 在目标文章或网页上打开标签页。
2. 点击扩展图标 → **Capture Page**。
3. 页面右上角会出现状态提示：先提取正文，再显示下载图片数量；完成后若成功，会显示绿色成功提示（含标题与图片统计）。
4. 若失败，会通过 **alert** 显示错误信息（例如未登录、会话过期、网络或服务端错误）。

---

## 工作原理

1. **内容脚本（注入页内）**
  - 注入 `Readability.js` 后执行 `extractContent()`。  
  - 先在一级 DOM 上尽量把懒加载图转为真实 `src`，再用 Readability 从克隆文档中抽取正文；并剔除侧栏、页脚等噪声节点（含部分站点 `data-test-id`）。  
  - 为正文补充标题与元信息（日期、作者、站点名等，来自 Readability 或 meta）。  
  - 从正文中收集所有非 `data:` 的 `img` 的 URL，去重后发给后台。
2. **后台 Service Worker（`background.js`）**
  - 使用 `fetch`（`credentials: 'include'`）下载每张图，转为 base64 data URL，在 HTML 字符串中全局替换对应 URL。  
  - 读取已保存的 `serverUrl`、`authToken`；若无 token 且存有用户名密码，则调用登录接口刷新 token。  
  - 向 `{serverUrl}/api/files/upload-web-clip` 发送 JSON（见下节）。
3. **服务端**
  - 校验用户、将完整 HTML 写入 OSS，并登记处理任务（具体以服务端实现为准）。

---

## 后端 API

扩展与下列 HTTP 接口对接（路径相对于 **Server URL** 根路径）。

### 1. 登录

`POST /api/auth/login`

**请求体（JSON）**

```json
{
  "username": "your_user",
  "password": "your_password",
  "client_type": "extension"
}
```

**响应**：需包含 `access_token`（扩展存为 `authToken`）。

---

### 2. 上传网页剪藏

`POST /api/files/upload-web-clip`

**请求头**

- `Content-Type: application/json`
- `Authorization: Bearer <access_token>`

**请求体（JSON）**


| 字段           | 类型     | 说明                                 |
| ------------ | ------ | ---------------------------------- |
| `title`      | string | 页面标题（1～500 字符）                     |
| `html`       | string | 正文 HTML，**图片建议已为 base64 data URL** |
| `source_url` | string | 原始页面 URL                           |


示例：

```json
{
  "title": "文章标题",
  "html": "<article>...</article>",
  "source_url": "https://example.com/article"
}
```

**成功响应（示例字段）**

```json
{
  "success": true,
  "file_id": 123,
  "task_id": "celery-task-id",
  "filename": "article-title_20260406_120000.html",
  "title": "文章标题",
  "source_url": "https://example.com/article",
  "file_size_mb": 1.23,
  "status": "processing"
}
```

**错误**：HTTP 4xx/5xx；401 时扩展会尝试用已存密码重登后再传一次。业务错误信息可能在响应体的 `detail` 等字段（扩展会尽量展示）。

---

## 权限说明

`manifest.json` 中声明了例如：

- `activeTab`、`scripting`、`storage`：注入脚本、读写扩展配置与登录状态。  
- `host_permissions: <all_urls>`：在任意站点抓取图片与调用自建服务器（请仅在信任的服务器上填写 Server URL）。

---

## 项目结构（扩展端）

```
chrome-extension/
├── manifest.json      # MV3 配置、权限、图标
├── background.js      # Service Worker：下图片、上传、页内提示注入函数
├── popup.html         # 弹出窗口 UI
├── popup.js           # 配置、登录、触发剪藏与 extractContent
├── Readability.js     # Mozilla Readability（正文提取）
├── icon16.png / icon48.png / icon128.png
└── HANDBOOK.md        # 本手册
```

---

## 常见问题


| 现象          | 可能原因与处理                                                                           |
| ----------- | --------------------------------------------------------------------------------- |
| 提示未登录       | 先在弹出窗口完成登录；检查 Server URL 是否正确。                                                    |
| 401 / 会话过期  | 重新登录；若曾勾选记住密码，扩展会尝试自动重登。                                                          |
| 保存失败且提示 OSS | 服务端 OSS 未配置或不可用，需运维检查后端与存储。                                                       |
| 图片数量偏少      | 站点使用特殊懒加载或防爬；可尝试在页面滚动加载完图片后再剪藏。                                                   |
| 正文不理想       | Readability 对少数站点效果一般；可在 `popup.js` 中调整 `noiseSelectors`、回退 `selectors` 顺序（开发者向）。 |


---

## 自定义与二次开发

- **界面主色**：弹出窗与页内状态条使用蓝色 `#1677ff`、成功 `#52c41a`，可在 `popup.html` / `background.js` 中搜索替换。  
- **正文策略**：修改 `popup.js` 中 `extractContent()` 与 Readability 参数（如 `charThreshold`）。  
- **与 save-to-notes 的差异**：save-to-notes 面向通用 `POST /save-blog-post` 与本地 Python 示例服务；本扩展面向 **GuanHe** 的登录与 `/api/files/upload-web-clip`，不可混用端点。

---

## 许可证与致谢

**Readability.js**：遵循其原有许可证（见文件头或 [mozilla/readability](https://github.com/mozilla/readability)）。  


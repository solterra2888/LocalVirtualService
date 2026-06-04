# 全高布局与内部滚动修复指南

本文档总结 **Sources（`/files`）** 模块中「右侧内容与侧栏底部齐平、内部可滚动、滚动条按需显示」的修复方法，供 **Feeds、Watchlist、Daily** 及其他全屏模块复用。

## 1. 问题现象与根因

### 1.1 常见现象

| 现象 | 含义 |
|------|------|
| 右侧内容超出侧栏底部 | 页面整体在滚，而不是内容区在滚 |
| 内容被裁切但滚不动 | 外层 `overflow: hidden` 锁死高度，内层未成为滚动容器 |
| 滚动条一直占位 | 使用了 `scrollbar-gutter: stable` 或常驻 `::-webkit-scrollbar-thumb` 样式 |
| 修复后编译报错 `Type 'number' is not assignable to type 'Timeout'` | 浏览器 `window.setTimeout` 与 Node `Timeout` 类型不一致 |

### 1.2 根因（三层）

```
侧栏 position:fixed，高度 = calc(100vh - 2×inset)
        │
        ▼
主内容区应同为「视口高度 − 边距」，且 overflow:hidden
        │  ← 若此处只有 min-height:100vh，子内容会把整页撑高
        ▼
页面根（如 .files-page-layout）应 flex:1; min-height:0
        │  ← 若 Ant Design <App> 未参与 flex，高度链在此断裂
        ▼
各栏/分屏再各自 overflow-y:auto
        │  ← 若滚动区子元素 flex:1 撑高，或滚动区自身 display:flex 不当，仍无法滚
        ▼
用户在该层滚动
```

**关键结论：**

1. **只允许一层「页面级」高度**（`100vh`），其余层用 **内部 `overflow-y: auto`**。
2. **`layout-content > .your-page-root` 选择器常失效**：页面外包了 `<App>`，实际 DOM 为 `layout-content > .ant-app > … > .your-page-root`。
3. **Flex 子项默认 `min-height: auto`**，会被内容撑开；需 `min-height: 0` 且优先 `flex: 1 1 0`。
4. **滚动容器内子组件不要用 `flex: 1` 占满**，否则内容高度参与 flex 计算，滚动发生在错误层级。

---

## 2. 标准 DOM 与职责划分

以侧栏布局为例（`Layout.tsx` + `LayoutSidebar.css`）：

```
.main-layout-with-sider
├── .layout-sidebar.ant-layout-sider     ← fixed，高度与视口对齐
└── .ant-layout（主栏 wrapper）           ← height:100vh; overflow:hidden
    └── .layout-content                  ← height:calc(100vh - 2×inset); flex 列
        └── .ant-app                     ← 【必须】flex:1; min-height:0; overflow:hidden
            └── .{module}-page-layout    ← flex:1; min-height:0; overflow:hidden
                └── .{module}-page-inner ← Grid 或 Flex 分栏
                    ├── 左栏（列表）
                    └── 右栏（详情）
                        └── .xxx-content   ← overflow-y:auto（唯一滚动层）
```

**每层只做一件事：**

- **外层**：定高 + `overflow: hidden`
- **中间**：`flex: 1 1 0` + `min-height: 0` 传递高度
- **最内滚动层**：`overflow-y: auto` + 块级内容（勿再套一层会撑高的 flex 子树）

---

## 3. 布局层修复（`LayoutSidebar.css`）

### 3.1 为主内容 wrapper 锁定视口高度

对需要与侧栏齐平的模块，在 `LayoutSidebar.css` 增加（将 `your-page-root-class` 换成实际类名）：

```css
/* 1) 主栏 Ant Layout：禁止整页被撑高 */
.main-layout-with-sider:has(.your-page-root-class) > .ant-layout:not(.layout-sidebar) {
  display: flex;
  flex-direction: column;
  height: 100vh;
  min-height: 100vh;
  max-height: 100vh;
  overflow: hidden;
}

/* 2) layout-content 参与 flex 并限定高度 */
.main-layout-with-sider:has(.your-page-root-class) > .ant-layout:not(.layout-sidebar) > .layout-content {
  flex: 1;
  min-height: 0;
}

.main-layout-with-sider .layout-content:has(.your-page-root-class) {
  display: flex;
  flex-direction: column;
  margin: var(--layout-content-gap) var(--layout-content-gap) var(--layout-content-gap) 0;
  min-height: calc(100vh - 2 * var(--layout-sidebar-inset));
  height: calc(100vh - 2 * var(--layout-sidebar-inset));
  overflow: hidden;
}
```

侧栏自身高度（已有）：

```css
.main-layout-with-sider > .layout-sidebar.ant-layout-sider {
  height: calc(100vh - 2 * var(--layout-sidebar-inset));
  /* top/bottom: var(--layout-sidebar-inset) */
}
```

二者相加后，**主内容区底边与侧栏底边对齐**。

### 3.2 打通 Ant Design `<App>` 包装层（必做）

许多页面结构为：

```tsx
<ConfigProvider>
  <App>
    <YourPageInner />  {/* 根节点 className="your-page-root-class" */}
  </App>
</ConfigProvider>
```

**不要只写** `.layout-content > .your-page-root-class`（直接子选择器匹配不到）。

```css
/* 直接子：.ant-app 或少数无 App 包裹时的页面根 */
.main-layout-with-sider .layout-content:has(.your-page-root-class) > .ant-app,
.main-layout-with-sider .layout-content:has(.your-page-root-class) > .your-page-root-class {
  display: flex;
  flex-direction: column;
  flex: 1 1 0;
  min-height: 0;
  width: 100%;
  overflow: hidden;
}

/* 后代：页面根 */
.main-layout-with-sider .layout-content:has(.your-page-root-class) .your-page-root-class {
  flex: 1 1 0;
  min-height: 0;
  width: 100%;
  height: 100%;
  overflow: hidden;
}
```

**参考实现：** `web-frontend/src/components/LayoutSidebar.css`（`files-page-layout`、`feed-page-layout`、`watchlist-page-layout`、`daily-page-scroll-root`）。

### 3.3 新模块接入清单

1. 页面根节点使用**稳定、唯一**的 class（如 `feed-page-layout`）。
2. 在 `LayoutSidebar.css` 中把该类加入上述 `:has(...)` 规则（可复制 Sources 一段并替换类名）。
3. 确认 `App.css` 中未再给该页 `layout-content` 加大 `margin` / `min-height` 导致双重滚动。

---

## 4. 页面内分栏（`App.css` 或模块 CSS）

### 4.1 推荐：CSS Grid 固定行高

比纯 Flex 更不易被内容撑破（Sources 已采用）：

```css
.your-page-layout {
  flex: 1 1 0;
  min-height: 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.your-page-layout .your-page-inner {
  flex: 1 1 0;
  min-height: 0;
  display: grid;
  grid-template-columns: 300px minmax(0, 1fr); /* 左固定 + 右自适应 */
  grid-template-rows: minmax(0, 1fr);           /* 关键：行高受限 */
  gap: 12px;
  padding: 10px;
  overflow: hidden;
}

.your-page-layout .your-list-panel {
  grid-column: 1;
  grid-row: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.your-page-layout .your-detail-container {
  grid-column: 2;
  grid-row: 1;
  min-height: 0;
  display: flex;
  overflow: hidden;
}
```

### 4.2 详情区分屏（左右 Original / Info）

```css
.your-detail-split {
  flex: 1 1 0;
  min-height: 0;
  display: grid;
  grid-template-columns: minmax(280px, 1fr) minmax(280px, 1fr);
  grid-template-rows: minmax(0, 1fr);
  overflow: hidden;
}

.your-detail-left,
.your-detail-right {
  min-height: 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.your-detail-left-content,
.your-detail-right-content {
  flex: 1 1 0;
  min-height: 0;
  overflow-y: auto;
  overflow-x: hidden;
  -webkit-overflow-scrolling: touch;
  /* 不要用 display:flex 包一整块可增长正文，除非子项不再 flex:1 撑高 */
}
```

### 4.3 滚动区内的 React 组件

| 做法 | 说明 |
|------|------|
| 正文容器 `width: 100%` | 由父级 `.xxx-content` 负责滚动 |
| 避免 `flex: 1; minHeight: 0` 包住长文本 | 会把 flex 子项撑到内容高度，滚动失效 |
| PDF/iframe 需要占满可视区 | 单独 class，如 `.files-original-media-fill { min-height: min(720px, calc(100vh - 200px)); }` |

**参考：** `FilesPage.tsx` 中 `OriginalContentView`、`files-original-body` 已去掉多余 `flex: 1`。

---

## 5. 弱化滚动条（按需显示）

### 5.1 样式（`web-frontend/src/styles/subtleScrollbar.css`）

- 默认：`scrollbar-width: none`，WebKit thumb 透明。
- **悬停**、**聚焦**、或带 **`.is-scrolling`** 时显示滑块。
- 变体：
  - `subtle-scrollbar subtle-scrollbar--sidebar` — 侧栏导航
  - `subtle-scrollbar subtle-scrollbar--panel` — 浅色面板
  - `subtle-scrollbar subtle-scrollbar--panel-dark` — 深色面板

在 `App.css` 顶部已 `@import './styles/subtleScrollbar.css';`。

### 5.2 侧栏导航（`Layout.tsx`）

```tsx
import { useScrollingClass } from '../hooks/useScrollingClass';
import '../styles/subtleScrollbar.css';

const sidebarNavRef = useScrollingClass<HTMLElement>();

<nav
  ref={sidebarNavRef}
  className="layout-sidebar-nav subtle-scrollbar subtle-scrollbar--sidebar"
  aria-label="Main navigation"
>
```

### 5.3 页面内多滚动区（`FilesPage.tsx` 模式）

```tsx
const panelScrollbarClass = isDark
  ? 'subtle-scrollbar subtle-scrollbar--panel-dark'
  : 'subtle-scrollbar subtle-scrollbar--panel';

<div className={`files-list-scroll ${panelScrollbarClass}`} ref={filesListScrollRef} />
<div ref={detailLeftScrollRef} className={`files-detail-left-content ${panelScrollbarClass}`} />
```

列表若已有 `scroll` 监听（如无限加载），在回调里追加 `is-scrolling`：

```tsx
const filesListScrollIdleRef = useRef<number | undefined>(undefined);

const onScroll = () => {
  const el = filesListScrollRef.current;
  if (!el) return;
  el.classList.add('is-scrolling');
  if (filesListScrollIdleRef.current !== undefined) {
    window.clearTimeout(filesListScrollIdleRef.current);
  }
  filesListScrollIdleRef.current = window.setTimeout(() => {
    el.classList.remove('is-scrolling');
  }, 700);
  // …原有逻辑
};
```

其他区域可用 **`useScrollingClass`**（`web-frontend/src/hooks/useScrollingClass.ts`）自动切换 `is-scrolling`。

### 5.4 避免与弱化滚动条冲突

- **删除** 针对同一容器的常驻 `::-webkit-scrollbar-thumb { background: #xxx }` 规则。
- **不要** 对「仅在有溢出时才滚」的容器使用 `scrollbar-gutter: stable`（无溢出时也会占位）。

---

## 6. TypeScript：定时器类型

在 CRA / 浏览器 TS 环境中，`window.setTimeout` 返回 **`number`**，不要写成 Node 的 `Timeout`。

```ts
// useScrollingClass.ts / FilesPage.tsx
let idleTimer: number | undefined;

idleTimer = window.setTimeout(() => { /* ... */ }, 700);
if (idleTimer !== undefined) {
  window.clearTimeout(idleTimer);
}
```

---

## 7. 调试步骤（其他模块照此排查）

1. **DevTools → Elements**：从 `.layout-content` 向下找，哪一层 `scrollHeight > clientHeight` 且 `overflow` 不是 `auto`？
2. **检查是否存在 `.ant-app`**：若无 flex 规则，先补 §3.2。
3. **临时给可疑层加 outline**，确认是哪一层在变高。
4. **滚动测试**：鼠标在目标区域上滚轮，看 `scrollTop` 是否变化（Console：`$0.scrollTop`）。
5. **编译**：改 hook 后跑 `npm run build` 或 dev 编译，确认无 `Timeout` 类型错误。

---

## 8. 已落地文件索引（Sources 范例）

| 文件 | 作用 |
|------|------|
| `web-frontend/src/components/LayoutSidebar.css` | 视口高度、`.ant-app` 桥接、`:has(.files-page-layout)` |
| `web-frontend/src/App.css` | `.files-page-layout` Grid、详情滚动区 |
| `web-frontend/src/pages/FilesPage.tsx` | 滚动 class、`useScrollingClass`、列表 `is-scrolling` |
| `web-frontend/src/components/Layout.tsx` | 侧栏导航弱化滚动条 |
| `web-frontend/src/styles/subtleScrollbar.css` | 滚动条显隐样式 |
| `web-frontend/src/hooks/useScrollingClass.ts` | 滚动中 class 钩子 |

**同类模块可对照：**

- Daily：`daily-page-scroll-root`（`DailyPage.tsx` + `LayoutSidebar.css`）
- Feeds：`feed-page-layout`（`FeedReaderPage` 等）
- Watchlist：`watchlist-page-layout`

新模块按 §3.3 增加根 class 并复制 Layout 规则即可。

---

## 9. 反模式速查

| 反模式 | 后果 |
|--------|------|
| 仅 `min-height: 100vh` 不设 `max-height` / `overflow:hidden` | 整页滚动，侧栏与内容底不齐 |
| `layout-content > .page-root` 忽略 `.ant-app` | 高度链断，内容区滚不动 |
| 滚动容器 `display:flex` + 子元素 `flex:1` 装长文 | 父级被撑高，无滚动条 |
| 多层 `overflow-y:auto` 嵌套 | 滚轮不知道滚哪一层 |
| `scrollbar-gutter: stable` 用于可选滚动区 | 无内容时也显示沟槽 |
| `ReturnType<typeof setTimeout>` 存 `window.setTimeout` 返回值 | TS2322 编译失败 |

---

## 10. 最小可复制模板（新模块）

**1）页面根（React）**

```tsx
return (
  <div className="foo-page-layout">
    <div className="foo-page-inner">
      <aside className="foo-list-panel">…</aside>
      <main className="foo-detail-container">
        <div className="foo-detail-content subtle-scrollbar subtle-scrollbar--panel">…</div>
      </main>
    </div>
  </div>
);
```

**2）LayoutSidebar.css** — 将 `foo-page-layout` 加入 §3.1、§3.2 的 `:has()` 列表。

**3）模块 CSS** — 按 §4 写 Grid + 单一 `overflow-y:auto` 滚动层。

**4）可选** — 滚动区 `ref={useScrollingClass()}` 或列表 scroll 里加 `is-scrolling`。

按以上四步，即可在其他模块复现 Sources 的「全高对齐 + 内部滚动 + 按需显示滚动条」行为。

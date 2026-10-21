# 查询控件规范（UI Control Style Spec）

**状态**: 生效中（2026-10 制定） · **实现**: `web-frontend/src/styles/control-style.css`（App.tsx 全局引入） · **参考实现**: Earnings Calendar 页

所有功能模块的**查询/筛选控件**（Segmented 切换组、翻页/刷新按钮、搜索框）必须遵守本规范。版式取自 earningshub.com（药丸/圆形、小号加粗、无边框），**色系对齐左侧导航栏**（渐变 `#1b4b7d → #1d5998`，激活项白底胶囊 + 蓝字）。

## 1. 设计 Token

### 色彩（对齐导航栏蓝色系）

| Token | 亮色 | 暗色 | 用途 |
|---|---|---|---|
| `--ctl-track-bg` | `rgba(29,89,152,0.12)` | `rgba(125,175,225,0.20)` | Segmented 轨道底色 |
| `--ctl-selected-bg` | `#ffffff` | `#f0f8ff` | Segmented 选中项（= 导航栏激活胶囊） |
| `--ctl-selected-text` | `#1d5998` | `#1d5998` | 选中项文字（白底蓝字） |
| `--ctl-text` | `rgba(27,75,125,0.72)` | `rgba(255,255,255,0.72)` | 未选中文字 |
| `--ctl-hover-text` | `#1b4b7d` | `#ffffff` | 未选中 hover 文字 |
| `--ctl-btn-bg` | `rgba(29,89,152,0.10)` | `rgba(125,175,225,0.14)` | 按钮/搜索框底色 |
| `--ctl-btn-bg-hover` | `rgba(29,89,152,0.18)` | `rgba(125,175,225,0.24)` | 按钮 hover 底色 |
| `--ctl-btn-text` | `#1d5998` | `#cfe1f2` | 按钮图标/文字 |
| 选中投影 | `0 2px 8px rgba(27,75,125,0.18)` | `0 2px 8px rgba(0,0,0,0.35)` | 选中项浮起感 |

核心原则：**选中态 = 白底蓝字**，与导航栏激活项是同一设计语言；一切底色都是品牌蓝的浅透明变体，**不用边框**（border: none），层级靠底色深浅。

### 形状 / 尺寸 / 字体

| Token | 值 | 用途 |
|---|---|---|
| Segmented 轨道圆角 | 10px（内层 item/thumb 8px） | 药丸感 |
| Segmented 标签 | 11px / 700（全局层为 600；查询栏内按钮/搜索框同 11px） | 紧凑加粗 |
| 图标按钮（翻页/刷新） | 32×32 正圆 | `<Button icon={...}/>` 自动识别 |
| 文字按钮（日期胶囊） | 高 32px、圆角 999px 胶囊 | |
| 搜索框 | 圆角 10px、无边框、同色浅蓝底 | |
| 控件高度基准 | 32px | 与图标按钮同高对齐 |

### 布局对齐

- 全屏模块页的查询栏卡片**顶部与导航栏顶部对齐**（页面容器 `padding: 0 24px 24px`，导航栏 fixed 于 viewport y=12px）。
- 参照 EarningsCalendarPage 容器写法：`height: calc(100vh - 24px)` 拉满内容卡片。

## 2. 使用方法（两个层级）

### 层级一：Segmented —— 全局自动生效，零改动

`control-style.css` 全局层已对全站所有 `antd Segmented` 生效（含暗色，通过 Layout 根节点 `.main-layout-dark` 自动切换）。**新模块直接用 `<Segmented>` 即符合规范，不需要写任何样式。**

### 层级二：查询栏容器加 `.app-controls` 类

查询/筛选控制栏（含按钮、搜索框的完整版式）在容器上加类：

```tsx
// ✅ 规范用法：查询栏卡片
<Card className="app-controls">
  <Segmented options={scopeOptions} value={scope} onChange={...} />
  <Button icon={<LeftOutlined />} onClick={...} />        {/* 自动变圆形浅蓝 */}
  <Button onClick={...}>Oct 20 – Oct 24</Button>          {/* 自动变胶囊 */}
  <Input.Search placeholder="Search ticker or company" /> {/* 自动同色无边框 */}
</Card>
```

暗色无需传任何标记，自动适配。

**单个按钮**（不在查询栏容器里、但需要同款样式的按钮，如面板内的"生成 Recap"）直接加 `className="app-ctl-btn"`，与 `.app-controls .ant-btn` 同一套规则（含暗色）。

### 不适用的场景（保持 antd 默认）

- 表单提交/重置按钮、Modal 的确定/取消、危险操作按钮
- 行内操作按钮（表格行内编辑/删除）
- 主行动按钮（primary 语义不变）

即：**只有"查询/筛选/视图切换"语境的控件走本规范**，操作语境按钮不动。

## 3. 实现要点（改样式时必读）

- 全局层选择器写到 3 个 class（轨道用 `html .ant-segmented` 前缀）是有意为之：antd v5 样式是**运行时注入**，静态 CSS 在同特异性下会输给后注入方，必须用更高特异性稳定压制。
- `.app-controls` 按钮的 hover 用了 `!important`：antd 的 `.ant-btn-default:hover` 会试图上主色描边，必须压掉。
- 暗色判定**不要**在各页面自行传 dark 类；统一依赖 `.main-layout-dark`（Layout.tsx 按 `themeMode` 切换，来源 `useDashboardSettings()`）。

## 4. 旧模块迁移

- **已迁移**: Earnings Calendar（`.app-controls` 参考实现）
- **自动跟随（无需改动）**: 所有使用裸 `<Segmented>` 的页面 —— Economic Events、Feeds Dashboard、Feeds Reader、Finviz Dashboard、LLM Monitoring、Daily Preview Drawer 等，已随全局层生效
- **遗留清单（自带旧作用域样式，特异性更高，暂覆盖全局层）**: Files 页（`.files-page-layout .files-list-search-segmented`）、Mobile Feeds/Sources 页、Upload Files Modal —— 这些页面自带 `!important` 蓝色系样式，视觉与新规范接近但非同一 token。改版时按本规范重写并删除旧作用域样式即可。

## 5. 待决事项

- 全局 `colorPrimary` 仍为 antd 默认 `#1890ff`（App.tsx），与导航蓝 `#1d5998` 不完全一致。若未来统一品牌色，需评估全站链接/开关/聚焦色的影响面，届时 Segmented token 不受影响（本规范用显式 CSS，不依赖 colorPrimary）。

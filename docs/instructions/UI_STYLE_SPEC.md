# 全站 UI 样式规范（UI Style Spec）

**状态**: 生效中（2026-10 制定） · **定位**: 全站 UI 控件/元素样式的统一规范入口。各模块新增或改版时一律按本文件执行；新的样式标准成熟后也录入本文件，作为以后统一各模块工具样式的依据。

当前包含两大板块：

| 板块 | 实现 | 参考实现 |
|---|---|---|
| [一、查询控件规范](#一查询控件规范)（Segmented / 按钮 / 搜索框） | `web-frontend/src/styles/control-style.css`（App.tsx 全局引入） | Earnings Calendar 页 |
| [二、标签胶囊规范（Tag Chip）](#二标签胶囊规范tag-chip) | `web-frontend/src/components/shared/aiTagChipStyles.ts` | Sources 模块（Upload Files）详情面板 Info → Tags |

---

## 一、查询控件规范

所有功能模块的**查询/筛选控件**（Segmented 切换组、翻页/刷新按钮、搜索框）必须遵守本规范。版式取自 earningshub.com（药丸/圆形、小号加粗、无边框），**色系对齐左侧导航栏**（渐变 `#1b4b7d → #1d5998`，激活项白底胶囊 + 蓝字）。

### 1.1 设计 Token

#### 色彩（对齐导航栏蓝色系）

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

#### 形状 / 尺寸 / 字体

| Token | 值 | 用途 |
|---|---|---|
| Segmented 轨道圆角 | 10px（内层 item/thumb 8px） | 药丸感 |
| Segmented 标签 | 11px / 700（全局层为 600；查询栏内按钮/搜索框同 11px） | 紧凑加粗 |
| 图标按钮（翻页/刷新） | 32×32 正圆 | `<Button icon={...}/>` 自动识别 |
| 文字按钮（日期胶囊） | 高 32px、圆角 999px 胶囊 | |
| 搜索框 | 圆角 10px、无边框、同色浅蓝底（`Input.Search` 与远程搜索 `Select` 同规则） | |
| 可换行胶囊切换组 | 高 28px、圆角 999px、11px / 600，选中 = 白底蓝字 + 浮起投影 | `.app-ctl-pills`（选项多于 Segmented 容量时） |
| 控件高度基准 | 32px | 与图标按钮同高对齐 |

#### 布局对齐

- 全屏模块页的查询栏卡片**顶部与导航栏顶部对齐**（页面容器 `padding: 0 24px 24px`，导航栏 fixed 于 viewport y=12px）。
- 参照 EarningsCalendarPage 容器写法：`height: calc(100vh - 24px)` 拉满内容卡片。

### 1.2 使用方法（两个层级）

#### 层级一：Segmented —— 全局自动生效，零改动

`control-style.css` 全局层已对全站所有 `antd Segmented` 生效（含暗色，通过 Layout 根节点 `.main-layout-dark` 自动切换）。**新模块直接用 `<Segmented>` 即符合规范，不需要写任何样式。**

#### 层级二：查询栏容器加 `.app-controls` 类

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

查询栏内的**远程搜索 `<Select showSearch>`** 同样自动获得浅蓝无边框样式（`.app-controls` 容器内生效，含 placeholder / 后缀箭头配色），无需额外处理。

#### 层级三：可换行胶囊切换组 `.app-ctl-pills`

选项较多、Segmented（不支持换行）放不下的筛选切换（如 13F 的 20 家预设机构），容器加类，选中项照常用 `type="primary"`：

```tsx
<div className="app-ctl-pills">
  <Button size="small" type={active ? 'primary' : 'default'} onClick={...}>Bridgewater</Button>
  {/* … */}
</div>
```

选中态 = 白底蓝字 + 浮起投影，与 Segmented 选中项 / 导航栏激活胶囊同一设计语言；暗色自动适配。**选项 ≤ 6 个时仍优先用 Segmented。**

#### 不适用的场景（保持 antd 默认）

- 表单提交/重置按钮、Modal 的确定/取消、危险操作按钮
- 行内操作按钮（表格行内编辑/删除）
- 主行动按钮（primary 语义不变）

即：**只有"查询/筛选/视图切换"语境的控件走本规范**，操作语境按钮不动。

### 1.3 实现要点（改样式时必读）

- 全局层选择器写到 3 个 class（轨道用 `html .ant-segmented` 前缀）是有意为之：antd v5 样式是**运行时注入**，静态 CSS 在同特异性下会输给后注入方，必须用更高特异性稳定压制。
- `.app-controls` 按钮的 hover 用了 `!important`：antd 的 `.ant-btn-default:hover` 会试图上主色描边，必须压掉。
- 暗色判定**不要**在各页面自行传 dark 类；统一依赖 `.main-layout-dark`（Layout.tsx 按 `themeMode` 切换，来源 `useDashboardSettings()`）。

### 1.4 旧模块迁移

- **已迁移**: Earnings Calendar（`.app-controls` 参考实现）、Form 13F（2026-10-22：顶栏 `.app-controls` + 预设机构 `.app-ctl-pills` 首个参考实现）、Economic Events（2026-10-22：TE 风格经济日历改版——`.app-controls` 查询栏 + `.app-ctl-pills` Impact 星级多选 + `aiTagChipStyle` 期间/口径/Today 标签，配套页面级样式 `pages/EconomicCalendar.css`，暗色走 `.main-layout-dark`）
- **自动跟随（无需改动）**: 所有使用裸 `<Segmented>` 的页面 —— Feeds Dashboard、Feeds Reader、Finviz Dashboard、LLM Monitoring、Daily Preview Drawer 等，已随全局层生效
- **遗留清单（自带旧作用域样式，特异性更高，暂覆盖全局层）**: Files 页（`.files-page-layout .files-list-search-segmented`）、Mobile Feeds/Sources 页、Upload Files Modal —— 这些页面自带 `!important` 蓝色系样式，视觉与新规范接近但非同一 token。改版时按本规范重写并删除旧作用域样式即可。

### 1.5 待决事项

- 全局 `colorPrimary` 仍为 antd 默认 `#1890ff`（App.tsx），与导航蓝 `#1d5998` 不完全一致。若未来统一品牌色，需评估全站链接/开关/聚焦色的影响面，届时 Segmented token 不受影响（本规范用显式 CSS，不依赖 colorPrimary）。

---

## 二、标签胶囊规范（Tag Chip）

**实现**: `web-frontend/src/components/shared/aiTagChipStyles.ts` · **参考实现**: Sources 模块（Upload Files）详情面板 Info → Tags 区块 · **已接入**: Sources、Feeds、Daily、Reports ticker 标签、Task Execution History、Database Table Viewer、Earnings Calendar（2026-10-21）

全站所有**文字标签**（AI 打标内容、状态徽标、币种/期间标记、"+N" 溢出计数等）一律使用本标准。**不允许在业务组件里自写胶囊样式或硬编码标签颜色。**

### 2.1 设计 Token

| Token | 值 | 说明 |
|---|---|---|
| 字号 / 行高 | 9px / 12px | 全站标签统一小字号 |
| 内边距 | 2px 8px | |
| 圆角 | 9999px | 全胶囊 |
| 字重 | 500 | |
| 边框 | 无 | antd Tag 必须加 `bordered={false}` |
| 外边距 | 0 | 覆盖 antd Tag 默认 `marginInlineEnd: 8px` |

### 2.2 语义色板（variant × 明暗主题）

| variant | 亮色 bg / fg | 暗色 bg / fg | 语义用途 |
|---|---|---|---|
| `ticker` | `#e3f2fd` / `#1565c0` | `rgba(33,150,243,0.22)` / `#90caf9` | 股票代码、期间（Q3'26）等主信息标签 |
| `sector` | `#e8f5e9` / `#1b5e20` | `rgba(76,175,80,0.24)` / `#a5d6a7` | 行业 / 板块 |
| `concept` | `#f3e5f5` / `#6a1b9a` | `rgba(156,39,176,0.24)` / `#ce93d8` | 概念 |
| `country` | `#fff3e0` / `#e65100` | `rgba(255,152,0,0.22)` / `#ffcc80` | 国家 / 币种 / 地区 |
| `person` | `#e8eaf6` / `#283593` | `rgba(63,81,181,0.28)` / `#9fa8da` | 人物 |
| `theme` | `#e0f7fa` / `#00695c` | `rgba(0,150,136,0.22)` / `#80cbc4` | 投资主题 |
| `signal-bullish` | `#e8f5e9` / `#2e7d32` | `rgba(76,175,80,0.28)` / `#a5d6a7` | 看涨 / beat |
| `signal-bearish` | `#fce4ec` / `#c62828` | `rgba(244,67,54,0.28)` / `#ef9a9a` | 看跌 / miss |
| `signal-neutral` | `#f5f5f5` / `#616161` | `rgba(255,255,255,0.08)` / `#9e9e9e` | 中性强调（Today 等状态高亮） |
| `signal-meta` | `#fafafa` / `#757575` | `rgba(255,255,255,0.06)` / `#757575` | 元信息 / 去强调（AI、cached、Pending） |
| `overflow` | `rgba(0,0,0,0.06)` / `rgba(0,0,0,0.72)` | `rgba(255,255,255,0.1)` / `rgba(255,255,255,0.78)` | "+N" 溢出计数 |

### 2.3 使用方法

```tsx
import { aiTagChipStyle } from '../shared/aiTagChipStyles';

const tagMode = isDark ? 'dark' : 'light'; // isDark 来自 useDashboardSettings() 或组件 props

// antd Tag 场景（不要叠 antd color 属性——内联样式已含 bg/fg）
<Tag bordered={false} style={aiTagChipStyle('ticker', tagMode)}>AAPL</Tag>

// 纯 span 场景（如图表标题旁的币种标记）
<span style={aiTagChipStyle('country', tagMode)}>HKD</span>
```

- 语义色场景一律 `aiTagChipStyle(variant, mode)`；确需 antd 预设色时只叠基础形态：`<Tag color="blue" bordered={false} style={AI_TAG_CHIP_BASE}>`。
- 一行多个标签的容器用 `<Space size={[4, 4]} wrap>`（Sources Tags 参考实现写法）。
- 需要**新语义色**时，在 `aiTagChipStyles.ts` 的 `PALETTE` 里新增 variant 并同步更新本表，不要在调用点写死颜色。
- 旧入口 `feed/feedTagChipStyles.ts` 已 deprecated（仅转发），新代码一律从 `shared/aiTagChipStyles` 引入。

### 2.4 实现要点

- 明暗适配靠调用方**显式传 `themeMode` 参数**（内联样式无法用 CSS 作用域切换，与查询控件规范的 `.main-layout-dark` 机制不同）。
- `AI_TAG_CHIP_BASE` 只含形态（字号/圆角/边距/无边框），不含颜色，可安全与 antd `color` 叠用；`aiTagChipStyle()` = BASE + 语义色，两者不要重复叠加。

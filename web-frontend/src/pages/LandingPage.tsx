import React from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { useAuthStore } from '../store/authStore';
import { useScrollReveal, useScrolled } from '../hooks/useScrollReveal';
import './LandingPage.css';

/* ===================== 数据 ===================== */

const SOURCES = [
  'Twitter', 'YouTube', 'Bloomberg', 'CNBC', 'Finviz', 'EODHD',
  'Reuters', '微博', '知识星球', 'Apple Podcasts',
];

const THESIS = [
  {
    num: '01',
    title: '跨源聚合',
    en: 'Unified Aggregation',
    desc: '订阅 Feed、上传研报、全球行情汇聚到同一条时间线，告别在十几个终端间反复切换。',
  },
  {
    num: '02',
    title: 'AI 深度分析',
    en: 'AI-Native Analysis',
    desc: '自动打标、摘要、命名、归类与向量语义检索——让每一份资料可被一句话提问、一秒钟召回。',
  },
  {
    num: '03',
    title: '全流程闭环',
    en: 'End-to-End Workflow',
    desc: '从每日信息中枢、标的研究终端，到 AI 助手对外输出，投资决策链路一站贯通。',
  },
];

interface ModuleInfo {
  no: string;
  en: string;
  cn: string;
  lead: string;
  features: { b: string; s: string }[];
  visual: 'daily' | 'feeds' | 'upload' | 'watchlist' | 'mcp';
}

const MODULES: ModuleInfo[] = [
  {
    no: '01',
    en: 'Daily',
    cn: '每日信息中枢',
    lead: 'One timeline for everything — uploads and subscriptions merged into a single stream. Replace two hours across platforms with ten minutes.',
    features: [
      { b: '跨源统一时间线', s: '9+ sources unified into one time-ordered stream' },
      { b: 'AI 摘要与智能标签', s: 'Summary, GICS sector, tickers, people & countries' },
      { b: '就地预览抽屉', s: 'In-place preview, keyboard navigation' },
      { b: '结构化每日日报', s: 'Bloomberg · CNBC · Finviz market recap' },
    ],
    visual: 'daily',
  },
  {
    no: '02',
    en: 'Feeds',
    cn: '多渠道智能订阅',
    lead: 'Your follows, continuously synced. Bring scattered sources into one inbox — never miss a signal again.',
    features: [
      { b: '多源订阅', s: 'Twitter · YouTube · News · 微博 · 知识星球 · Podcasts' },
      { b: '定时自动抓取', s: 'Auto-fetch with graceful backoff' },
      { b: '跨用户内容复用', s: 'Cross-user dedup, globally shared tagging' },
      { b: 'AI 打标 + 智能日报', s: 'Auto-tagging + two-phase daily digest' },
    ],
    visual: 'feeds',
  },
  {
    no: '03',
    en: 'Upload Sources',
    cn: '私人投研资料库',
    lead: 'Drop anything in. Reports, earnings, audio, video, web — auto-named, auto-tagged, auto-analyzed the moment it lands.',
    features: [
      { b: '多格式一键上传', s: '10+ formats, content auto-extracted' },
      { b: '四路并行 AI 自动化', s: 'Name · tag · classify · analyze in parallel' },
      { b: '向量语义检索 RAG', s: 'Semantic search across million-word docs' },
      { b: '交易报告结构化', s: 'IB statements parsed & visualized' },
    ],
    visual: 'upload',
  },
  {
    no: '04',
    en: 'Watchlist',
    cn: '自选股研究终端',
    lead: 'Every ticker, one terminal. Quotes, charts, fundamentals and earnings — automatically aggregated onto each stock you follow.',
    features: [
      { b: '全球多市场自选', s: 'US · HK · A-shares in one list' },
      { b: '实时行情 + 多周期 K 线', s: 'Realtime + 10-year EOD history' },
      { b: '深度基本面 17 大类', s: 'Statements · valuation · holders · insider' },
      { b: '财报日历 + 资讯聚合', s: 'Earnings calendar + related research' },
    ],
    visual: 'watchlist',
  },
  {
    no: '05',
    en: 'Agent MCP',
    cn: '把平台能力装进你的 AI 助手',
    lead: 'Your research library, inside your AI. Query the entire platform from Cursor or Claude Desktop through the open MCP protocol.',
    features: [
      { b: '什么是 MCP', s: 'The universal socket connecting AI to your data' },
      { b: '多角度并行检索', s: 'Multi-angle parallel search with dedup' },
      { b: '语义 + 混合向量检索', s: 'Semantic + hybrid RRF search' },
      { b: '报告随取随用', s: 'Reports via public share links' },
    ],
    visual: 'mcp',
  },
];

const AI_CARDS = [
  {
    title: '统一 AI 框架',
    en: 'Unified AI Framework',
    desc: '系统级与用户级 Jinja2 模板、多用户 provider / model 配置与限流，兼顾全局一致性与个性化。',
    icon: 'framework',
  },
  {
    title: '向量 RAG 语义检索',
    en: 'Vector RAG Search',
    desc: '长文档切块向量化入库，按"意思"而非字面检索，支持概念级提问与章节级精确定位。',
    icon: 'rag',
  },
  {
    title: '智能每日日报',
    en: 'Intelligent Daily Reports',
    desc: '两阶段生成、跨用户复用、时区对齐，以可控成本输出个性化的市场复盘与主题报告。',
    icon: 'report',
  },
];

const DAY = [
  {
    time: '06:30',
    label: 'Morning · 早间',
    text: 'Open Daily, preset to Twitter + News — grasp overnight headlines and KOL views in five minutes.',
  },
  {
    time: '09:30',
    label: 'Intraday · 盘中',
    text: 'Search any ticker or name, surface every related item across sources, and read in place without losing context.',
  },
  {
    time: '16:00',
    label: 'After-Hours · 盘后',
    text: 'Daily Market Report — narrative, bull/bear, quant panel and macro catalysts across four tabs.',
  },
  {
    time: 'Deep',
    label: 'Research · 深度研究',
    text: 'Upload a report → ask inside Cursor via MCP → vector search pinpoints the exact section you need.',
  },
];

/* ===================== SVG 装饰 ===================== */

const HeroChart: React.FC = () => (
  <svg viewBox="0 0 480 440" fill="none" aria-hidden="true">
    <defs>
      <linearGradient id="ghGold" x1="0" y1="0" x2="1" y2="1">
        <stop offset="0%" stopColor="#E3C884" />
        <stop offset="100%" stopColor="#C9A961" />
      </linearGradient>
      <linearGradient id="ghArea" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor="#C9A961" stopOpacity="0.3" />
        <stop offset="100%" stopColor="#C9A961" stopOpacity="0" />
      </linearGradient>
    </defs>
    {[90, 170, 250, 330].map((y) => (
      <line key={y} x1="30" y1={y} x2="460" y2={y} stroke="rgba(255,255,255,0.05)" />
    ))}
    <path
      d="M40,310 L100,280 L165,295 L225,235 L285,250 L345,175 L405,150 L445,105 L445,400 L40,400 Z"
      fill="url(#ghArea)"
    />
    <path
      d="M40,310 L100,280 L165,295 L225,235 L285,250 L345,175 L405,150 L445,105"
      stroke="url(#ghGold)"
      strokeWidth="2.5"
      fill="none"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
    {[
      [40, 310], [165, 295], [285, 250], [405, 150], [445, 105],
    ].map(([x, y], i) => (
      <circle key={i} cx={x} cy={y} r="4" fill="#0F1B2D" stroke="#C9A961" strokeWidth="2" />
    ))}
    {/* 蜡烛 */}
    {[
      { x: 100, yo: 295, yc: 270 }, { x: 225, yo: 250, yc: 220 },
      { x: 345, yo: 190, yc: 160 }, { x: 445, yo: 120, yc: 95 },
    ].map((c, i) => (
      <g key={i}>
        <line x1={c.x} y1={c.yc - 14} x2={c.x} y2={c.yo + 14} stroke="#C9A961" strokeWidth="1" opacity="0.4" />
        <rect x={c.x - 6} y={c.yc} width="12" height={c.yo - c.yc} fill="#C9A961" opacity="0.55" rx="1" />
      </g>
    ))}
    {/* 悬浮标签 */}
    <g transform="translate(296,46)">
      <rect width="140" height="48" rx="6" fill="rgba(10,19,34,0.9)" stroke="rgba(201,169,97,0.4)" />
      <text x="14" y="20" fill="#8A94A6" fontSize="10" letterSpacing="2">DAILY MARKET REPORT</text>
      <text x="14" y="38" fill="#F3EFE5" fontSize="14" fontWeight="600">The Close +1.84%</text>
    </g>
  </svg>
);

const ModuleVisual: React.FC<{ type: ModuleInfo['visual'] }> = ({ type }) => {
  if (type === 'daily') {
    return (
      <svg viewBox="0 0 320 280" fill="none" aria-hidden="true">
        {[40, 110, 180].map((y, i) => (
          <g key={i}>
            <circle cx="22" cy={y + 8} r="5" fill="#C9A961" />
            <rect x="40" y={y} width="260" height="56" rx="6" fill="rgba(255,255,255,0.04)" stroke="rgba(255,255,255,0.08)" />
            <rect x="54" y={y + 12} width="120" height="8" rx="2" fill="rgba(201,169,97,0.7)" />
            <rect x="54" y={y + 28} width="200" height="6" rx="2" fill="rgba(255,255,255,0.12)" />
            <rect x="54" y={y + 40} width="160" height="6" rx="2" fill="rgba(255,255,255,0.08)" />
            <rect x="270" y={y + 10} width="20" height="14" rx="3" fill="rgba(201,169,97,0.18)" />
          </g>
        ))}
        <line x1="22" y1="48" x2="22" y2="188" stroke="rgba(201,169,97,0.3)" strokeDasharray="2 4" />
      </svg>
    );
  }
  if (type === 'feeds') {
    return (
      <svg viewBox="0 0 320 280" fill="none" aria-hidden="true">
        {[
          { y: 40, label: 'X' }, { y: 110, label: '▶' }, { y: 180, label: '闻' }, { y: 250, label: '博' },
        ].map((s, i) => (
          <g key={i}>
            <circle cx="40" cy={s.y} r="16" fill="rgba(255,255,255,0.05)" stroke="rgba(201,169,97,0.45)" />
            <text x="40" y={s.y + 5} textAnchor="middle" fill="#C9A961" fontSize="12">{s.label}</text>
            <path d={`M56,${s.y} C150,${s.y} 170,140 250,140`} stroke="rgba(201,169,97,0.3)" fill="none" strokeWidth="1.2" />
          </g>
        ))}
        <rect x="232" y="108" width="64" height="64" rx="8" fill="rgba(201,169,97,0.12)" stroke="#C9A961" strokeWidth="1.5" />
        <text x="264" y="144" textAnchor="middle" fill="#E3C884" fontSize="13" fontWeight="700">Daily</text>
      </svg>
    );
  }
  if (type === 'upload') {
    return (
      <svg viewBox="0 0 320 280" fill="none" aria-hidden="true">
        {[
          { x: 60, y: 70 }, { x: 90, y: 100 }, { x: 120, y: 130 },
        ].map((d, i) => (
          <g key={i}>
            <rect x={d.x} y={d.y} width="150" height="110" rx="6" fill="rgba(255,255,255,0.04)" stroke="rgba(255,255,255,0.1)" />
            <rect x={d.x + 16} y={d.y + 16} width="80" height="6" rx="2" fill="rgba(201,169,97,0.6)" />
            <rect x={d.x + 16} y={d.y + 30} width="118" height="4" rx="2" fill="rgba(255,255,255,0.1)" />
            <rect x={d.x + 16} y={d.y + 42} width="100" height="4" rx="2" fill="rgba(255,255,255,0.08)" />
          </g>
        ))}
        <rect x="200" y="60" width="70" height="22" rx="11" fill="rgba(201,169,97,0.18)" stroke="rgba(201,169,97,0.5)" />
        <text x="235" y="75" textAnchor="middle" fill="#E3C884" fontSize="11" fontWeight="600">AI · RAG</text>
      </svg>
    );
  }
  if (type === 'watchlist') {
    return (
      <svg viewBox="0 0 320 280" fill="none" aria-hidden="true">
        {[60, 110, 160, 210].map((y) => (
          <line key={y} x1="20" y1={y} x2="300" y2={y} stroke="rgba(255,255,255,0.05)" />
        ))}
        {[
          { x: 40, yo: 150, yc: 130 }, { x: 80, yo: 140, yc: 115 }, { x: 120, yo: 125, yc: 100 },
          { x: 160, yo: 110, yc: 85 }, { x: 200, yo: 95, yc: 70 }, { x: 240, yo: 80, yc: 55 },
        ].map((c, i) => (
          <g key={i}>
            <line x1={c.x} y1={c.yc - 12} x2={c.x} y2={c.yo + 12} stroke="#C9A961" strokeWidth="1" opacity="0.45" />
            <rect x={c.x - 8} y={c.yc} width="16" height={c.yo - c.yc} fill="#C9A961" opacity="0.6" rx="1.5" />
          </g>
        ))}
        <path d="M30,160 L90,140 L150,115 L210,90 L270,60" stroke="#4A90E2" strokeWidth="2" fill="none" opacity="0.8" />
        <g transform="translate(220,30)">
          <rect width="76" height="26" rx="4" fill="rgba(10,19,34,0.9)" stroke="rgba(201,169,97,0.4)" />
          <text x="38" y="18" textAnchor="middle" fill="#E3C884" fontSize="12" fontWeight="600">AAPL ▲2.4%</text>
        </g>
      </svg>
    );
  }
  // mcp
  const nodes = [
    { x: 160, y: 140, main: true }, { x: 50, y: 60 }, { x: 270, y: 60 },
    { x: 50, y: 220 }, { x: 270, y: 220 },
  ];
  return (
    <svg viewBox="0 0 320 280" fill="none" aria-hidden="true">
      {nodes.filter((n) => !n.main).map((n, i) => (
        <line key={i} x1="160" y1="140" x2={n.x} y2={n.y} stroke="rgba(201,169,97,0.35)" strokeWidth="1.2" />
      ))}
      {nodes.map((n, i) =>
        n.main ? (
          <g key={i}>
            <circle cx={n.x} cy={n.y} r="34" fill="rgba(201,169,97,0.14)" stroke="#C9A961" strokeWidth="1.5" />
            <text x={n.x} y={n.y + 4} textAnchor="middle" fill="#E3C884" fontSize="13" fontWeight="700">MCP</text>
          </g>
        ) : (
          <g key={i}>
            <circle cx={n.x} cy={n.y} r="18" fill="rgba(255,255,255,0.05)" stroke="rgba(201,169,97,0.45)" />
            <circle cx={n.x} cy={n.y} r="5" fill="#C9A961" />
          </g>
        )
      )}
    </svg>
  );
};

const AiIcon: React.FC<{ name: string }> = ({ name }) => {
  const common = { width: 22, height: 22, viewBox: '0 0 24 24', fill: 'none' } as const;
  if (name === 'framework')
    return (
      <svg {...common}>
        <path d="M4 7h16M4 12h10M4 17h16" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
        <circle cx="18" cy="12" r="2.4" stroke="currentColor" strokeWidth="2" />
      </svg>
    );
  if (name === 'rag')
    return (
      <svg {...common}>
        <circle cx="11" cy="11" r="6" stroke="currentColor" strokeWidth="2" />
        <path d="M20 20l-4-4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
      </svg>
    );
  return (
    <svg {...common}>
      <rect x="5" y="4" width="14" height="17" rx="2" stroke="currentColor" strokeWidth="2" />
      <path d="M8 9h8M8 13h8M8 17h4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
    </svg>
  );
};

/* ===================== 主组件 ===================== */

const LandingPage: React.FC = () => {
  const navigate = useNavigate();
  const { isAuthenticated } = useAuthStore();
  const scrolled = useScrolled(20);

  useScrollReveal();

  const goTo = (id: string) => {
    const el = document.getElementById(id);
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    else window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  const primaryGo = () => navigate(isAuthenticated ? '/daily' : '/login');
  const primaryLabel = isAuthenticated ? '进入工作台  Enter Platform' : '登录  Login';

  return (
    <div className="landing">
      {/* ---------- Nav ---------- */}
      <nav className={`gh-nav${scrolled ? ' scrolled' : ''}`}>
        <div className="gh-nav-inner">
          <a href="#top" className="gh-brand">
            <span className="gh-brand-mark">观</span>
            <span className="gh-brand-text">
              <span className="gh-brand-cn">观合资本</span>
              <span className="gh-brand-en">GuanHe Capital</span>
            </span>
          </a>
          <div className="gh-nav-links">
            <a href="#platforms">平台 Platforms</a>
            <a href="#capabilities">能力 Capabilities</a>
            <a href="#workflow">流程 Workflow</a>
          </div>
          <button className="gh-btn gh-btn-primary gh-btn-sm" onClick={primaryGo}>
            {primaryLabel}
          </button>
        </div>
      </nav>

      {/* ---------- Hero ---------- */}
      <header className="gh-hero" id="top">
        <div className="gh-container gh-hero-inner">
          <div className="reveal">
            <span className="gh-eyebrow">AI-Native Investment Research</span>
            <h1 className="gh-hero-title">
              Invest with <span className="accent">Intelligence.</span>
            </h1>
            <div className="gh-hero-title-cn">AI 驱动的二级市场投资研究平台</div>
            <p className="gh-hero-sub">
              聚合订阅源、研报与全球行情，AI 自动打标、摘要与语义检索——让信息过载的时代，投资决策更高效、更深度、更智能。
            </p>
            <div className="gh-hero-ctas">
              <button className="gh-btn gh-btn-primary" onClick={primaryGo}>{primaryLabel}</button>
              <button className="gh-btn gh-btn-ghost" onClick={() => goTo('platforms')}>
                了解更多 Explore
              </button>
            </div>
          </div>
          <div className="gh-hero-visual reveal" style={{ transitionDelay: '120ms' }}>
            <HeroChart />
          </div>
        </div>
        <div className="gh-hero-scroll">
          <span>SCROLL</span>
          <span className="line" />
        </div>
      </header>

      {/* ---------- 数据源条 ---------- */}
      <div className="gh-strip">
        <div className="gh-container">
          <div className="gh-strip-label">Aggregated Data Sources · 覆盖数据源</div>
          <div className="gh-strip-row">
            {SOURCES.map((s) => (
              <span className="gh-strip-item" key={s}>
                <span className="dot" />
                {s}
              </span>
            ))}
          </div>
        </div>
      </div>

      {/* ---------- 品牌主张 ---------- */}
      <section className="gh-section gh-thesis">
        <div className="gh-container">
          <div className="reveal">
            <span className="gh-eyebrow">Why GuanHe</span>
            <p className="gh-thesis-lead">
              We bring <span className="gold">every source</span>, every report and every quote into one
              AI-powered workspace — so the signal rises above the noise, and research becomes decisions.
            </p>
          </div>
          <div className="gh-thesis-grid">
            {THESIS.map((t, i) => (
              <div className="gh-thesis-card reveal" key={t.num} style={{ transitionDelay: `${i * 100}ms` }}>
                <div className="gh-thesis-num">{t.num}</div>
                <h3>{t.title}</h3>
                <p style={{ color: 'var(--gh-text-3)', fontSize: 12, letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 10 }}>
                  {t.en}
                </p>
                <p>{t.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ---------- 五大模块 ---------- */}
      <section className="gh-section" id="platforms">
        <div className="gh-container">
          <div className="reveal" style={{ marginBottom: 24 }}>
            <span className="gh-eyebrow">The Platform</span>
            <h2 className="gh-section-title">Five modules, one workspace.</h2>
            <div className="gh-section-title-cn">五大模块构成完整投研闭环</div>
          </div>
          {MODULES.map((m) => (
            <div className="gh-module" key={m.no}>
              <div className="gh-module-text reveal">
                <div className="gh-module-no">MODULE / {m.no}</div>
                <h3>{m.en}</h3>
                <div className="cn">{m.cn}</div>
                <p className="lead">{m.lead}</p>
                <ul className="gh-module-features">
                  {m.features.map((f) => (
                    <li key={f.b}>
                      <span><b>{f.b}</b> · {f.s}</span>
                    </li>
                  ))}
                </ul>
              </div>
              <div className="gh-module-visual reveal" style={{ transitionDelay: '100ms' }}>
                <ModuleVisual type={m.visual} />
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* ---------- AI 能力 ---------- */}
      <section className="gh-section gh-ai" id="capabilities">
        <div className="gh-container">
          <div className="gh-ai-head reveal">
            <span className="gh-eyebrow">AI Capabilities</span>
            <h2 className="gh-section-title">An AI engine, built for research.</h2>
            <div className="gh-section-title-cn">为投研而生的 AI 引擎</div>
          </div>
          <div className="gh-ai-grid">
            {AI_CARDS.map((c, i) => (
              <div className="gh-ai-card reveal" key={c.title} style={{ transitionDelay: `${i * 100}ms` }}>
                <div className="gh-ai-icon">
                  <AiIcon name={c.icon} />
                </div>
                <h4>{c.title}</h4>
                <p style={{ color: 'var(--gh-text-3)', fontSize: 12, letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 10 }}>
                  {c.en}
                </p>
                <p>{c.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ---------- 投资者的一天 ---------- */}
      <section className="gh-section" id="workflow">
        <div className="gh-container">
          <div className="reveal" style={{ marginBottom: 8 }}>
            <span className="gh-eyebrow">A Day with GuanHe</span>
            <h2 className="gh-section-title">From morning scan to deep research.</h2>
            <div className="gh-section-title-cn">投资者的完整一天</div>
          </div>
          <div className="gh-day-timeline">
            {DAY.map((d, i) => (
              <div className="gh-day-node reveal" key={d.time} style={{ transitionDelay: `${i * 100}ms` }}>
                <div className="gh-day-dot" />
                <div className="gh-day-time">{d.time}</div>
                <div className="gh-day-label">{d.label}</div>
                <div className="gh-day-card">{d.text}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ---------- CTA ---------- */}
      <section className="gh-section gh-cta-wrap">
        <div className="gh-container">
          <div className="gh-cta-card reveal">
            <h2>Start your AI investment desk.</h2>
            <div className="cn">开始你的 AI 投研工作台</div>
            <p>
              Aggregate every source, sediment every report, and let AI do the reading — turning information
              overload into decisive action.
            </p>
            <button className="gh-btn gh-btn-primary" onClick={primaryGo}>{primaryLabel}</button>
          </div>
        </div>
      </section>

      {/* ---------- Footer ---------- */}
      <footer className="gh-footer">
        <div className="gh-container">
          <div className="gh-footer-grid">
            <div className="gh-footer-brand">
              <a href="#top" className="gh-brand">
                <span className="gh-brand-mark">观</span>
                <span className="gh-brand-text">
                  <span className="gh-brand-cn">观合资本</span>
                  <span className="gh-brand-en">GuanHe Capital</span>
                </span>
              </a>
              <p className="gh-footer-tag">
                AI 驱动的二级市场投资研究平台。聚合、沉淀、分析、决策——一个属于你的投研工作台。
              </p>
              <div className="gh-footer-slogan">Do the right things. Do things right.</div>
            </div>
            <div className="gh-footer-col">
              <h5>平台 Platforms</h5>
              <a href="#platforms">Daily</a>
              <a href="#platforms">Feeds</a>
              <a href="#platforms">Upload Sources</a>
              <a href="#platforms">Watchlist</a>
              <a href="#platforms">Agent MCP</a>
            </div>
            <div className="gh-footer-col">
              <h5>能力 Capabilities</h5>
              <a href="#capabilities">AI 框架</a>
              <a href="#capabilities">向量检索</a>
              <a href="#capabilities">每日日报</a>
              <a href="#capabilities">市场复盘</a>
            </div>
            <div className="gh-footer-col">
              <h5>公司 Company</h5>
              <Link to="/login">登录 Login</Link>
              <a href="https://guanhecapital.hk" rel="noreferrer">guanhecapital.hk</a>
            </div>
          </div>
          <div className="gh-footer-bottom">
            <span>© 2026 GuanHe Capital · 观合资本</span>
            <span>Hong Kong · 二级市场投资研究平台</span>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default LandingPage;

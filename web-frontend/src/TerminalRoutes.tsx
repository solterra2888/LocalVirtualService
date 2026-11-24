import React, { Suspense, useEffect } from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { ConfigProvider, theme } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import { useAuthStore } from './store/authStore';
import { useDeviceDetection } from './hooks/useDeviceDetection';
import { lazyWithRetry } from './utils/lazyWithRetry';
import { DashboardSettingsProvider } from './contexts/DashboardSettingsContext';
import { FileProcessingProvider } from './contexts/FileProcessingContext';
import './App.css';
import './styles/control-style.css';

/* =========================================================================
   Terminal 应用分包（2026-11-16 大陆跨境链路优化）。
   官网落地页（/，LandingPage）留在主包直达；从这里开始才是登录页 + 终端
   全部页面 + antd 主题/上下文/布局——访客不进终端就永远不下载这一坨。
   所有页面经 lazyWithRetry 懒加载（发版后旧标签页 chunk 404 自愈），
   分包产物 immutable 长缓存，进过一次终端后二次进入零下载。
   ========================================================================= */

// 布局
const Layout = lazyWithRetry(() => import('./components/Layout'));
const MobileLayout = lazyWithRetry(() => import('./components/MobileLayout'));

// 登录
const LoginPage = lazyWithRetry(() => import('./pages/LoginPage'));

// 看板
const DashboardPage = lazyWithRetry(() => import('./pages/DashboardPage'));
const MobileDashboardPage = lazyWithRetry(() => import('./pages/MobileDashboardPage'));

// 市场与宏观
const MarketsPage = lazyWithRetry(() => import('./pages/MarketsPage'));
const EconomicEventsPage = lazyWithRetry(() => import('./pages/EconomicEventsPage'));
const EarningsCalendarPage = lazyWithRetry(() => import('./pages/EarningsCalendarPage'));
const MacroIndicatorsPage = lazyWithRetry(() => import('./pages/MacroIndicatorsPage'));
const CentralBankDashboardPage = lazyWithRetry(() => import('./pages/CentralBankDashboardPage'));
const Form13FPage = lazyWithRetry(() => import('./pages/Form13FPage'));
const MobileForm13FPage = lazyWithRetry(() => import('./pages/MobileForm13FPage'));
const FundamentalsPage = lazyWithRetry(() => import('./pages/FundamentalsPage'));
const EODFetchPage = lazyWithRetry(() => import('./pages/EODFetchPage'));
const CandlestickDemoPage = lazyWithRetry(() => import('./pages/CandlestickDemoPage'));

// 文件
const FilesPage = lazyWithRetry(() => import('./pages/FilesPage'));
const MobileSourcesPage = lazyWithRetry(() => import('./pages/MobileSourcesPage'));

// 订阅信息
const FeedReaderPage = lazyWithRetry(() => import('./pages/FeedReaderPage'));
const MobileFeedsPage = lazyWithRetry(() => import('./pages/MobileFeedsPage'));
const MobileFeedsListView = lazyWithRetry(() => import('./pages/MobileFeedsListView'));
const MobileFeedArticlePage = lazyWithRetry(() => import('./pages/MobileFeedArticlePage'));

// 报告
const ReportsDetailLayout = lazyWithRetry(() => import('./components/ReportsDetailLayout'));
const ReportsOverviewPage = lazyWithRetry(() => import('./pages/ReportsOverviewPage'));
const DailyMarketReportPage = lazyWithRetry(() => import('./pages/DailyMarketReportPage'));
const CloseBriefReportPage = lazyWithRetry(() => import('./pages/CloseBriefReportPage'));
const ClosingBellReportPage = lazyWithRetry(() => import('./pages/ClosingBellReportPage'));
const MorningBriefReportPage = lazyWithRetry(() => import('./pages/MorningBriefReportPage'));
const SurveillanceReportPage = lazyWithRetry(() => import('./pages/SurveillanceReportPage'));
const ThemeReportPage = lazyWithRetry(() => import('./pages/ThemeReportPage'));
const TwitterDailyReportPage = lazyWithRetry(() => import('./pages/TwitterDailyReportPage'));
const YouTubeDailyReportPage = lazyWithRetry(() => import('./pages/YouTubeDailyReportPage'));
const MobileReportsOverviewPage = lazyWithRetry(() => import('./pages/MobileReportsOverviewPage'));
const MobileReportsDetailLayout = lazyWithRetry(() => import('./components/MobileReportsDetailLayout'));
// 暂时下线 Daily 聚合视图页面 (代码保留, 恢复时取消注释并还原 /daily 路由):
// import DailyPage from './pages/DailyPage';
// import MobileDailyPage from './pages/MobileDailyPage';

// 主题 / 项目 / 设置
const InvestmentThemesPage = lazyWithRetry(() => import('./pages/InvestmentThemesPage'));
const ProjectsPage = lazyWithRetry(() => import('./pages/ProjectsPage'));
const ProjectDetailPage = lazyWithRetry(() => import('./pages/ProjectDetailPage'));
const DashboardSettingsPage = lazyWithRetry(() => import('./pages/DashboardSettingsPage'));
const ApiKeysPage = lazyWithRetry(() => import('./pages/ApiKeysPage'));
const AgentIntegrationPage = lazyWithRetry(() => import('./pages/AgentIntegrationPage'));
const PromptTemplatesPage = lazyWithRetry(() => import('./pages/PromptTemplatesPage'));

// 管理
const UsersPage = lazyWithRetry(() => import('./pages/UsersPage'));
const SchedulerPage = lazyWithRetry(() => import('./pages/SchedulerPage'));
const MobileSchedulerPage = lazyWithRetry(() => import('./pages/MobileSchedulerPage'));
const MonitoringPage = lazyWithRetry(() => import('./pages/MonitoringPage'));
const LLMConfigPage = lazyWithRetry(() => import('./pages/LLMConfigPage'));
const ZsxqAdminPage = lazyWithRetry(() => import('./pages/ZsxqAdminPage'));
const SubstackAdminPage = lazyWithRetry(() => import('./pages/SubstackAdminPage'));

/* 私有路由组件 */
const PrivateRoute: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { isAuthenticated, isInitialized } = useAuthStore();

  // 如果认证状态还没有初始化完成，显示加载状态
  if (!isInitialized) {
    return (
      <div style={{
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        height: '100vh'
      }}>
        <div>Initializing...</div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />;
  }

  return <>{children}</>;
};

/* 管理员路由组件 */
const AdminRoute: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { isAuthenticated: adminIsAuthenticated, user: adminUser, isInitialized: adminIsInitialized } = useAuthStore();

  // 如果认证状态还没有初始化完成，显示加载状态
  if (!adminIsInitialized) {
    return (
      <div style={{
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        height: '100vh'
      }}>
        <div>Initializing...</div>
      </div>
    );
  }

  if (!adminIsAuthenticated) {
    return <Navigate to="/login" replace />;
  }

  if (!adminUser?.is_admin) {
    return <Navigate to="/feeds" replace />;
  }

  return <>{children}</>;
};

/* 全屏加载态：分包 chunk 下载时的过渡（无 antd 依赖，纯内联样式） */
const ChunkLoading: React.FC = () => (
  <div style={{
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    height: '100vh',
    color: '#5d7086',
    fontFamily: 'system-ui, -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif',
    fontSize: 14,
    letterSpacing: '0.08em',
  }}>
    Loading…
  </div>
);

/* 内部组件，用于自动选择布局 */
const TerminalContent: React.FC = () => {
  const { isMobile } = useDeviceDetection();
  const LayoutComponent = isMobile ? MobileLayout : Layout;
  const DashboardComponent = isMobile ? MobileDashboardPage : DashboardPage;
  const FilesComponent = isMobile ? MobileSourcesPage : FilesPage;
  const SchedulerComponent = isMobile ? MobileSchedulerPage : SchedulerPage;
  const Form13fComponent = isMobile ? MobileForm13FPage : Form13FPage;
  // const DailyComponent = isMobile ? MobileDailyPage : DailyPage;  // 暂时下线 Daily

  return (
    <div className="App">
      <Suspense fallback={<ChunkLoading />}>
        <Routes>
          {/* 登录页面 */}
          <Route path="/login" element={<LoginPage />} />

          {/* 私有路由 */}
          {/* 私有路由（pathless layout route：子路由相对路径不变，/daily /files /reports 等顶级路径零破坏） */}
          <Route
            element={
              <PrivateRoute>
                <LayoutComponent />
              </PrivateRoute>
            }
          >
            {/* Daily 聚合视图暂时下线 (桌面+手机): 路由重定向到订阅阅读器, 页面代码保留
                (见 Layout.tsx 侧栏 / MobileBottomNav 同口径屏蔽; 恢复时还原本路由与上方 import) */}
            <Route path="daily" element={<Navigate to="/feeds" replace />} />
            <Route path="dashboard" element={<DashboardComponent />} />
            <Route path="markets" element={<MarketsPage />} />
            <Route path="markets/earnings-calendar" element={<EarningsCalendarPage />} />
            <Route path="markets/economic-events" element={<EconomicEventsPage />} />
            <Route path="markets/macro-indicators" element={<MacroIndicatorsPage />} />
            <Route path="central-bank" element={<CentralBankDashboardPage />} />
            <Route path="central-bank/:bank" element={<Navigate to="/central-bank" replace />} />
            <Route path="form13f" element={<Form13fComponent />} />
            <Route path="files" element={<FilesComponent />} />
            <Route path="fundamentals" element={<FundamentalsPage />} />
            <Route path="eod-fetch" element={<EODFetchPage />} />
            <Route
              path="scheduler"
              element={
                <AdminRoute>
                  <SchedulerComponent />
                </AdminRoute>
              }
            />
            <Route path="scheduler/logs" element={<Navigate to="/monitoring?tab=scheduler" replace />} />
            {isMobile ? (
              <Route path="feeds" element={<MobileFeedsPage />}>
                <Route index element={<MobileFeedsListView />} />
                <Route path="article/:subscriptionId/:contentId" element={<MobileFeedArticlePage />} />
              </Route>
            ) : (
              <Route path="feeds" element={<FeedReaderPage />} />
            )}
            {/* Feeds Dashboard 已下线: 旧路径重定向到订阅阅读器 */}
            <Route path="feeds-dashboard" element={<Navigate to="/feeds" replace />} />
            {/* Reports 路由 */}
            {isMobile ? (
              <>
                {/* 手机端 Reports 概览入口页（对齐桌面版卡片概览） */}
                <Route path="reports" element={<MobileReportsOverviewPage />} />
                {/* 手机端具体报告页面（复用桌面报告页，用 MobileReportsDetailLayout 提供返回按钮） */}
                <Route
                  path="reports/twitter"
                  element={
                    <MobileReportsDetailLayout>
                      <TwitterDailyReportPage />
                    </MobileReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/youtube"
                  element={
                    <MobileReportsDetailLayout>
                      <YouTubeDailyReportPage />
                    </MobileReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/theme"
                  element={
                    <MobileReportsDetailLayout>
                      <ThemeReportPage />
                    </MobileReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/daily-market"
                  element={
                    <MobileReportsDetailLayout>
                      <DailyMarketReportPage />
                    </MobileReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/close-brief"
                  element={
                    <MobileReportsDetailLayout>
                      <CloseBriefReportPage />
                    </MobileReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/closing-bell"
                  element={
                    <MobileReportsDetailLayout>
                      <ClosingBellReportPage />
                    </MobileReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/morning-brief"
                  element={
                    <MobileReportsDetailLayout>
                      <MorningBriefReportPage />
                    </MobileReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/surveillance"
                  element={
                    <MobileReportsDetailLayout>
                      <SurveillanceReportPage />
                    </MobileReportsDetailLayout>
                  }
                />
              </>
            ) : (
              <>
                {/* 桌面端 Reports 概览页面 */}
                <Route path="reports" element={<ReportsOverviewPage />} />
                {/* 桌面端具体报告页面（使用 ReportsDetailLayout 提供返回按钮） */}
                <Route
                  path="reports/twitter"
                  element={
                    <ReportsDetailLayout>
                      <TwitterDailyReportPage />
                    </ReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/youtube"
                  element={
                    <ReportsDetailLayout>
                      <YouTubeDailyReportPage />
                    </ReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/theme"
                  element={
                    <ReportsDetailLayout>
                      <ThemeReportPage />
                    </ReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/daily-market"
                  element={
                    <ReportsDetailLayout>
                      <DailyMarketReportPage />
                    </ReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/close-brief"
                  element={
                    <ReportsDetailLayout>
                      <CloseBriefReportPage />
                    </ReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/closing-bell"
                  element={
                    <ReportsDetailLayout>
                      <ClosingBellReportPage />
                    </ReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/morning-brief"
                  element={
                    <ReportsDetailLayout>
                      <MorningBriefReportPage />
                    </ReportsDetailLayout>
                  }
                />
                <Route
                  path="reports/surveillance"
                  element={
                    <ReportsDetailLayout>
                      <SurveillanceReportPage />
                    </ReportsDetailLayout>
                  }
                />
              </>
            )}
            {/* Backward-compatible redirects for old report URLs */}
            <Route path="twitter-daily-report" element={<Navigate to="/reports/twitter" replace />} />
            <Route path="youtube-daily-report" element={<Navigate to="/reports/youtube" replace />} />
            <Route path="prompt-templates" element={<PromptTemplatesPage />} />
            <Route path="investment-themes" element={<InvestmentThemesPage />} />
            <Route path="projects" element={<ProjectsPage />} />
            <Route path="projects/:projectId" element={<ProjectDetailPage />} />
            <Route path="candlestick-demo" element={<CandlestickDemoPage />} />
            <Route path="settings/dashboard" element={<DashboardSettingsPage />} />
            <Route path="settings/api-keys" element={<ApiKeysPage />} />
            <Route path="agent-integration" element={<AgentIntegrationPage />} />

            {/* 管理员路由 */}
            <Route
              path="users"
              element={
                <AdminRoute>
                  <UsersPage />
                </AdminRoute>
              }
            />
            <Route
              path="llm-config"
              element={
                <AdminRoute>
                  <LLMConfigPage />
                </AdminRoute>
              }
            />
            <Route path="system-logs" element={<Navigate to="/monitoring" replace />} />
            <Route
              path="monitoring"
              element={
                <AdminRoute>
                  <MonitoringPage />
                </AdminRoute>
              }
            />
            <Route
              path="zsxq-admin"
              element={
                <AdminRoute>
                  <ZsxqAdminPage />
                </AdminRoute>
              }
            />
            <Route
              path="substack-admin"
              element={
                <AdminRoute>
                  <SubstackAdminPage />
                </AdminRoute>
              }
            />
          </Route>

          {/* 404：跳回公共介绍页 */}
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </Suspense>
    </div>
  );
};

/**
 * Terminal 应用入口（含登录页）：antd 主题 + 全局上下文 + 全部受保护路由。
 * 由 App.tsx 以 React.lazy 挂载在 "/" 之外的 catch-all 上——
 * 本组件及其依赖（antd、布局、全部页面）全部落入异步分包，官网访客零下载。
 */
const TerminalRoutes: React.FC = () => {
  // 浏览器标签页标题：进入终端（含登录页）即「GuanHe Terminal」，
  // 与官网（/ 落地页，Home | GuanHe Capital）区分开
  useEffect(() => {
    document.title = 'GuanHe Terminal';
  }, []);

  return (
    <ConfigProvider
      locale={zhCN}
      theme={{
        algorithm: theme.defaultAlgorithm,
        token: {
          colorPrimary: '#1890ff',
          borderRadius: 6,
        },
      }}
    >
      <DashboardSettingsProvider>
        <FileProcessingProvider>
          <TerminalContent />
        </FileProcessingProvider>
      </DashboardSettingsProvider>
    </ConfigProvider>
  );
};

export default TerminalRoutes;

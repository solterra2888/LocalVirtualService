import React, { useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { ConfigProvider, theme } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import { useAuthStore } from './store/authStore';
import { useDeviceDetection } from './hooks/useDeviceDetection';
import Layout from './components/Layout';
import MobileLayout from './components/MobileLayout';
import LoginPage from './pages/LoginPage';
import DashboardPage from './pages/DashboardPage';
import MobileDashboardPage from './pages/MobileDashboardPage';
import MarketsPage from './pages/MarketsPage';
import EconomicEventsPage from './pages/EconomicEventsPage';
import MacroIndicatorsPage from './pages/MacroIndicatorsPage';
import FilesPage from './pages/FilesPage';
import MobileSourcesPage from './pages/MobileSourcesPage';
import DailyPage from './pages/DailyPage';
import MobileDailyPage from './pages/MobileDailyPage';
import UsersPage from './pages/UsersPage';
import SchedulerPage from './pages/SchedulerPage';
import MonitoringPage from './pages/MonitoringPage';
import FeedReaderPage from './pages/FeedReaderPage';
import MobileFeedsPage from './pages/MobileFeedsPage';
import MobileFeedsListView from './pages/MobileFeedsListView';
import MobileFeedArticlePage from './pages/MobileFeedArticlePage';
import FundamentalsPage from './pages/FundamentalsPage';
import EODFetchPage from './pages/EODFetchPage';
import LLMConfigPage from './pages/LLMConfigPage';
import PromptTemplatesPage from './pages/PromptTemplatesPage';
import InvestmentThemesPage from './pages/InvestmentThemesPage';
import CandlestickDemoPage from './pages/CandlestickDemoPage';
import MarketWeekly from './pages/MarketWeekly';
import MarketWeeklyReportPage from './pages/MarketWeeklyReportPage';
import ThemeReportPage from './pages/ThemeReportPage';
import ProjectsPage from './pages/ProjectsPage';
import ProjectDetailPage from './pages/ProjectDetailPage';
import DashboardSettingsPage from './pages/DashboardSettingsPage';
import ApiKeysPage from './pages/ApiKeysPage';
import AgentIntegrationPage from './pages/AgentIntegrationPage';
import TwitterDailyReportPage from './pages/TwitterDailyReportPage';
import YouTubeDailyReportPage from './pages/YouTubeDailyReportPage';
import ReportsDetailLayout from './components/ReportsDetailLayout';
import ReportsOverviewPage from './pages/ReportsOverviewPage';
import DailyMarketReportPage from './pages/DailyMarketReportPage';
import MobileReportsOverviewPage from './pages/MobileReportsOverviewPage';
import MobileReportsDetailLayout from './components/MobileReportsDetailLayout';
import ZsxqAdminPage from './pages/ZsxqAdminPage';
import { DashboardSettingsProvider } from './contexts/DashboardSettingsContext';
import { FileProcessingProvider } from './contexts/FileProcessingContext';
import './App.css';

// 私有路由组件
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

// 管理员路由组件
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
    return <Navigate to="/daily" replace />;
  }
  
  return <>{children}</>;
};

// 内部组件，用于自动选择布局
const AppContent: React.FC = () => {
  const { isMobile } = useDeviceDetection();
  const LayoutComponent = isMobile ? MobileLayout : Layout;
  const DashboardComponent = isMobile ? MobileDashboardPage : DashboardPage;
  const FilesComponent = isMobile ? MobileSourcesPage : FilesPage;
  const DailyComponent = isMobile ? MobileDailyPage : DailyPage;

  return (
    <div className="App">
      <Routes>
        {/* 登录页面 */}
        <Route path="/login" element={<LoginPage />} />
        
        {/* 私有路由 */}
        <Route
          path="/"
          element={
            <PrivateRoute>
              <LayoutComponent />
            </PrivateRoute>
          }
        >
          {/* Daily 聚合视图 = 登录后首屏: 把 Upload Files 与 Feeds 订阅按时间轴合并 */}
          <Route index element={<Navigate to="/daily" replace />} />
          <Route path="daily" element={<DailyComponent />} />
          <Route path="dashboard" element={<DashboardComponent />} />
          <Route path="markets" element={<MarketsPage />} />
          <Route path="markets/economic-events" element={<EconomicEventsPage />} />
          <Route path="markets/macro-indicators" element={<MacroIndicatorsPage />} />
          <Route path="files" element={<FilesComponent />} />
          <Route path="fundamentals" element={<FundamentalsPage />} />
          <Route path="eod-fetch" element={<EODFetchPage />} />
          <Route
            path="scheduler"
            element={
              <AdminRoute>
                <SchedulerPage />
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
                path="reports/market-weekly"
                element={
                  <MobileReportsDetailLayout>
                    <MarketWeeklyReportPage />
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
                path="reports/market-weekly"
                element={
                  <ReportsDetailLayout>
                    <MarketWeeklyReportPage />
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
            </>
          )}
          {/* Backward-compatible redirects for old report URLs */}
          <Route path="twitter-daily-report" element={<Navigate to="/reports/twitter" replace />} />
          <Route path="youtube-daily-report" element={<Navigate to="/reports/youtube" replace />} />
          <Route path="market-weekly-report" element={<Navigate to="/reports/market-weekly" replace />} />
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
        </Route>
        
        {/* 404页面 */}
        <Route path="*" element={<Navigate to="/daily" replace />} />
      </Routes>
    </div>
  );
};

const App: React.FC = () => {
  const { initializeAuth } = useAuthStore();

  useEffect(() => {
    // 应用启动时初始化认证状态
    initializeAuth().catch(console.error);
  }, [initializeAuth]);

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
      <Router>
        <DashboardSettingsProvider>
          <FileProcessingProvider>
            <AppContent />
          </FileProcessingProvider>
        </DashboardSettingsProvider>
      </Router>
    </ConfigProvider>
  );
};

export default App; 
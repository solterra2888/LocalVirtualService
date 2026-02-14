import React, { useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { ConfigProvider, theme } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import { useAuthStore } from './store/authStore';
import { LoggerProvider } from './contexts/LoggerContext';
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
import TasksPage from './pages/TasksPage';
import UsersPage from './pages/UsersPage';
import NewsPage from './pages/NewsPage';
import SchedulerPage from './pages/SchedulerPage';
import SchedulerLogsPage from './pages/SchedulerLogsPage';
import SystemLogsPage from './pages/SystemLogsPage';
import FundamentalsPage from './pages/FundamentalsPage';
import EODFetchPage from './pages/EODFetchPage';
import LLMConfigPage from './pages/LLMConfigPage';
import PromptTemplatesPage from './pages/PromptTemplatesPage';
import InvestmentThemesPage from './pages/InvestmentThemesPage';
import CandlestickDemoPage from './pages/CandlestickDemoPage';
import MarketWeekly from './pages/MarketWeekly';
import ProjectsPage from './pages/ProjectsPage';
import ProjectDetailPage from './pages/ProjectDetailPage';
import DashboardSettingsPage from './pages/DashboardSettingsPage';
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
    return <Navigate to="/dashboard" replace />;
  }
  
  return <>{children}</>;
};

// 内部组件，用于自动选择布局
const AppContent: React.FC = () => {
  const { isMobile } = useDeviceDetection();
  const LayoutComponent = isMobile ? MobileLayout : Layout;
  const DashboardComponent = isMobile ? MobileDashboardPage : DashboardPage;

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
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="market-weekly" element={<MarketWeekly />} />
          <Route path="dashboard" element={<DashboardComponent />} />
          <Route path="markets" element={<MarketsPage />} />
          <Route path="markets/economic-events" element={<EconomicEventsPage />} />
          <Route path="markets/macro-indicators" element={<MacroIndicatorsPage />} />
          <Route path="files" element={<FilesPage />} />
          <Route path="tasks" element={<TasksPage />} />
          <Route path="news" element={<NewsPage />} />
          <Route path="fundamentals" element={<FundamentalsPage />} />
          <Route path="eod-fetch" element={<EODFetchPage />} />
          <Route path="scheduler" element={<SchedulerPage />} />
          <Route path="scheduler/logs" element={<SchedulerLogsPage />} />
          <Route path="prompt-templates" element={<PromptTemplatesPage />} />
          <Route path="investment-themes" element={<InvestmentThemesPage />} />
          <Route path="projects" element={<ProjectsPage />} />
          <Route path="projects/:projectId" element={<ProjectDetailPage />} />
          <Route path="candlestick-demo" element={<CandlestickDemoPage />} />
          <Route path="settings/dashboard" element={<DashboardSettingsPage />} />
          
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
          <Route
            path="system-logs"
            element={
              <AdminRoute>
                <SystemLogsPage />
              </AdminRoute>
            }
          />
        </Route>
        
        {/* 404页面 */}
        <Route path="*" element={<Navigate to="/dashboard" replace />} />
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
      <LoggerProvider>
        <Router>
          <DashboardSettingsProvider>
            <FileProcessingProvider>
              <AppContent />
            </FileProcessingProvider>
          </DashboardSettingsProvider>
        </Router>
      </LoggerProvider>
    </ConfigProvider>
  );
};

export default App; 
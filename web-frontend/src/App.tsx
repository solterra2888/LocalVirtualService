import React, { useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { ConfigProvider, theme } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import { useAuthStore } from './store/authStore';
import { LoggerProvider } from './contexts/LoggerContext';
import Layout from './components/Layout';
import LoginPage from './pages/LoginPage';
import DashboardPage from './pages/DashboardPage';
import MarketsPage from './pages/MarketsPage';
import EconomicEventsPage from './pages/EconomicEventsPage';
import MacroIndicatorsPage from './pages/MacroIndicatorsPage';
import FilesPage from './pages/FilesPage';
import TasksPage from './pages/TasksPage';
import UsersPage from './pages/UsersPage';
import DiagnosticPage from './pages/DiagnosticPage';
import NewsPage from './pages/NewsPage';
import SchedulerPage from './pages/SchedulerPage';
import CompaniesPage from './pages/CompaniesPage';
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
          <div className="App">
            <Routes>
              {/* 登录页面 */}
              <Route path="/login" element={<LoginPage />} />
              
              {/* 私有路由 */}
              <Route
                path="/"
                element={
                  <PrivateRoute>
                    <Layout />
                  </PrivateRoute>
                }
              >
                <Route index element={<Navigate to="/dashboard" replace />} />
                <Route path="dashboard" element={<DashboardPage />} />
                <Route path="markets" element={<MarketsPage />} />
                <Route path="markets/economic-events" element={<EconomicEventsPage />} />
                <Route path="markets/macro-indicators" element={<MacroIndicatorsPage />} />
                <Route path="files" element={<FilesPage />} />
                <Route path="tasks" element={<TasksPage />} />
                <Route path="news" element={<NewsPage />} />
                <Route path="fundamentals/companies" element={<CompaniesPage />} />
                <Route path="scheduler" element={<SchedulerPage />} />
                <Route path="diagnostic" element={<DiagnosticPage />} />
                
                {/* 管理员路由 */}
                <Route
                  path="users"
                  element={
                    <AdminRoute>
                      <UsersPage />
                    </AdminRoute>
                  }
                />
              </Route>
              
              {/* 404页面 */}
              <Route path="*" element={<Navigate to="/dashboard" replace />} />
            </Routes>
          </div>
        </Router>
      </LoggerProvider>
    </ConfigProvider>
  );
};

export default App; 
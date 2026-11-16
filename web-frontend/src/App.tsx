import React, { Suspense, lazy, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { useAuthStore } from './store/authStore';
import LandingPage from './pages/LandingPage';
import { lazyWithRetry } from './utils/lazyWithRetry';

/* =========================================================================
   应用壳（2026-11-16 大陆跨境链路优化：主/分包拆分）
   主包只保留官网落地页（/）及其依赖——react/router/authStore/落地页样式，
   不含 antd、不含任何终端页面；访客在大陆跨境弱网下首屏下载从 1.77MB gzip
   降到 ~100KB 级。
   其余全部路径（/login 与登录后的整个 Terminal）走 TerminalRoutes 异步分包：
   首次进入按需下载（immutable 长缓存，之后零下载），chunk 404 由
   lazyWithRetry 整页刷新自愈（发版换哈希后旧标签页不再白屏）。
   ========================================================================= */

// 终端分包（登录页 + 全部终端页面 + antd 主题/上下文/布局）
const TerminalRoutes = lazyWithRetry(() => import('./TerminalRoutes'));

/* 分包下载过渡页：无 antd 依赖的极简加载态（官网同款蓝色系） */
const Splash: React.FC = () => (
  <div style={{
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    height: '100vh',
    background: '#02243c',
    color: '#b9c7d6',
    fontFamily: 'system-ui, -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif',
    fontSize: 14,
    letterSpacing: '0.08em',
  }}>
    Loading…
  </div>
);

const App: React.FC = () => {
  const { initializeAuth } = useAuthStore();

  useEffect(() => {
    // 应用启动时初始化认证状态
    initializeAuth().catch(console.error);
  }, [initializeAuth]);

  return (
    <Router>
      <Suspense fallback={<Splash />}>
        <Routes>
          {/* 公共介绍页（根域名，无需登录）：主包直达 */}
          <Route path="/" element={<LandingPage />} />
          {/* 其余全部路径（/login + 整个终端）：懒加载终端分包 */}
          <Route path="*" element={<TerminalRoutes />} />
        </Routes>
      </Suspense>
    </Router>
  );
};

export default App;

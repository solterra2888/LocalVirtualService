import React from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import {
  ApiOutlined,
  FileTextOutlined,
  ProfileOutlined,
  ReadOutlined,
  StockOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import './MobileBottomNav.css';

/**
 * 移动端底部导航栏：Daily（登录默认页, 聚合视图）、Sources、Feeds、Reports
 * Watchlist 与 PC 顶栏一致，指向 /dashboard
 */
const MobileBottomNav: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();

  const navItems = [
    {
      key: '/daily',
      icon: <ThunderboltOutlined />,
      label: 'Daily',
    },
    {
      key: '/files',
      icon: <FileTextOutlined />,
      label: 'Sources',
    },
    {
      key: '/feeds',
      icon: <ReadOutlined />,
      label: 'Feeds',
    },
    {
      key: '/reports',
      icon: <ProfileOutlined />,
      label: 'Reports',
    },
    {
      key: '/agent-integration',
      icon: <ApiOutlined />,
      label: 'Agent',
    },
    {
      key: '/dashboard',
      icon: <StockOutlined />,
      label: 'Watchlist',
    },
  ];

  const isActive = (path: string) =>
    location.pathname === path || location.pathname.startsWith(`${path}/`);

  return (
    <div className="mobile-bottom-nav">
      {navItems.map(item => (
        <div
          key={item.key}
          className={`nav-item ${isActive(item.key) ? 'active' : ''}`}
          onClick={() => navigate(item.key)}
        >
          <div className="nav-icon">{item.icon}</div>
          <div className="nav-label">{item.label}</div>
        </div>
      ))}
    </div>
  );
};

export default MobileBottomNav;

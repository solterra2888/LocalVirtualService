import React from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { FileTextOutlined, ProfileOutlined, ReadOutlined, StockOutlined, ThunderboltOutlined } from '@ant-design/icons';
import './MobileBottomNav.css';

/**
 * 移动端底部导航栏：Daily（登录默认页, 聚合视图）、Sources、Feeds、Reports
 * Watchlist 入口与 PC 一致暂时隐藏
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
      key: '/dashboard',
      icon: <StockOutlined />,
      label: 'Watchlist',
    },
  ].filter(item => item.key !== '/dashboard'); // 与 PC NavBar 一致暂时隐藏（未成熟）

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

import React from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import {
  ApiOutlined,
  FileTextOutlined,
  ProfileOutlined,
  ReadOutlined,
  StockOutlined,
} from '@ant-design/icons';
import './MobileBottomNav.css';

/**
 * 移动端底部导航栏：Feeds（登录默认页）、Sources、Reports
 * Watchlist 与 PC 顶栏一致，指向 /dashboard
 * Daily 暂时从导航隐藏 (使用人数不多; 路由保留), 与桌面侧栏口径一致
 */
const MobileBottomNav: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();

  const navItems = [
    {
      key: '/feeds',
      icon: <ReadOutlined />,
      label: 'Feeds',
    },
    {
      key: '/files',
      icon: <FileTextOutlined />,
      label: 'Sources',
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

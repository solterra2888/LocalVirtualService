import React from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { StockOutlined, NotificationOutlined, CalendarOutlined, AppstoreOutlined } from '@ant-design/icons';
import './MobileBottomNav.css';

/**
 * 移动端底部导航栏组件
 */
const MobileBottomNav: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();

  const navItems = [
    {
      key: '/dashboard',
      icon: <StockOutlined />,
      label: 'Watchlist',
    },
    {
      key: '/news',
      icon: <NotificationOutlined />,
      label: 'News',
    },
    {
      key: '/markets/economic-events',
      icon: <CalendarOutlined />,
      label: 'Events',
    },
    {
      key: '/files',
      icon: <AppstoreOutlined />,
      label: 'More',
    },
  ];

  return (
    <div className="mobile-bottom-nav">
      {navItems.map(item => (
        <div
          key={item.key}
          className={`nav-item ${location.pathname === item.key ? 'active' : ''}`}
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


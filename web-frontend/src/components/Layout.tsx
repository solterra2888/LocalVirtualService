import React, { useEffect, useState } from 'react';
import { Layout as AntLayout, Menu, Button, Dropdown, Avatar, Typography, Tooltip, Badge } from 'antd';
import {
  HomeOutlined,
  FileTextOutlined,
  AppstoreOutlined,
  UserOutlined,
  LogoutOutlined,
  TeamOutlined,
  ToolOutlined,
  CodeOutlined,
  StockOutlined,
  RobotOutlined,
  SettingOutlined,
  BugOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  NotificationOutlined,
  ThunderboltOutlined,
  BuildOutlined
} from '@ant-design/icons';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useAuthStore } from '../store/authStore';
import { useGlobalLogger } from '../contexts/LoggerContext';
import { LogViewer } from '../components/LogViewer';
import TimeDisplay from './TimeDisplay';

const { Header, Sider, Content } = AntLayout;
const { Title } = Typography;

const Layout: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { user, logout } = useAuthStore();
  const globalLogger = useGlobalLogger();
  const [collapsed, setCollapsed] = useState(false);

  // 快捷键支持：Ctrl + ` 切换日志窗口
  useEffect(() => {
    const handleKeyPress = (e: KeyboardEvent) => {
      if (e.ctrlKey && e.key === '`') {
        e.preventDefault();
        globalLogger.actions.setShowFloating(!globalLogger.showFloating);
      }
    };
    
    document.addEventListener('keydown', handleKeyPress);
    return () => document.removeEventListener('keydown', handleKeyPress);
  }, [globalLogger]);



  // 菜单项配置
  const menuItems = [
    {
      key: 'dashboard',
      icon: <HomeOutlined />,
      label: 'UG Dashboard',
      children: [
        {
          key: '/dashboard',
          icon: <HomeOutlined />,
          label: 'Target Tracking',
        },
        {
          key: '/fundamentals/screener',
          icon: <AppstoreOutlined />,
          label: 'Stock Screener',
        },
      ],
    },
    {
      key: 'markets',
      icon: <StockOutlined />,
      label: 'Markets',
      children: [
        {
          key: '/markets/economic-events',
          icon: <NotificationOutlined />,
          label: 'Economic Events',
        },
        {
          key: '/markets/macro-indicators',
          icon: <CodeOutlined />,
          label: 'Macro Indicators',
        },
      ],
    },
    {
      key: '/news',
      icon: <NotificationOutlined />,
      label: 'News',
    },
    {
      key: '/fundamentals',
      icon: <FileTextOutlined />,
      label: 'Company Query',
    },
    {
      key: 'ai-agents',
      icon: <RobotOutlined />,
      label: 'AI Agents',
      children: [
        {
          key: '/files',
          icon: <FileTextOutlined />,
          label: 'Upload Files',
        },
        {
          key: '/tasks',
          icon: <AppstoreOutlined />,
          label: 'AI Agent Management',
        },
      ],
    },
    {
      key: 'settings',
      icon: <SettingOutlined />,
      label: 'Settings',
      children: [
        {
          key: '/scheduler',
          icon: <ThunderboltOutlined />,
          label: 'Data Source Management',
        },
        {
          key: '/diagnostic',
          icon: <BugOutlined />,
          label: 'System Diagnostics',
        },
        ...(user?.is_admin ? [{
          key: '/users',
          icon: <TeamOutlined />,
          label: 'User Management',
        }] : []),
      ],
    },
  ];

  // 用户菜单配置
  const userMenuItems = [
    {
      key: 'settings',
      icon: <ToolOutlined />,
      label: 'Settings',
      onClick: () => {
        // TODO: 实现设置功能
      }
    },
    {
      key: 'logout',
      icon: <LogoutOutlined />,
      label: 'Logout',
      onClick: handleLogout
    }
  ];

  function handleMenuClick({ key }: { key: string }) {
    navigate(key);
  }

  function handleLogout() {
    logout();
    navigate('/login');
  }

  // 获取当前页面标题
  const getCurrentPageTitle = () => {
    // 递归查找菜单项
    const findMenuItemByKey = (items: any[], targetKey: string): any => {
      for (const item of items) {
        if (item.key === targetKey) {
          return item;
        }
        if (item.children) {
          const found = findMenuItemByKey(item.children, targetKey);
          if (found) return found;
        }
      }
      return null;
    };

    const currentItem = findMenuItemByKey(menuItems, location.pathname);
    return currentItem?.label || 'Universal Glance';
  };

  // 检查是否有新的错误日志（用于显示红点提示）
  const hasErrorLogs = globalLogger.logs.some(log => 
    log.level === 'error' && 
    Date.now() - new Date(log.timestamp).getTime() < 5 * 60 * 1000 // 5分钟内的错误
  );

  return (
    <AntLayout className="main-layout">
      <Sider 
        width={240} 
        className="layout-sider"
        theme="light"
        breakpoint="lg"
        collapsedWidth="80"
        collapsed={collapsed}
        trigger={null}
      >
        <div style={{ 
          padding: collapsed ? '16px 8px' : '24px 16px', 
          borderBottom: '1px solid #f0f0f0',
          textAlign: collapsed ? 'center' : 'left'
        }}>
          <Title 
            level={collapsed ? 5 : 4} 
            style={{ 
              margin: 0, 
              textAlign: collapsed ? 'center' : 'left',
              fontSize: collapsed ? '14px' : '20px'
            }}
          >
            {collapsed ? 'UG' : 'Universal Glance'}
          </Title>
        </div>
        
        <Menu
          mode="inline"
          selectedKeys={[location.pathname]}
          defaultOpenKeys={['dashboard', 'markets', 'ai-agents', 'settings']}
          items={menuItems}
          onClick={handleMenuClick}
          style={{ border: 'none', height: 'calc(100vh - 84px)' }}
          inlineCollapsed={collapsed}
        />
      </Sider>

      <AntLayout className="main-content-layout">
        <Header className="layout-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
            <Button
              type="text"
              icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
              onClick={() => setCollapsed(!collapsed)}
              className="collapse-button"
              style={{
                fontSize: '16px',
                width: 64,
                height: 64,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
              }}
            />
            <div style={{ fontSize: 16, fontWeight: 500 }}>
              {getCurrentPageTitle()}
            </div>
          </div>
          
          <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
            <span>Welcome, {user?.username}</span>
            
            {/* 时间显示 */}
            <TimeDisplay />
            
            {/* 全局日志按钮 */}
            <Tooltip title="System Logs (Ctrl + `)">
              <Badge dot={hasErrorLogs} size="small">
                <Button
                  type="text"
                  icon={<CodeOutlined />}
                  onClick={() => globalLogger.actions.setShowFloating(true)}
                  style={{
                    color: globalLogger.showFloating ? '#1890ff' : '#8c8c8c',
                    fontSize: '16px'
                  }}
                />
              </Badge>
            </Tooltip>
            
            {user?.is_admin && (
              <span style={{ 
                background: '#f50', 
                color: 'white', 
                padding: '2px 8px', 
                borderRadius: 4,
                fontSize: 12
              }}>
                Admin
              </span>
            )}
            
            <Dropdown 
              menu={{ items: userMenuItems }}
              placement="bottomRight"
            >
              <Button
                type="text"
                icon={<Avatar size="small" icon={<UserOutlined />} />}
                style={{ display: 'flex', alignItems: 'center' }}
              />
            </Dropdown>
          </div>
        </Header>

        <Content className="layout-content">
          <Outlet />
        </Content>
      </AntLayout>
      
      {/* 全局日志查看器 */}
      <LogViewer
        logs={globalLogger.logs}
        autoScroll={globalLogger.autoScroll}
        showModal={globalLogger.showModal}
        showFloating={globalLogger.showFloating}
        minimizedFloating={globalLogger.minimizedFloating}
        onAutoScrollChange={globalLogger.actions.setAutoScroll}
        onModalClose={() => globalLogger.actions.setShowModal(false)}
        onFloatingClose={() => globalLogger.actions.setShowFloating(false)}
        onMinimizeToggle={() => globalLogger.actions.setMinimizedFloating(!globalLogger.minimizedFloating)}
        onClearLogs={globalLogger.actions.clearLogs}
        onDownloadLogs={globalLogger.actions.downloadLogs}
      />
    </AntLayout>
  );
};

export default Layout; 
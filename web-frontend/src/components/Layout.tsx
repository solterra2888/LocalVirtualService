import React, { useEffect, useState } from 'react';
import { Layout as AntLayout, Menu, Button, Dropdown, Avatar, Typography, Tooltip, Badge } from 'antd';
import {
  FileTextOutlined,
  AppstoreOutlined,
  UserOutlined,
  LogoutOutlined,
  TeamOutlined,
  ToolOutlined,
  CodeOutlined,
  DatabaseOutlined,
  StockOutlined,
  RobotOutlined,
  SettingOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  NotificationOutlined,
  FundOutlined,
  BuildOutlined,
  CalendarOutlined,
  ControlOutlined,
  BarChartOutlined
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
      key: '/market-weekly',
      icon: <BarChartOutlined />,
      label: 'Market Weekly',
    },
    {
      key: '/dashboard',
      icon: <StockOutlined />,
      label: 'Target Tracking',
    },
    {
      key: '/news',
      icon: <NotificationOutlined />,
      label: 'News',
    },
    {
      key: '/markets/economic-events',
      icon: <CalendarOutlined />,
      label: 'Economic Events',
    },
    {
      key: 'ug-data',
      icon: <FileTextOutlined />,
      label: 'UG Data',
      children: [
        {
          key: '/fundamentals',
          icon: <FileTextOutlined />,
          label: 'Company Query',
        },
        {
          key: '/markets/macro-indicators',
          icon: <CodeOutlined />,
          label: 'Macro Indicators',
        },
        {
          key: '/eod-fetch',
          icon: <DatabaseOutlined />,
          label: 'EOD Fetch',
        },
      ],
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
          key: '/prompt-templates',
          icon: <BuildOutlined />,
          label: 'Prompt Templates',
        },
        {
          key: '/investment-themes',
          icon: <FundOutlined />,
          label: 'Themes Settings',
        },
        {
          key: '/tasks',
          icon: <AppstoreOutlined />,
          label: 'AI Agent Settings',
        },
        ...(user?.is_admin ? [
          {
            key: '/llm-config',
            icon: <RobotOutlined />,
            label: 'LLM Settings',
          }
        ] : []),
      ],
    },
    {
      key: 'settings',
      icon: <SettingOutlined />,
      label: 'Settings',
      children: [
        {
          key: '/scheduler',
          icon: <ControlOutlined />,
          label: 'Data Source Management',
        },
        ...(user?.is_admin ? [
          {
            key: '/users',
            icon: <TeamOutlined />,
            label: 'User Management',
          }
        ] : []),
      ],
    },
  ].filter(item => {
    // 如果菜单项有子菜单，检查是否有可见的子菜单项
    if (item.children) {
      return item.children.length > 0;
    }
    // 如果没有子菜单，直接显示
    return true;
  });

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
          textAlign: collapsed ? 'center' : 'left',
          display: 'flex',
          alignItems: 'center',
          justifyContent: collapsed ? 'center' : 'space-between'
        }}>
          <Title 
            level={collapsed ? 5 : 4} 
            style={{ 
              margin: 0, 
              textAlign: collapsed ? 'center' : 'left',
              fontSize: collapsed ? '14px' : '20px',
              cursor: collapsed ? 'pointer' : 'default'
            }}
            onClick={collapsed ? () => setCollapsed(false) : undefined}
          >
            {collapsed ? 'UG' : 'Universal Glance'}
          </Title>
          {!collapsed && (
            <Button
              type="text"
              icon={<MenuFoldOutlined />}
              onClick={() => setCollapsed(true)}
              style={{
                fontSize: '16px',
                width: 32,
                height: 32,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
              }}
            />
          )}
        </div>
        
        <Menu
          mode="inline"
          selectedKeys={[location.pathname]}
          defaultOpenKeys={['ug-data', 'ai-agents', 'settings']}
          items={menuItems}
          onClick={handleMenuClick}
          style={{ border: 'none', height: collapsed ? 'calc(100vh - 120px)' : 'calc(100vh - 120px)' }}
          inlineCollapsed={collapsed}
        />
        {collapsed && (
          <div style={{ 
            padding: '16px 8px', 
            borderTop: '1px solid #f0f0f0',
            textAlign: 'center'
          }}>
            <Button
              type="text"
              icon={<MenuUnfoldOutlined />}
              onClick={() => setCollapsed(false)}
              title="展开导航栏"
              style={{
                fontSize: '16px',
                width: 32,
                height: 32,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
              }}
            />
          </div>
        )}
      </Sider>

      <AntLayout className="main-content-layout">
        <Header className="layout-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
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
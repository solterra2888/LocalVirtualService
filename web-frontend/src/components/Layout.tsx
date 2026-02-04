import React, { useEffect } from 'react';
import { Layout as AntLayout, Menu, Button, Dropdown, Avatar, Typography, Tooltip, Badge, Switch } from 'antd';
import {
  FileTextOutlined,
  AppstoreOutlined,
  UserOutlined,
  LogoutOutlined,
  TeamOutlined,
  CodeOutlined,
  DatabaseOutlined,
  StockOutlined,
  RobotOutlined,
  SettingOutlined,
  NotificationOutlined,
  FundOutlined,
  BuildOutlined,
  CalendarOutlined,
  ControlOutlined,
  BarChartOutlined,
  HistoryOutlined,
  FolderOutlined
} from '@ant-design/icons';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useAuthStore } from '../store/authStore';
import { useGlobalLogger } from '../contexts/LoggerContext';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { LogViewer } from '../components/LogViewer';
import TimeDisplay from './TimeDisplay';

// 导航栏主题（与 Dashboard dark 蓝灰一致）
const LAYOUT_NAV_THEME = {
  light: {
    siderBg: '#fff',
    siderBorder: '#f0f0f0',
    navText: 'rgba(0, 0, 0, 0.88)',
    headerBg: '#fff',
    headerText: 'rgba(0, 0, 0, 0.88)',
    headerTextSecondary: 'rgba(0, 0, 0, 0.45)',
  },
  dark: {
    siderBg: '#212936',
    siderBorder: '#364152',
    navText: '#e0e6f1',
    headerBg: '#1a2234',
    headerText: '#e0e6f1',
    headerTextSecondary: 'rgba(255, 255, 255, 0.5)',
  },
} as const;

const { Header, Content } = AntLayout;
const { Title } = Typography;

const Layout: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { user, logout } = useAuthStore();
  const globalLogger = useGlobalLogger();
  const { themeMode, setThemeMode } = useDashboardSettings();
  const navTheme = LAYOUT_NAV_THEME[themeMode];

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

  // dark 模式下为根容器设置深色背景，避免露出白色
  useEffect(() => {
    const root = document.querySelector('.App');
    if (root) {
      root.classList.toggle('app-dark', themeMode === 'dark');
    }
    return () => {
      root?.classList.remove('app-dark');
    };
  }, [themeMode]);



  // 菜单项配置
  const menuItems = [
    {
      key: '/dashboard',
      icon: <StockOutlined />,
      label: 'Watchlist',
    },
    {
      key: '/projects',
      icon: <FolderOutlined />,
      label: 'Investment Tracking',
    },
    {
      key: '/news',
      icon: <NotificationOutlined />,
      label: 'News',
    },
    {
      key: '/market-weekly',
      icon: <BarChartOutlined />,
      label: 'Market Weekly',
    },
    {
      key: 'ug-data',
      icon: <FileTextOutlined />,
      label: 'UG Data',
      children: [
        {
          key: '/markets/economic-events',
          icon: <CalendarOutlined />,
          label: 'Economic Events',
        },
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
      key: 'system',
      icon: <SettingOutlined />,
      label: 'System',
      children: [
        {
          key: '/scheduler',
          icon: <ControlOutlined />,
          label: 'Scheduler Management',
        },
        {
          key: '/scheduler/logs',
          icon: <HistoryOutlined />,
          label: 'Scheduler Logs',
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

  // 用户菜单：仅登出（System 已移至顶栏）
  const userMenuItems = [
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

  // 检查是否有新的错误日志（用于显示红点提示）
  const hasErrorLogs = globalLogger.logs.some(log => 
    log.level === 'error' && 
    Date.now() - new Date(log.timestamp).getTime() < 5 * 60 * 1000 // 5分钟内的错误
  );

  return (
    <AntLayout
      className={`main-layout ${themeMode === 'dark' ? 'main-layout-dark' : ''}`}
      style={{ background: themeMode === 'dark' ? '#1a202c' : undefined }}
    >
      <Header
        className="layout-header"
        style={{
          background: navTheme.headerBg,
          color: navTheme.headerText,
          boxShadow: themeMode === 'dark' ? '0 1px 4px rgba(0,0,0,0.2)' : undefined,
          padding: '0 24px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 24,
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 24, flex: 1, minWidth: 0 }}>
          <Title
            level={5}
            style={{
              margin: 0,
              fontSize: '18px',
              fontWeight: 600,
              color: navTheme.headerText,
              whiteSpace: 'nowrap',
              flexShrink: 0,
            }}
          >
            Universal Glance
          </Title>
          <Menu
            mode="horizontal"
            theme={themeMode}
            selectedKeys={[location.pathname]}
            items={menuItems}
            onClick={handleMenuClick}
            style={{
              border: 'none',
              background: 'transparent',
              flex: 1,
              minWidth: 0,
              lineHeight: '64px',
            }}
            className="layout-top-nav-menu"
          />
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 16, color: navTheme.headerText, flexShrink: 0 }}>
          <span style={{ color: navTheme.headerTextSecondary, fontSize: 13 }}>Welcome, {user?.username}</span>

          <TimeDisplay />

          <Tooltip title={themeMode === 'dark' ? 'Dark mode' : 'Light mode'}>
            <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ fontSize: 12, color: navTheme.headerTextSecondary }}>Dark</span>
              <Switch
                size="small"
                checked={themeMode === 'dark'}
                onChange={(checked) => setThemeMode(checked ? 'dark' : 'light')}
              />
            </span>
          </Tooltip>

          <Tooltip title="System Logs (Ctrl + `)">
            <Badge dot={hasErrorLogs} size="small">
              <Button
                type="text"
                icon={<CodeOutlined />}
                onClick={() => globalLogger.actions.setShowFloating(true)}
                style={{
                  color: globalLogger.showFloating ? '#1890ff' : navTheme.headerTextSecondary,
                  fontSize: '16px'
                }}
              />
            </Badge>
          </Tooltip>

          {user?.is_admin && (
            <span style={{
              background: themeMode === 'dark' ? '#38a169' : '#f50',
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
              style={{ display: 'flex', alignItems: 'center', color: navTheme.headerText }}
            />
          </Dropdown>
        </div>
      </Header>

      <Content
        className="layout-content"
        style={{ background: themeMode === 'dark' ? '#1a202c' : undefined }}
      >
        <Outlet />
      </Content>
      
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
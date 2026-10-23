import React, { useEffect, useRef, useState } from 'react';
import type { MenuProps } from 'antd';
import { Layout as AntLayout, Button, Dropdown, Avatar, Switch, Modal, Popover, Tooltip } from 'antd';
import {
  FileTextOutlined,
  LogoutOutlined,
  TeamOutlined,
  CodeOutlined,
  DatabaseOutlined,
  StockOutlined,
  RobotOutlined,
  FundOutlined,
  BuildOutlined,
  CalendarOutlined,
  ScheduleOutlined,
  ControlOutlined,
  LineChartOutlined,
  FolderOutlined,
  UploadOutlined,
  SoundOutlined,
  LinkOutlined,
  SyncOutlined,
  SunOutlined,
  MoonOutlined,
  ReadOutlined,
  AppstoreOutlined,
  EditOutlined,
  KeyOutlined,
  ProfileOutlined,
  GlobalOutlined,
  BankOutlined,
  FundViewOutlined,
  AppleOutlined,
  ApiOutlined,
  CloseOutlined,
  MenuUnfoldOutlined,
} from '@ant-design/icons';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { getModalStyles } from '../utils/modalStyles';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import UserProfileModal from './UserProfileModal';
import TimeDisplay from './TimeDisplay';
import UploadFilesModal from './UploadFilesModal';
import UploadAudioModal from './UploadAudioModal';
import UploadYouTubeLinkModal from './UploadYouTubeLinkModal';
import UploadApplePodcastModal from './UploadApplePodcastModal';
import UploadWebLinkModal from './UploadWebLinkModal';
import CreateTextNoteModal from './CreateTextNoteModal';
import { TaskProgressPopover } from './TaskProgressPopover';
import { userAvatarLetter, USER_AVATAR_LETTER_BG } from '../utils/userAvatar';
import { useScrollingClass } from '../hooks/useScrollingClass';
import './LayoutSidebar.css';
import '../styles/subtleScrollbar.css';

const SIDEBAR_COLLAPSED_WIDTH = 72;
const SIDEBAR_EXPANDED_WIDTH = 260;
const SIDEBAR_BG = 'linear-gradient(180deg, #1b4b7d 0%, #1d5998 100%)';

const NAV_DESCRIPTIONS: Record<string, string> = {
  '/daily': 'Overview & monitoring',
  '/files': 'Sources & documents',
  '/feeds': 'Subscriptions & feeds',
  '/feeds-dashboard': 'Topics & highlights from feeds',
  '/dashboard': 'Markets & watchlist',
  '/reports': 'Daily & Weekly Reports',
  '/agent-integration': 'MCP & integrations',
  '/investment-themes': 'Investment themes & tags',
};

const { Content, Sider } = AntLayout;

function getSelectedNavKey(pathname: string): string {
  if (pathname.startsWith('/reports')) return '/reports';
  if (pathname.startsWith('/agent-integration')) return '/agent-integration';
  if (pathname.startsWith('/daily')) return '/daily';
  if (pathname.startsWith('/files')) return '/files';
  // ⚠ 必须在 '/feeds' 之前判断, 否则 /feeds-dashboard 会被 startsWith('/feeds') 吞掉
  if (pathname.startsWith('/feeds-dashboard')) return '/feeds-dashboard';
  if (pathname.startsWith('/feeds')) return '/feeds';
  if (pathname.startsWith('/dashboard')) return '/dashboard';
  if (pathname.startsWith('/investment-themes')) return '/investment-themes';
  if (pathname.startsWith('/central-bank')) return '/central-bank';
  if (pathname.startsWith('/form13f')) return '/form13f';
  return pathname;
}

// Note: getFileProcessingStatusText removed as it's no longer needed with the new multi-task UI

const Layout: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { user, logout } = useAuthStore();
  const { themeMode, setThemeMode } = useDashboardSettings();
  const { tasks } = useFileProcessing();

  const [showUploadOptions, setShowUploadOptions] = useState(false);
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [showUploadAudioModal, setShowUploadAudioModal] = useState(false);
  const [showUploadYouTubeModal, setShowUploadYouTubeModal] = useState(false);
  const [showUploadApplePodcastModal, setShowUploadApplePodcastModal] = useState(false);
  const [showUploadWebLinkModal, setShowUploadWebLinkModal] = useState(false);
  const [showCreateTextNoteModal, setShowCreateTextNoteModal] = useState(false);
  const [profileModalOpen, setProfileModalOpen] = useState(false);
  const [sidebarExpanded, setSidebarExpanded] = useState(true);
  const sidebarNavRef = useScrollingClass<HTMLElement>();
  const openProfileModal = () => setProfileModalOpen(true);

  const sidebarWidth = sidebarExpanded ? SIDEBAR_EXPANDED_WIDTH : SIDEBAR_COLLAPSED_WIDTH;

  const toggleSidebar = () => setSidebarExpanded((prev) => !prev);

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



  // 主导航：Feeds Dashboard 首位（登录后默认页，按主题聚合视图），Sources 第二，Feeds 第三
  const menuItems = [
    {
      key: '/feeds-dashboard',
      icon: <AppstoreOutlined />,
      label: 'Feeds Dashboard',
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
      key: '/dashboard',
      icon: <StockOutlined />,
      label: 'Watchlist',
    },
    {
      key: '/markets/earnings-calendar',
      icon: <ScheduleOutlined />,
      label: 'Earnings Calendar',
    },
    {
      key: '/markets/economic-events',
      icon: <CalendarOutlined />,
      label: 'Economic Calendar',
    },
    {
      key: '/central-bank',
      icon: <BankOutlined />,
      label: 'Central Banks',
    },
    {
      key: '/form13f',
      icon: <FundViewOutlined />,
      label: '13F Filings',
    },
    {
      key: '/projects',
      icon: <FolderOutlined />,
      label: 'Investment Tracking',
    },
    {
      key: '/reports',
      icon: <ProfileOutlined />,
      label: 'Reports',
    },
    {
      key: '/agent-integration',
      icon: <ApiOutlined />,
      label: 'Agent Integration',
    },
    {
      key: '/investment-themes',
      icon: <FundOutlined />,
      label: 'Themes',
    },
    {
      key: 'ug-data',
      icon: <FileTextOutlined />,
      label: 'UG Data',
      children: [
        // Economic Events 已提升为顶级菜单 "Economic Calendar"（/markets/economic-events）
        { key: '/fundamentals', icon: <FileTextOutlined />, label: 'Company Query' },
        { key: '/markets/macro-indicators', icon: <CodeOutlined />, label: 'Macro Indicators' },
        { key: '/eod-fetch', icon: <DatabaseOutlined />, label: 'EOD Fetch' },
      ],
    },
  ].filter(item => {
    // 暂时屏蔽 Investment Tracking，该模块仍未开发
    if (item.key === '/projects') return false;
    // 暂时屏蔽 UG Data：功能未成熟；路由与页面代码保留
    if (item.key === 'ug-data') return false;
    // 暂时屏蔽 Daily：使用人数不多，默认落地页已改为 Feeds Dashboard；路由与页面代码保留
    if (item.key === '/daily') return false;
    if (item.children) return item.children.length > 0;
    return true;
  });

  // 用户头像下拉：配置类功能 + 登出（主题开关在侧栏底部）
  const userMenuItems = [
    { key: '/settings/api-keys', icon: <KeyOutlined />, label: 'API Keys', onClick: () => navigate('/settings/api-keys') },
    { key: '/prompt-templates', icon: <BuildOutlined />, label: 'Prompt Templates', onClick: () => navigate('/prompt-templates') },
    ...(user?.is_admin ? [{ key: '/llm-config', icon: <RobotOutlined />, label: 'LLM Settings', onClick: () => navigate('/llm-config') }] : []),
    { type: 'divider' as const },
    ...(user?.is_admin
      ? [
          { key: '/scheduler', icon: <ControlOutlined />, label: 'Scheduler Management', onClick: () => navigate('/scheduler') },
          { key: '/monitoring', icon: <LineChartOutlined />, label: 'Monitoring', onClick: () => navigate('/monitoring') },
        ]
      : []),
    ...(user?.is_admin ? [{ key: '/users', icon: <TeamOutlined />, label: 'User Management', onClick: () => navigate('/users') }] : []),
    { type: 'divider' as const },
    { key: 'edit-profile', icon: <EditOutlined />, label: 'Edit Profile', onClick: openProfileModal },
    { key: 'logout', icon: <LogoutOutlined />, label: 'Logout', onClick: handleLogout },
  ];

  function handleMenuClick({ key }: { key: string }) {
    if (!sidebarExpanded) {
      setSidebarExpanded(true);
    }
    navigate(key);
  }

  function handleLogout() {
    logout();
    navigate('/login');
  }

  const selectedNavKey = getSelectedNavKey(location.pathname);
  const flatNavItems = menuItems.filter(
    (item): item is typeof menuItems[number] & { key: string; label: string } =>
      typeof item.key === 'string' && !('children' in item && item.children),
  );

  const renderNavItem = (item: (typeof flatNavItems)[number]) => {
    const isActive = selectedNavKey === item.key;
    const description = NAV_DESCRIPTIONS[item.key] ?? '';
    const button = (
      <button
        type="button"
        className={`layout-sidebar-nav-item${isActive ? ' is-active' : ''}`}
        onClick={() => handleMenuClick({ key: item.key })}
        aria-label={item.label}
        aria-current={isActive ? 'page' : undefined}
      >
        {item.icon}
        {sidebarExpanded && (
          <>
            <span className="layout-sidebar-nav-text">
              <span className="layout-sidebar-nav-label">{item.label}</span>
              {description ? <span className="layout-sidebar-nav-desc">{description}</span> : null}
            </span>
            {isActive ? <span className="layout-sidebar-nav-active-dot" aria-hidden /> : null}
          </>
        )}
      </button>
    );

    if (sidebarExpanded) {
      return (
        <span key={item.key} className="layout-sidebar-nav-tooltip-wrap">
          {button}
        </span>
      );
    }

    return (
      <Tooltip key={item.key} title={item.label} placement="right">
        <span className="layout-sidebar-nav-tooltip-wrap">{button}</span>
      </Tooltip>
    );
  };

  const openUploadSources = () => setShowUploadOptions(true);

  const uploadSourcesButton = (
    <button
      type="button"
      className="layout-sidebar-upload-btn"
      onClick={openUploadSources}
      aria-label="Upload Sources"
    >
      <UploadOutlined />
      {sidebarExpanded && <span className="layout-sidebar-upload-label">Upload Sources</span>}
    </button>
  );

  const toggleThemeMode = () => setThemeMode(themeMode === 'dark' ? 'light' : 'dark');

  const themeModeToggle = sidebarExpanded ? (
    <div
      className="layout-sidebar-theme"
      role="group"
      aria-label="Theme mode"
      onClick={(e) => e.stopPropagation()}
    >
      <span className="layout-sidebar-theme-icon" aria-hidden>
        {themeMode === 'dark' ? <MoonOutlined /> : <SunOutlined />}
      </span>
      <span className="layout-sidebar-theme-label">
        {themeMode === 'dark' ? 'Dark mode' : 'Light mode'}
      </span>
      <Switch
        size="small"
        checked={themeMode === 'dark'}
        onChange={(checked) => setThemeMode(checked ? 'dark' : 'light')}
        aria-label={themeMode === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
      />
    </div>
  ) : (
    <Tooltip
      title={themeMode === 'dark' ? 'Dark mode — click for light' : 'Light mode — click for dark'}
      placement="right"
    >
      <button
        type="button"
        className="layout-sidebar-theme-collapsed"
        onClick={toggleThemeMode}
        aria-label={themeMode === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
      >
        {themeMode === 'dark' ? <MoonOutlined /> : <SunOutlined />}
      </button>
    </Tooltip>
  );

  const taskProgressButton = (
    <button
      type="button"
      className="layout-sidebar-task-dot"
      onClick={() => navigate('/files')}
      aria-label={`${tasks.size} processing tasks`}
    >
      <SyncOutlined spin />
      {sidebarExpanded && (
        <span className="layout-sidebar-task-label">
          {tasks.size} {tasks.size === 1 ? 'task' : 'tasks'} processing
        </span>
      )}
      {!sidebarExpanded && <span className="layout-sidebar-task-badge">{tasks.size}</span>}
    </button>
  );

  return (
    <AntLayout
      className={`main-layout main-layout-with-sider ${sidebarExpanded ? 'layout-sidebar-expanded' : 'layout-sidebar-collapsed'} ${themeMode === 'dark' ? 'main-layout-dark' : ''}`}
      style={{
        background: themeMode === 'dark' ? '#1a202c' : undefined,
        minHeight: '100vh',
        ['--layout-sidebar-width' as string]: `${sidebarWidth}px`,
      }}
    >
      <Sider
        width={sidebarWidth}
        collapsedWidth={SIDEBAR_COLLAPSED_WIDTH}
        collapsible={false}
        className={`layout-sidebar ${sidebarExpanded ? 'layout-sidebar-expanded' : 'layout-sidebar-collapsed'}`}
        style={{
          background: SIDEBAR_BG,
          width: sidebarWidth,
          minWidth: sidebarWidth,
          maxWidth: sidebarWidth,
          flex: `0 0 ${sidebarWidth}px`,
        }}
      >
        <header className="layout-sidebar-header">
          <div className="layout-sidebar-header-top">
            {sidebarExpanded ? (
              <>
                <div className="layout-sidebar-brand-meta">
                  <h2 className="layout-sidebar-brand-title">GuanHe Terminal</h2>
                </div>
                <button
                  type="button"
                  className="layout-sidebar-toggle-btn"
                  onClick={toggleSidebar}
                  aria-label="Collapse sidebar"
                >
                  <CloseOutlined />
                </button>
              </>
            ) : (
              <Tooltip title="Expand sidebar" placement="right">
                <button
                  type="button"
                  className="layout-sidebar-expand-btn"
                  onClick={toggleSidebar}
                  aria-label="Expand sidebar"
                >
                  <MenuUnfoldOutlined />
                </button>
              </Tooltip>
            )}
          </div>
          <div className="layout-sidebar-brand-block">
            {sidebarExpanded ? (
              uploadSourcesButton
            ) : (
              <Tooltip title="Upload Sources" placement="right">
                <span className="layout-sidebar-upload-tooltip-wrap">{uploadSourcesButton}</span>
              </Tooltip>
            )}
          </div>
        </header>

        <nav
          ref={sidebarNavRef}
          className="layout-sidebar-nav subtle-scrollbar subtle-scrollbar--sidebar"
          aria-label="Main navigation"
        >
          {flatNavItems.map(renderNavItem)}
        </nav>

        <div className="layout-sidebar-footer">
          <div className="layout-sidebar-theme-wrap">{themeModeToggle}</div>

          <div className="layout-sidebar-time-wrap">
            <TimeDisplay variant="sidebar" compact={!sidebarExpanded} />
          </div>

          {tasks.size > 0 && (
            <Popover
              content={<TaskProgressPopover tasks={tasks} themeMode={themeMode} />}
              placement={sidebarExpanded ? 'right' : 'rightTop'}
              trigger="hover"
              overlayClassName={themeMode === 'dark' ? 'task-progress-popover-dark' : undefined}
              overlayStyle={{
                ...(themeMode === 'dark'
                  ? {
                      background: '#1a2234',
                      border: '1px solid #364152',
                      borderRadius: 8,
                    }
                  : {}),
                boxShadow: themeMode === 'dark'
                  ? '0 6px 16px rgba(0, 0, 0, 0.5)'
                  : '0 3px 12px rgba(0, 0, 0, 0.15)',
              }}
            >
              {taskProgressButton}
            </Popover>
          )}

          <Dropdown
            menu={{
              items: userMenuItems,
              selectedKeys: userMenuItems.some((i: { key?: string }) => i.key === location.pathname)
                ? [location.pathname]
                : [],
              style: { minWidth: 200 },
            }}
            placement={sidebarExpanded ? 'top' : 'topRight'}
          >
            <button type="button" className="layout-sidebar-user-btn" aria-label="Account menu">
              {user?.avatar_url ? (
                <Avatar size={sidebarExpanded ? 32 : 36} src={user.avatar_url} />
              ) : (
                <Avatar
                  size={sidebarExpanded ? 32 : 36}
                  style={{ backgroundColor: USER_AVATAR_LETTER_BG, fontSize: 14 }}
                >
                  {userAvatarLetter(user?.username)}
                </Avatar>
              )}
              {sidebarExpanded && (
                <span className="layout-sidebar-user-meta">
                  <span className="layout-sidebar-user-label">
                    {user?.username ? `Welcome, ${user.username}` : 'Account'}
                  </span>
                  {user?.is_admin && <span className="layout-sidebar-admin-badge">Admin</span>}
                </span>
              )}
            </button>
          </Dropdown>
        </div>
      </Sider>

      <AntLayout style={{ background: themeMode === 'dark' ? '#1a202c' : undefined }}>
        <Content
          className="layout-content"
          style={{ background: themeMode === 'dark' ? '#1a202c' : undefined }}
        >
          <Outlet />
        </Content>
      </AntLayout>

      {/* 上传选项菜单 Modal（由侧栏 Upload Sources 打开） */}
      <Modal
        open={showUploadOptions}
        onCancel={() => setShowUploadOptions(false)}
        footer={null}
        width={360}
        centered
        closable={false}
        maskStyle={{
          background: themeMode === 'dark' ? 'rgba(0, 0, 0, 0.6)' : 'rgba(0, 0, 0, 0.25)',
          backdropFilter: 'blur(6px)',
          WebkitBackdropFilter: 'blur(6px)',
        }}
        styles={{
          ...getModalStyles(themeMode),
          body: { ...getModalStyles(themeMode).body, padding: '24px 16px' },
        }}
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
          <Button
            type="text"
            size="large"
            icon={<EditOutlined style={{ fontSize: '18px' }} />}
            onClick={() => {
              setShowUploadOptions(false);
              setShowCreateTextNoteModal(true);
            }}
            style={{
              height: '52px',
              fontSize: '15px',
              fontWeight: 500,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'flex-start',
              paddingLeft: '20px',
              background: themeMode === 'dark' ? '#212936' : '#fafafa',
              color: themeMode === 'dark' ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
              border: 'none',
              borderRadius: '26px',
              transition: 'all 0.2s',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = themeMode === 'dark' ? '#2a3544' : '#e6f7ff';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = themeMode === 'dark' ? '#212936' : '#fafafa';
            }}
          >
            <span style={{ marginLeft: '12px' }}>My Notes / Thoughts</span>
          </Button>
          <Button
            type="text"
            size="large"
            icon={<FileTextOutlined style={{ fontSize: '18px' }} />}
            onClick={() => {
              setShowUploadOptions(false);
              setShowUploadModal(true);
            }}
            style={{
              height: '52px',
              fontSize: '15px',
              fontWeight: 500,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'flex-start',
              paddingLeft: '20px',
              background: themeMode === 'dark' ? '#212936' : '#fafafa',
              color: themeMode === 'dark' ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
              border: 'none',
              borderRadius: '26px',
              transition: 'all 0.2s',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = themeMode === 'dark' ? '#2a3544' : '#e6f7ff';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = themeMode === 'dark' ? '#212936' : '#fafafa';
            }}
          >
            <span style={{ marginLeft: '12px' }}>Upload Files</span>
          </Button>
          <Button
            type="text"
            size="large"
            icon={<SoundOutlined style={{ fontSize: '18px' }} />}
            onClick={() => {
              setShowUploadOptions(false);
              setShowUploadAudioModal(true);
            }}
            style={{
              height: '52px',
              fontSize: '15px',
              fontWeight: 500,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'flex-start',
              paddingLeft: '20px',
              background: themeMode === 'dark' ? '#212936' : '#fafafa',
              color: themeMode === 'dark' ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
              border: 'none',
              borderRadius: '26px',
              transition: 'all 0.2s',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = themeMode === 'dark' ? '#2a3544' : '#e6f7ff';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = themeMode === 'dark' ? '#212936' : '#fafafa';
            }}
          >
            <span style={{ marginLeft: '12px' }}>Upload Audio</span>
          </Button>
          <Button
            type="text"
            size="large"
            icon={<LinkOutlined style={{ fontSize: '18px' }} />}
            onClick={() => {
              setShowUploadOptions(false);
              setShowUploadYouTubeModal(true);
            }}
            style={{
              height: '52px',
              fontSize: '15px',
              fontWeight: 500,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'flex-start',
              paddingLeft: '20px',
              background: themeMode === 'dark' ? '#212936' : '#fafafa',
              color: themeMode === 'dark' ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
              border: 'none',
              borderRadius: '26px',
              transition: 'all 0.2s',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = themeMode === 'dark' ? '#2a3544' : '#e6f7ff';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = themeMode === 'dark' ? '#212936' : '#fafafa';
            }}
          >
            <span             style={{ marginLeft: '12px' }}>YouTube Link</span>
          </Button>
          <Button
            type="text"
            size="large"
            icon={<AppleOutlined style={{ fontSize: '18px', color: '#7224d8' }} />}
            onClick={() => {
              setShowUploadOptions(false);
              setShowUploadApplePodcastModal(true);
            }}
            style={{
              height: '52px',
              fontSize: '15px',
              fontWeight: 500,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'flex-start',
              paddingLeft: '20px',
              background: themeMode === 'dark' ? '#212936' : '#fafafa',
              color: themeMode === 'dark' ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
              border: 'none',
              borderRadius: '26px',
              transition: 'all 0.2s',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = themeMode === 'dark' ? '#2a3544' : '#e6f7ff';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = themeMode === 'dark' ? '#212936' : '#fafafa';
            }}
          >
            <span style={{ marginLeft: '12px' }}>Apple Podcast</span>
          </Button>
          <Button
            type="text"
            size="large"
            icon={<GlobalOutlined style={{ fontSize: '18px' }} />}
            onClick={() => {
              setShowUploadOptions(false);
              setShowUploadWebLinkModal(true);
            }}
            style={{
              height: '52px',
              fontSize: '15px',
              fontWeight: 500,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'flex-start',
              paddingLeft: '20px',
              background: themeMode === 'dark' ? '#212936' : '#fafafa',
              color: themeMode === 'dark' ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
              border: 'none',
              borderRadius: '26px',
              transition: 'all 0.2s',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = themeMode === 'dark' ? '#2a3544' : '#e6f7ff';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = themeMode === 'dark' ? '#212936' : '#fafafa';
            }}
          >
            <span style={{ marginLeft: '12px' }}>Web Link</span>
          </Button>
        </div>
      </Modal>

      {/* Upload Files Modal */}
      <UploadFilesModal
        visible={showUploadModal}
        onCancel={() => setShowUploadModal(false)}
        themeMode={themeMode}
      />

      {/* Upload Audio Modal */}
      <UploadAudioModal
        visible={showUploadAudioModal}
        onCancel={() => setShowUploadAudioModal(false)}
        themeMode={themeMode}
      />

      {/* Upload YouTube Link Modal */}
      <UploadYouTubeLinkModal
        visible={showUploadYouTubeModal}
        onCancel={() => setShowUploadYouTubeModal(false)}
        themeMode={themeMode}
      />

      {/* Upload Apple Podcast Modal */}
      <UploadApplePodcastModal
        visible={showUploadApplePodcastModal}
        onCancel={() => setShowUploadApplePodcastModal(false)}
        themeMode={themeMode}
      />

      {/* Upload Web Link Modal */}
      <UploadWebLinkModal
        visible={showUploadWebLinkModal}
        onCancel={() => setShowUploadWebLinkModal(false)}
        themeMode={themeMode}
      />

      {/* Create Text Note Modal */}
      <CreateTextNoteModal
        visible={showCreateTextNoteModal}
        onCancel={() => setShowCreateTextNoteModal(false)}
        themeMode={themeMode}
      />

      <UserProfileModal
        open={profileModalOpen}
        onClose={() => setProfileModalOpen(false)}
        themeMode={themeMode}
      />
    </AntLayout>
  );
};

export default Layout; 
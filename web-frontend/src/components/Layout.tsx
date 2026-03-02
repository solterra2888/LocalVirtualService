import React, { useEffect, useState, useRef } from 'react';
import type { MenuProps } from 'antd';
import { Layout as AntLayout, Menu, Button, Dropdown, Avatar, Typography, Badge, Switch, Modal, Popover, Input, message } from 'antd';
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
  NotificationOutlined,
  FundOutlined,
  BuildOutlined,
  CalendarOutlined,
  ControlOutlined,
  BarChartOutlined,
  HistoryOutlined,
  FolderOutlined,
  UploadOutlined,
  SoundOutlined,
  LinkOutlined,
  SyncOutlined,
  SunOutlined,
  MoonOutlined,
  ReadOutlined,
  EditOutlined,
  CameraOutlined,
} from '@ant-design/icons';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useAuthStore } from '../store/authStore';
import { useGlobalLogger } from '../contexts/LoggerContext';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import { LogViewer } from '../components/LogViewer';
import { usersApi } from '../services/users';
import TimeDisplay from './TimeDisplay';
import UploadFilesModal from './UploadFilesModal';
import UploadAudioModal from './UploadAudioModal';
import UploadYouTubeLinkModal from './UploadYouTubeLinkModal';
import { TaskProgressPopover } from './TaskProgressPopover';

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

// Note: getFileProcessingStatusText removed as it's no longer needed with the new multi-task UI

const Layout: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { user, logout, updateUser } = useAuthStore();
  const globalLogger = useGlobalLogger();
  const { themeMode, setThemeMode } = useDashboardSettings();
  const { tasks } = useFileProcessing();
  const navTheme = LAYOUT_NAV_THEME[themeMode];
  
  // 浮动上传按钮相关状态
  const [floatingExpanded, setFloatingExpanded] = useState(false);
  const [showUploadOptions, setShowUploadOptions] = useState(false);
  const [showUploadModal, setShowUploadModal] = useState(false);
  const [showUploadAudioModal, setShowUploadAudioModal] = useState(false);
  const [showUploadYouTubeModal, setShowUploadYouTubeModal] = useState(false);

  // Profile edit modal
  const [profileModalOpen, setProfileModalOpen] = useState(false);
  const [profileUsername, setProfileUsername] = useState('');
  const [profileAvatarUrl, setProfileAvatarUrl] = useState<string | undefined>();
  const [profileSaving, setProfileSaving] = useState(false);
  const [avatarUploading, setAvatarUploading] = useState(false);
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const avatarInputRef = useRef<HTMLInputElement>(null);

  const openProfileModal = () => {
    setProfileUsername(user?.username || '');
    setProfileAvatarUrl(user?.avatar_url);
    setNewPassword('');
    setConfirmPassword('');
    setProfileModalOpen(true);
  };

  const handleAvatarUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const allowed = ['image/jpeg', 'image/png', 'image/gif', 'image/webp'];
    if (!allowed.includes(file.type)) {
      message.error('Only JPG, PNG, GIF, WEBP allowed');
      return;
    }
    if (file.size > 5 * 1024 * 1024) {
      message.error('Avatar must be under 5MB');
      return;
    }
    setAvatarUploading(true);
    try {
      const result = await usersApi.uploadAvatar(file);
      setProfileAvatarUrl(result.avatar_url);
      updateUser({ avatar_url: result.avatar_url });
      message.success('Avatar uploaded');
    } catch {
      message.error('Avatar upload failed');
    } finally {
      setAvatarUploading(false);
      if (avatarInputRef.current) avatarInputRef.current.value = '';
    }
  };

  const handleProfileSave = async () => {
    if (!profileUsername.trim()) {
      message.error('Username cannot be empty');
      return;
    }
    if (newPassword || confirmPassword) {
      if (newPassword.length < 6) {
        message.error('Password must be at least 6 characters');
        return;
      }
      if (newPassword !== confirmPassword) {
        message.error('Passwords do not match');
        return;
      }
    }
    setProfileSaving(true);
    try {
      const updated = await usersApi.updateMyProfile({
        username: profileUsername.trim(),
        avatar_url: profileAvatarUrl,
      });
      updateUser({ username: updated.username, avatar_url: updated.avatar_url });

      if (newPassword && user?.id) {
        await usersApi.changePassword(user.id, newPassword);
      }

      message.success(newPassword ? 'Profile and password updated' : 'Profile updated');
      setProfileModalOpen(false);
    } catch {
      message.error('Failed to update profile');
    } finally {
      setProfileSaving(false);
    }
  };

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



  // 主导航：Source 首位，Feeds 第二，其余日常业务入口 + Add Content
  const menuItems = [
    {
      key: '/files',
      icon: <FileTextOutlined />,
      label: 'Source',
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
        { key: '/markets/economic-events', icon: <CalendarOutlined />, label: 'Economic Events' },
        { key: '/fundamentals', icon: <FileTextOutlined />, label: 'Company Query' },
        { key: '/markets/macro-indicators', icon: <CodeOutlined />, label: 'Macro Indicators' },
        { key: '/eod-fetch', icon: <DatabaseOutlined />, label: 'EOD Fetch' },
      ],
    },
  ].filter(item => {
    // 暂时屏蔽 Investment Tracking，该模块仍未开发
    if (item.key === '/projects') return false;
    if (item.children) return item.children.length > 0;
    return true;
  });

  // 检查是否有新的错误日志（用于显示红点提示）
  const hasErrorLogs = globalLogger.logs.some(log =>
    log.level === 'error' &&
    Date.now() - new Date(log.timestamp).getTime() < 5 * 60 * 1000 // 5分钟内的错误
  );

  // 用户头像下拉：主题 + 配置类功能 + 登出
  const userMenuItems = [
    {
      key: 'theme',
      icon: themeMode === 'dark' ? <MoonOutlined /> : <SunOutlined />,
      label: (
        <span onClick={(e) => e.stopPropagation()} style={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'space-between', width: '100%', gap: 8 }}>
          <span>Mode</span>
          <Switch size="small" checked={themeMode === 'dark'} onChange={(checked) => setThemeMode(checked ? 'dark' : 'light')} />
        </span>
      ),
      onClick: (info: Parameters<NonNullable<MenuProps['onClick']>>[0]) => info.domEvent.stopPropagation(),
    },
    { type: 'divider' as const },
    { key: '/prompt-templates', icon: <BuildOutlined />, label: 'Prompt Templates', onClick: () => navigate('/prompt-templates') },
    { key: '/investment-themes', icon: <FundOutlined />, label: 'Themes Settings', onClick: () => navigate('/investment-themes') },
    { key: '/tasks', icon: <AppstoreOutlined />, label: 'AI Agent Settings', onClick: () => navigate('/tasks') },
    ...(user?.is_admin ? [{ key: '/llm-config', icon: <RobotOutlined />, label: 'LLM Settings', onClick: () => navigate('/llm-config') }] : []),
    { type: 'divider' as const },
    { key: '/scheduler', icon: <ControlOutlined />, label: 'Scheduler Management', onClick: () => navigate('/scheduler') },
    { key: '/scheduler/logs', icon: <HistoryOutlined />, label: 'Scheduler Logs', onClick: () => navigate('/scheduler/logs') },
    ...(user?.is_admin ? [{ key: '/system-logs', icon: <FileTextOutlined />, label: 'System Log Files', onClick: () => navigate('/system-logs') }] : []),
    {
      key: 'runtime-logs',
      icon: <CodeOutlined />,
      label: (
        <Badge dot={hasErrorLogs} size="small">
          <span>Runtime Logs</span>
        </Badge>
      ),
      onClick: () => globalLogger.actions.setShowFloating(true),
    },
    ...(user?.is_admin ? [{ key: '/users', icon: <TeamOutlined />, label: 'User Management', onClick: () => navigate('/users') }] : []),
    { type: 'divider' as const },
    { key: 'edit-profile', icon: <EditOutlined />, label: 'Edit Profile', onClick: openProfileModal },
    { key: 'logout', icon: <LogoutOutlined />, label: 'Logout', onClick: handleLogout },
  ];

  function handleMenuClick({ key }: { key: string }) {
    navigate(key);
  }

  function handleLogout() {
    logout();
    navigate('/login');
  }

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
            {process.env.REACT_APP_APP_NAME || 'GuanHe Capital'}
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
          {tasks.size > 0 && (
            <Popover
              content={<TaskProgressPopover tasks={tasks} themeMode={themeMode} />}
              placement="bottomRight"
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
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 8,
                  height: 32,
                  padding: '0 12px',
                  borderRadius: 6,
                  background: themeMode === 'dark' ? 'rgba(24, 144, 255, 0.1)' : 'rgba(24, 144, 255, 0.05)',
                  border: `1px solid ${themeMode === 'dark' ? 'rgba(24, 144, 255, 0.3)' : 'rgba(24, 144, 255, 0.2)'}`,
                  cursor: 'pointer',
                  transition: 'all 0.3s',
                }}
                onClick={() => navigate('/files')}
              >
                <SyncOutlined spin style={{ color: '#1890ff', fontSize: 14 }} />
                <span style={{ 
                  fontSize: 13, 
                  fontWeight: 500,
                  color: themeMode === 'dark' ? '#e0e6f1' : 'rgba(0, 0, 0, 0.85)' 
                }}>
                  {tasks.size} {tasks.size === 1 ? 'task' : 'tasks'}
                </span>
                <span style={{ 
                  fontSize: 12, 
                  color: themeMode === 'dark' ? '#94a3b8' : '#666'
                }}>
                  {Math.round(
                    Array.from(tasks.values())
                      .reduce((sum, task) => sum + task.status.progress, 0) / tasks.size
                  )}%
                </span>
              </div>
            </Popover>
          )}
          <span style={{ color: navTheme.headerTextSecondary, fontSize: 13 }}>Welcome, {user?.username}</span>

          <TimeDisplay />

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
            menu={{
              items: userMenuItems,
              selectedKeys: userMenuItems.some((i: { key?: string }) => i.key === location.pathname) ? [location.pathname] : [],
              style: { minWidth: 200 },
            }}
            placement="bottomRight"
          >
            <Button
              type="text"
              icon={
                user?.avatar_url
                  ? <Avatar size="small" src={user.avatar_url} />
                  : <Avatar size="small" icon={<UserOutlined />} />
              }
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
      
      {/* 浮动上传按钮：收起时左侧始终露出上传图标，悬停展开显示 "Upload" */}
      <div
        style={{
          position: 'fixed',
          left: floatingExpanded ? 0 : -10,
          top: '50%',
          transform: 'translateY(-50%)',
          transition: 'left 0.25s cubic-bezier(0.4, 0, 0.2, 1)',
          zIndex: 999,
        }}
        onMouseEnter={() => setFloatingExpanded(true)}
        onMouseLeave={() => setFloatingExpanded(false)}
      >
        <Button
          type="primary"
          size="large"
          icon={<UploadOutlined style={{ fontSize: floatingExpanded ? '20px' : '22px', transition: 'font-size 0.3s' }} />}
          onClick={() => setShowUploadOptions(true)}
          style={{
            width: floatingExpanded ? 140 : 48,
            height: 48,
            borderRadius: '0 24px 24px 0',
            display: 'flex',
            alignItems: 'center',
            justifyContent: floatingExpanded ? 'flex-start' : 'center',
            paddingLeft: floatingExpanded ? 24 : 0,
            transition: 'all 0.25s cubic-bezier(0.4, 0, 0.2, 1)',
            background: themeMode === 'dark' ? 'linear-gradient(135deg, #1890ff 0%, #096dd9 100%)' : 'linear-gradient(135deg, #40a9ff 0%, #1890ff 100%)',
            border: 'none',
            boxShadow: floatingExpanded
              ? '4px 4px 16px rgba(24, 144, 255, 0.4)'
              : '2px 2px 12px rgba(24, 144, 255, 0.35)',
          }}
        >
          {floatingExpanded && (
            <span style={{
              marginLeft: 10,
              fontSize: 15,
              fontWeight: 600,
              whiteSpace: 'nowrap',
            }}>
              Upload
            </span>
          )}
        </Button>
      </div>

      {/* 上传选项菜单 Modal - 弹出式菜单 */}
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
        styles={
          themeMode === 'dark'
            ? {
                content: {
                  background: '#1a2234',
                  border: '1px solid #364152',
                  borderRadius: '12px',
                  boxShadow: '0 6px 16px rgba(0,0,0,0.4)',
                },
                body: {
                  padding: '24px 16px',
                  background: '#1a2234',
                  color: '#e0e6f1',
                  borderRadius: '12px',
                },
              }
            : undefined
        }
        bodyStyle={
          themeMode === 'dark'
            ? undefined
            : {
                padding: '24px 16px',
                background: '#fff',
                borderRadius: '12px',
              }
        }
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
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
            <span style={{ marginLeft: '12px' }}>YouTube Link</span>
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
      
      {/* Edit Profile Modal */}
      <Modal
        title="Edit Profile"
        open={profileModalOpen}
        onCancel={() => setProfileModalOpen(false)}
        onOk={handleProfileSave}
        okText="Save"
        confirmLoading={profileSaving}
        width={400}
        centered
        styles={
          themeMode === 'dark'
            ? {
                content: { background: '#1a2234', border: '1px solid #364152' },
                header: { background: '#1a2234', color: '#e0e6f1' },
                body: { background: '#1a2234', color: '#e0e6f1' },
                footer: { background: '#1a2234', borderTop: '1px solid #364152' },
              }
            : undefined
        }
      >
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 20, padding: '16px 0' }}>
          <div
            style={{ position: 'relative', cursor: 'pointer' }}
            onClick={() => avatarInputRef.current?.click()}
          >
            {profileAvatarUrl ? (
              <Avatar size={80} src={profileAvatarUrl} />
            ) : (
              <Avatar size={80} icon={<UserOutlined />} style={{ backgroundColor: '#1890ff' }} />
            )}
            <div
              style={{
                position: 'absolute',
                bottom: 0,
                right: 0,
                background: themeMode === 'dark' ? '#364152' : '#fff',
                borderRadius: '50%',
                width: 28,
                height: 28,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                boxShadow: '0 2px 6px rgba(0,0,0,0.2)',
                border: `2px solid ${themeMode === 'dark' ? '#1a2234' : '#fff'}`,
              }}
            >
              {avatarUploading ? <SyncOutlined spin style={{ fontSize: 14, color: '#1890ff' }} /> : <CameraOutlined style={{ fontSize: 14, color: themeMode === 'dark' ? '#e0e6f1' : '#666' }} />}
            </div>
            <input
              ref={avatarInputRef}
              type="file"
              accept="image/jpeg,image/png,image/gif,image/webp"
              style={{ display: 'none' }}
              onChange={handleAvatarUpload}
            />
          </div>
          <div style={{ width: '100%' }}>
            <div style={{ marginBottom: 8, fontWeight: 500, color: themeMode === 'dark' ? '#e0e6f1' : undefined }}>Username</div>
            <Input
              value={profileUsername}
              onChange={e => setProfileUsername(e.target.value)}
              maxLength={50}
              style={themeMode === 'dark' ? { background: '#212936', borderColor: '#364152', color: '#e0e6f1' } : undefined}
            />
          </div>
          <div style={{ width: '100%', borderTop: `1px solid ${themeMode === 'dark' ? '#364152' : '#f0f0f0'}`, paddingTop: 16 }}>
            <div style={{ marginBottom: 12, fontWeight: 500, fontSize: 13, color: themeMode === 'dark' ? '#94a3b8' : '#999' }}>Change Password (optional)</div>
            <div style={{ marginBottom: 8, fontWeight: 500, color: themeMode === 'dark' ? '#e0e6f1' : undefined }}>New Password</div>
            <Input.Password
              value={newPassword}
              onChange={e => setNewPassword(e.target.value)}
              placeholder="Leave empty to keep current"
              style={themeMode === 'dark' ? { background: '#212936', borderColor: '#364152', color: '#e0e6f1' } : undefined}
            />
            {newPassword && (
              <div style={{ marginTop: 12 }}>
                <div style={{ marginBottom: 8, fontWeight: 500, color: themeMode === 'dark' ? '#e0e6f1' : undefined }}>Confirm Password</div>
                <Input.Password
                  value={confirmPassword}
                  onChange={e => setConfirmPassword(e.target.value)}
                  placeholder="Re-enter new password"
                  status={confirmPassword && confirmPassword !== newPassword ? 'error' : undefined}
                  style={themeMode === 'dark' ? { background: '#212936', borderColor: confirmPassword && confirmPassword !== newPassword ? undefined : '#364152', color: '#e0e6f1' } : undefined}
                />
              </div>
            )}
          </div>
        </div>
      </Modal>

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
import React, { useState, useEffect } from 'react';
import type { MenuProps } from 'antd';
import { Layout as AntLayout, Button, Dropdown, Avatar, Switch } from 'antd';
import {
  LogoutOutlined,
  EditOutlined,
  KeyOutlined,
  SunOutlined,
  MoonOutlined,
  LineChartOutlined,
} from '@ant-design/icons';
import { Outlet, useNavigate } from 'react-router-dom';
import { useAuthStore } from '../store/authStore';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import UserProfileModal from './UserProfileModal';
import MobileBottomNav from './mobile/navigation/MobileBottomNav';
import MobileTaskProgressBar, { MOBILE_TASK_PROGRESS_BAR_HEIGHT } from './mobile/MobileTaskProgressBar';
import { userAvatarLetter, USER_AVATAR_LETTER_BG } from '../utils/userAvatar';

const { Header, Content } = AntLayout;

const MOBILE_SHELL_THEME = {
  light: {
    layoutBg: '#f5f5f5',
    headerBg: '#ffffff',
    headerBorder: '#f0f0f0',
    titleColor: 'rgba(0, 0, 0, 0.88)',
    headerShadow: undefined as string | undefined,
    avatarBtnColor: undefined as string | undefined,
  },
  dark: {
    layoutBg: '#1a202c',
    headerBg: '#1a2234',
    headerBorder: '#364152',
    titleColor: '#e0e6f1',
    headerShadow: '0 1px 4px rgba(0,0,0,0.2)',
    avatarBtnColor: '#e0e6f1',
  },
} as const;

/**
 * 移动端布局组件
 * 顶部栏 + 内容区 + 底部导航
 */
const MobileLayout: React.FC = () => {
  const navigate = useNavigate();
  const { user, logout } = useAuthStore();
  const { tasks } = useFileProcessing();
  const { themeMode, setThemeMode } = useDashboardSettings();
  const [profileModalOpen, setProfileModalOpen] = useState(false);
  const showTaskProgressBar = tasks.size > 0;
  const shell = MOBILE_SHELL_THEME[themeMode];

  useEffect(() => {
    const root = document.querySelector('.App');
    if (root) {
      root.classList.toggle('app-dark', themeMode === 'dark');
    }
    return () => {
      root?.classList.remove('app-dark');
    };
  }, [themeMode]);

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  // 移动端头像菜单：不提供 PC 端那套配置入口；管理员可进入 Monitoring（路由仍由 AdminRoute 校验）
  const userMenuItems = [
    {
      key: 'theme',
      icon: themeMode === 'dark' ? <MoonOutlined /> : <SunOutlined />,
      label: (
        <span
          onClick={(e) => e.stopPropagation()}
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            width: '100%',
            gap: 8,
          }}
        >
          <span>Mode</span>
          <Switch
            size="small"
            checked={themeMode === 'dark'}
            onChange={(checked) => setThemeMode(checked ? 'dark' : 'light')}
          />
        </span>
      ),
      onClick: (info: Parameters<NonNullable<MenuProps['onClick']>>[0]) => info.domEvent.stopPropagation(),
    },
    { type: 'divider' as const },
    {
      key: '/settings/api-keys',
      icon: <KeyOutlined />,
      label: 'API Keys',
      onClick: () => navigate('/settings/api-keys'),
    },
    ...(user?.is_admin
      ? [{ key: '/monitoring', icon: <LineChartOutlined />, label: 'Monitoring', onClick: () => navigate('/monitoring') }]
      : []),
    ...(user?.is_admin ? [{ type: 'divider' as const }] : []),
    {
      key: 'edit-profile',
      icon: <EditOutlined />,
      label: 'Edit Profile',
      onClick: () => setProfileModalOpen(true),
    },
    {
      key: 'logout',
      icon: <LogoutOutlined />,
      label: 'Logout',
      onClick: handleLogout
    }
  ];

  return (
    <AntLayout style={{ minHeight: '100vh', backgroundColor: shell.layoutBg }}>
      <Header
        style={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          zIndex: 999,
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          padding: '0 16px',
          backgroundColor: shell.headerBg,
          borderBottom: `1px solid ${shell.headerBorder}`,
          boxShadow: shell.headerShadow,
          height: '56px',
          lineHeight: '56px',
        }}
      >
        <span
          style={{
            fontFamily: "'Aladin', cursive",
            fontSize: 20,
            fontWeight: 400,
            letterSpacing: '0.02em',
            color: shell.titleColor,
            lineHeight: '56px',
          }}
        >
          GuanHe Daily
        </span>

        <Dropdown
          menu={{ items: userMenuItems, style: { minWidth: 200 } }}
          placement="bottomRight"
        >
          <Button
            type="text"
            icon={
              user?.avatar_url ? (
                <Avatar size="small" src={user.avatar_url} />
              ) : (
                <Avatar size="small" style={{ backgroundColor: USER_AVATAR_LETTER_BG, fontSize: 12 }}>
                  {userAvatarLetter(user?.username)}
                </Avatar>
              )
            }
            style={{
              display: 'flex',
              alignItems: 'center',
              ...(shell.avatarBtnColor ? { color: shell.avatarBtnColor } : {}),
            }}
          />
        </Dropdown>
      </Header>

      <UserProfileModal
        open={profileModalOpen}
        onClose={() => setProfileModalOpen(false)}
        themeMode={themeMode}
      />

      <MobileTaskProgressBar />

      <Content
        style={{
          marginTop: showTaskProgressBar ? 56 + MOBILE_TASK_PROGRESS_BAR_HEIGHT : 56,
          marginBottom: '60px',
          minHeight: showTaskProgressBar
            ? `calc(100vh - ${116 + MOBILE_TASK_PROGRESS_BAR_HEIGHT}px)`
            : 'calc(100vh - 116px)',
          backgroundColor: shell.layoutBg,
        }}
      >
        <Outlet />
      </Content>

      <MobileBottomNav />
    </AntLayout>
  );
};

export default MobileLayout;

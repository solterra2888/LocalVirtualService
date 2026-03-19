import React, { useState } from 'react';
import { Layout as AntLayout, Button, Dropdown, Avatar, Typography, Drawer, Menu } from 'antd';
import {
  UserOutlined,
  LogoutOutlined,
  MenuOutlined,
  SettingOutlined,
  BuildOutlined,
  FundOutlined,
  AppstoreOutlined,
  ControlOutlined,
  TeamOutlined,
  RobotOutlined,
  HistoryOutlined,
  BarChartOutlined
} from '@ant-design/icons';
import { Outlet, useNavigate } from 'react-router-dom';
import { useAuthStore } from '../store/authStore';
import MobileBottomNav from './mobile/navigation/MobileBottomNav';

const { Header, Content } = AntLayout;
const { Title } = Typography;

/**
 * 移动端布局组件
 * 顶部栏 + 内容区 + 底部导航
 */
const MobileLayout: React.FC = () => {
  const navigate = useNavigate();
  const { user, logout } = useAuthStore();
  const [drawerVisible, setDrawerVisible] = useState(false);

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  // 更多功能菜单项
  const moreMenuItems = [
    {
      key: 'market-weekly',
      icon: <BarChartOutlined />,
      label: 'Market Weekly',
      onClick: () => {
        navigate('/market-weekly');
        setDrawerVisible(false);
      }
    },
    {
      key: 'prompt-templates',
      icon: <BuildOutlined />,
      label: 'Prompt Templates',
      onClick: () => {
        navigate('/prompt-templates');
        setDrawerVisible(false);
      }
    },
    {
      key: 'investment-themes',
      icon: <FundOutlined />,
      label: 'Investment Themes',
      onClick: () => {
        navigate('/investment-themes');
        setDrawerVisible(false);
      }
    },
    {
      key: 'tasks',
      icon: <AppstoreOutlined />,
      label: 'AI Agent Settings',
      onClick: () => {
        navigate('/tasks');
        setDrawerVisible(false);
      }
    },
    {
      key: 'scheduler',
      icon: <ControlOutlined />,
      label: 'Scheduler',
      onClick: () => {
        navigate('/scheduler');
        setDrawerVisible(false);
      }
    },
    {
      key: 'scheduler-logs',
      icon: <HistoryOutlined />,
      label: 'Scheduler Logs',
      onClick: () => {
        navigate('/scheduler/logs');
        setDrawerVisible(false);
      }
    },
    ...(user?.is_admin ? [
      {
        key: 'llm-config',
        icon: <RobotOutlined />,
        label: 'LLM Settings',
        onClick: () => {
          navigate('/llm-config');
          setDrawerVisible(false);
        }
      },
      {
        key: 'users',
        icon: <TeamOutlined />,
        label: 'User Management',
        onClick: () => {
          navigate('/users');
          setDrawerVisible(false);
        }
      }
    ] : [])
  ];

  // 用户菜单
  const userMenuItems = [
    {
      key: 'settings',
      icon: <SettingOutlined />,
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

  return (
    <AntLayout style={{ minHeight: '100vh', backgroundColor: '#f5f5f5' }}>
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
          backgroundColor: '#ffffff',
          borderBottom: '1px solid #f0f0f0',
          height: '56px',
          lineHeight: '56px',
        }}
      >
        <Title
          level={5}
          style={{
            margin: 0,
            fontSize: '20px',
            fontWeight: 500,
            fontFamily: "'Aladin', cursive",
            letterSpacing: '0.02em',
          }}
        >
          {process.env.REACT_APP_APP_NAME || 'GuanHe Capital'}
        </Title>

        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <Button
            type="text"
            icon={<MenuOutlined />}
            onClick={() => setDrawerVisible(true)}
            style={{ fontSize: '20px' }}
          />
          
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

      <Content
        style={{
          marginTop: '56px',
          marginBottom: '60px',
          minHeight: 'calc(100vh - 116px)',
          backgroundColor: '#f5f5f5',
        }}
      >
        <Outlet />
      </Content>

      <MobileBottomNav />

      {/* 更多功能抽屉 */}
      <Drawer
        title="More Features"
        placement="right"
        onClose={() => setDrawerVisible(false)}
        open={drawerVisible}
        width="80%"
      >
        <Menu
          mode="inline"
          items={moreMenuItems}
          style={{ border: 'none' }}
        />
      </Drawer>
    </AntLayout>
  );
};

export default MobileLayout;


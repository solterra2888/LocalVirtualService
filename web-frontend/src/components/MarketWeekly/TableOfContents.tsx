import React, { useState, useEffect } from 'react';
import { Menu, Drawer, Button } from 'antd';
import { MenuOutlined, CloseOutlined, LeftOutlined, RightOutlined } from '@ant-design/icons';
import type { MenuProps } from 'antd';
import './TableOfContents.css';

interface TableOfContentsProps {
  className?: string;
}

type MenuItem = Required<MenuProps>['items'][number];

const TableOfContents: React.FC<TableOfContentsProps> = ({ className }) => {
  const [activeSection, setActiveSection] = useState<string>('executive-summary');
  const [openKeys, setOpenKeys] = useState<string[]>(['weekly-performance', 'technical-views', 'next-week-outlook']);
  const [drawerVisible, setDrawerVisible] = useState<boolean>(false);
  const [collapsed, setCollapsed] = useState<boolean>(false);

  // 监听页面滚动，高亮当前section
  useEffect(() => {
    const handleScroll = () => {
      // 查找所有带ID的section和subsection元素
      const sections = document.querySelectorAll('section[id], div.subsection[id]');
      let currentSection = 'executive-summary';
      let closestSection = null;
      let closestDistance = Infinity;

      sections.forEach((section) => {
        const rect = section.getBoundingClientRect();
        // 计算section顶部到视口导航栏下方（150px）的距离
        const distance = Math.abs(rect.top - 150);
        
        // 如果section在视口中且距离更近，则更新当前section
        if (rect.top <= 150 && rect.bottom >= 0) {
          if (distance < closestDistance) {
            closestDistance = distance;
            closestSection = section.id;
          }
        }
      });

      if (closestSection) {
        currentSection = closestSection;
      }

      setActiveSection(currentSection);
    };

    window.addEventListener('scroll', handleScroll);
    handleScroll(); // 初始化

    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  // 平滑滚动到指定section
  const scrollToSection = (sectionId: string) => {
    const element = document.getElementById(sectionId);
    if (element) {
      const offsetTop = element.offsetTop - 80; // 减去导航栏高度
      window.scrollTo({
        top: offsetTop,
        behavior: 'smooth'
      });
      setDrawerVisible(false); // 关闭抽屉
    }
  };

  // 统一处理菜单点击事件
  const handleMenuClick = ({ key }: { key: string }) => {
    scrollToSection(key);
  };

  const items: MenuItem[] = [
    {
      key: 'executive-summary',
      label: 'Executive Summary'
    },
    {
      key: 'weekly-performance',
      label: 'Weekly Performance',
      children: [
        { key: 'equity-weekly', label: 'Equity Indices' },
        { key: 'sectors-weekly', label: 'US Sectors' },
        { key: 'forex-weekly', label: 'Foreign Exchange' },
        { key: 'commodities-weekly', label: 'Commodities' },
        { key: 'yields-weekly', label: 'Treasury Yields' }
      ]
    },
    {
      key: 'technical-views',
      label: 'Technical Views',
      children: [
        { key: 'equity-technical', label: 'Equity Indices' },
        { key: 'sectors-technical', label: 'US Sectors' },
        { key: 'forex-technical', label: 'Foreign Exchange' },
        { key: 'commodities-technical', label: 'Commodities' },
        { key: 'yields-technical', label: 'Treasury Yields' }
      ]
    },
    {
      key: 'next-week-outlook',
      label: 'Next Week Outlook',
      children: [
        { key: 'economic-calendar', label: 'Economic Calendar' },
        { key: 'earnings-calendar', label: 'Earnings Calendar' },
        { key: 'ipo-calendar', label: 'IPO Calendar' }
      ]
    }
  ];

  const tocMenu = (
    <>
      <div className="toc-header">CONTENTS</div>
      <Menu
        mode="inline"
        selectedKeys={[activeSection]}
        openKeys={openKeys}
        onOpenChange={setOpenKeys}
        onClick={handleMenuClick}
        items={items}
        className="toc-menu"
      />
    </>
  );

  return (
    <>
      {/* 右侧固定目录 (宽屏显示) */}
      <div className={`market-weekly-toc market-weekly-toc-desktop ${collapsed ? 'toc-collapsed' : ''} ${className || ''}`}>
        {!collapsed && tocMenu}
        <Button
          className="toc-toggle-button"
          type="text"
          size="small"
          icon={collapsed ? <LeftOutlined /> : <RightOutlined />}
          onClick={() => setCollapsed(!collapsed)}
          title={collapsed ? '展开目录' : '收缩目录'}
        />
      </div>

      {/* 浮动按钮 (窄屏显示) */}
      <Button
        className="toc-floating-button"
        type="primary"
        shape="circle"
        icon={<MenuOutlined />}
        size="large"
        onClick={() => setDrawerVisible(true)}
      />

      {/* 抽屉式目录 (窄屏点击打开) */}
      <Drawer
        title="CONTENTS"
        placement="right"
        onClose={() => setDrawerVisible(false)}
        open={drawerVisible}
        className="toc-drawer"
        width={240}
        closeIcon={<CloseOutlined />}
      >
        <Menu
          mode="inline"
          selectedKeys={[activeSection]}
          openKeys={openKeys}
          onOpenChange={setOpenKeys}
          onClick={handleMenuClick}
          items={items}
          className="toc-menu"
        />
      </Drawer>
    </>
  );
};

export default TableOfContents;

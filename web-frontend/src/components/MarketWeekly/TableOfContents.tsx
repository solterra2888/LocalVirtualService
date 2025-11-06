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
      const sections = document.querySelectorAll('section[id]');
      let currentSection = 'executive-summary';

      sections.forEach((section) => {
        const rect = section.getBoundingClientRect();
        // 如果section在视口的上半部分，认为是当前section
        if (rect.top <= 150 && rect.bottom >= 150) {
          currentSection = section.id;
        }
      });

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

  const items: MenuItem[] = [
    {
      key: 'executive-summary',
      label: 'Executive Summary',
      onClick: () => scrollToSection('executive-summary')
    },
    {
      key: 'weekly-performance',
      label: 'Weekly Performance',
      onClick: () => scrollToSection('weekly-performance'),
      children: [
        { key: 'equity-weekly', label: 'Equity Indices', onClick: () => scrollToSection('equity-weekly') },
        { key: 'sectors-weekly', label: 'US Sectors', onClick: () => scrollToSection('sectors-weekly') },
        { key: 'forex-weekly', label: 'Foreign Exchange', onClick: () => scrollToSection('forex-weekly') },
        { key: 'commodities-weekly', label: 'Commodities', onClick: () => scrollToSection('commodities-weekly') },
        { key: 'yields-weekly', label: 'Treasury Yields', onClick: () => scrollToSection('yields-weekly') }
      ]
    },
    {
      key: 'technical-views',
      label: 'Technical Views',
      onClick: () => scrollToSection('technical-views'),
      children: [
        { key: 'equity-technical', label: 'Equity Indices', onClick: () => scrollToSection('equity-technical') },
        { key: 'sectors-technical', label: 'US Sectors', onClick: () => scrollToSection('sectors-technical') },
        { key: 'watchlist-technical', label: 'My Watchlist', onClick: () => scrollToSection('watchlist-technical') },
        { key: 'forex-technical', label: 'Foreign Exchange', onClick: () => scrollToSection('forex-technical') },
        { key: 'commodities-technical', label: 'Commodities', onClick: () => scrollToSection('commodities-technical') },
        { key: 'yields-technical', label: 'Treasury Yields', onClick: () => scrollToSection('yields-technical') }
      ]
    },
    {
      key: 'next-week-outlook',
      label: 'Next Week Outlook',
      onClick: () => scrollToSection('next-week-outlook'),
      children: [
        { key: 'economic-calendar', label: 'Economic Calendar', onClick: () => scrollToSection('economic-calendar') },
        { key: 'earnings-calendar', label: 'Earnings Calendar', onClick: () => scrollToSection('earnings-calendar') },
        { key: 'ipo-calendar', label: 'IPO Calendar', onClick: () => scrollToSection('ipo-calendar') }
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
          items={items}
          className="toc-menu"
        />
      </Drawer>
    </>
  );
};

export default TableOfContents;

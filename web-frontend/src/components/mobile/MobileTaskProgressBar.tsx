import React, { useMemo, useState } from 'react';
import { Button, Drawer } from 'antd';
import { SyncOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { useFileProcessing } from '../../contexts/FileProcessingContext';
import { useDashboardSettings } from '../../contexts/DashboardSettingsContext';
import { TaskProgressPopover } from '../TaskProgressPopover';
import { MODAL_DARK_BG, MODAL_DARK_BORDER } from '../../utils/modalStyles';

const BAR_HEIGHT = 38;

/** 与 PC NavBar 任务条数据一致；手机端为顶栏下固定条 + 点击打开底部 Drawer */
const MobileTaskProgressBar: React.FC = () => {
  const navigate = useNavigate();
  const { themeMode } = useDashboardSettings();
  const { tasks } = useFileProcessing();
  const [drawerOpen, setDrawerOpen] = useState(false);

  const isDark = themeMode === 'dark';

  const avgProgress = useMemo(() => {
    if (tasks.size === 0) return 0;
    return Math.round(
      Array.from(tasks.values()).reduce((sum, t) => sum + t.status.progress, 0) / tasks.size
    );
  }, [tasks]);

  if (tasks.size === 0) return null;

  const barStyle: React.CSSProperties = {
    position: 'fixed',
    top: 56,
    left: 0,
    right: 0,
    zIndex: 998,
    height: BAR_HEIGHT,
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    padding: '0 12px',
    borderBottom: `1px solid ${isDark ? 'rgba(24, 144, 255, 0.25)' : 'rgba(24, 144, 255, 0.15)'}`,
    background: isDark ? 'rgba(24, 144, 255, 0.12)' : 'rgba(24, 144, 255, 0.06)',
    cursor: 'pointer',
    WebkitTapHighlightColor: 'transparent',
  };

  return (
    <>
      <div
        role="button"
        tabIndex={0}
        aria-label={`${tasks.size} processing tasks, ${avgProgress}% average progress. Tap for details.`}
        style={barStyle}
        onClick={() => setDrawerOpen(true)}
        onKeyDown={e => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            setDrawerOpen(true);
          }
        }}
      >
        <SyncOutlined spin style={{ color: '#1890ff', fontSize: 14, flexShrink: 0 }} />
        <span
          style={{
            fontSize: 13,
            fontWeight: 500,
            color: '#1890ff',
            flex: 1,
            minWidth: 0,
            whiteSpace: 'nowrap',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
          }}
        >
          {tasks.size} {tasks.size === 1 ? 'task' : 'tasks'}
        </span>
        <span style={{ fontSize: 12, color: isDark ? '#94a3b8' : '#666', flexShrink: 0 }}>{avgProgress}%</span>
      </div>

      <Drawer
        title="Processing Tasks"
        placement="bottom"
        height="auto"
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        destroyOnClose
        styles={{
          header: isDark ? { background: MODAL_DARK_BG, borderBottom: '1px solid rgba(255,255,255,0.06)' } : undefined,
          body: {
            padding: 0,
            maxHeight: 'min(520px, 70vh)',
            overflow: 'auto',
            background: isDark ? MODAL_DARK_BG : undefined,
          },
          content: isDark ? { background: MODAL_DARK_BG } : undefined,
        }}
      >
        <TaskProgressPopover tasks={tasks} themeMode={themeMode} mobileLayout />
        <div
          style={{
            padding: '10px 12px 14px',
            borderTop: `1px solid ${isDark ? MODAL_DARK_BORDER : '#f0f0f0'}`,
            background: isDark ? MODAL_DARK_BG : '#fafafa',
          }}
        >
          <Button
            type="link"
            block
            size="small"
            onClick={() => {
              setDrawerOpen(false);
              navigate('/files');
            }}
            style={{ fontSize: 13 }}
          >
            Go to Sources
          </Button>
        </div>
      </Drawer>
    </>
  );
};

export const MOBILE_TASK_PROGRESS_BAR_HEIGHT = BAR_HEIGHT;
export default MobileTaskProgressBar;

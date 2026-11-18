/* =========================================================================
   移动版 Scheduler Management（admin 专属，路由由 AdminRoute 守卫）
   与桌面 SchedulerPage 同口径：数据源卡片视图 + 批量启停 + 关联任务明细 +
   调度配置（复用 DataSourceConfigModal）+ 直跑快捷操作 + 执行历史入口。
   排序遵循用户已保存的卡片顺序（只读，移动端不提供拖拽）。
   ========================================================================= */
import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { Button, Segmented, Switch, Tag, message, Modal, Spin, Typography } from 'antd';
import {
  ReloadOutlined,
  RightOutlined,
  LineChartOutlined,
  SettingOutlined,
  PlayCircleOutlined,
  ClockCircleOutlined,
  CheckCircleOutlined,
  SyncOutlined,
  ExclamationCircleOutlined,
  CloudDownloadOutlined,
  FileTextOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { useNavigate } from 'react-router-dom';
import { api } from '../services/api';
import { schedulerAPI, ScheduledTask } from '../services/scheduler';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import DataSourceConfigModal from '../components/DataSourceConfigModal';
import type { DataSourceConfigType } from '../components/DataSourceConfigModal';
import {
  CONFIGURABLE_SOURCE_IDS,
  INITIAL_SCHEDULER_DATA_SOURCES,
  getDefaultCategoryOrders,
  isTaskRelatedToSource,
  mergeCategoryOrder,
  type DataSource,
  type QuickAction,
  type SchedulerCategory,
} from '../config/schedulerSources';
import './MobileSchedulerPage.css';

const { Text } = Typography;

const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#ffffff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    subBg: '#f6f8fa',
    primary: '#1890ff',
  },
  dark: {
    pageBg: '#1a202c',
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#e0e6f1',
    textSecondary: '#A0A3AD',
    subBg: 'rgba(255, 255, 255, 0.04)',
    primary: '#2196F3',
  },
} as const;

const CATEGORY_LABELS: Record<SchedulerCategory, string> = {
  data_fetch: '数据获取',
  report_generation: '报告生成',
};

/** Configure 类快捷动作判定（与桌面 showQuickActionModal 同口径） */
function isConfigureAction(action: QuickAction): boolean {
  return (
    (action.id.includes('configure') && action.id.includes('schedule')) ||
    action.id === 'update_fundamentals_data'
  );
}

function scheduleSummary(task: ScheduledTask): string {
  if (task.schedule_type === 'interval') {
    return `every ${task.interval_minutes ?? '?'}m`;
  }
  return task.cron_expression || '-';
}

function formatTime(value?: string): string {
  if (!value) return '-';
  const d = dayjs(value);
  return d.isValid() ? d.format('MM-DD HH:mm') : '-';
}

function taskStatusTag(status: string | undefined) {
  const config =
    status === 'running'
      ? { color: 'processing', icon: <SyncOutlined spin /> }
      : status === 'error' || status === 'failed'
        ? { color: 'error', icon: <ExclamationCircleOutlined /> }
        : { color: 'default', icon: <ClockCircleOutlined /> };
  return (
    <Tag color={config.color} icon={config.icon} style={{ marginRight: 0 }}>
      {status === 'running' ? 'Running' : status === 'error' || status === 'failed' ? 'Error' : 'Idle'}
    </Tag>
  );
}

const MobileSchedulerPage: React.FC = () => {
  const navigate = useNavigate();
  const { themeMode } = useDashboardSettings();
  const colors = THEME_COLORS[themeMode];

  const [scheduledTasks, setScheduledTasks] = useState<ScheduledTask[]>([]);
  const [loading, setLoading] = useState(true);
  const [categoryOrder, setCategoryOrder] = useState<Record<SchedulerCategory, string[]>>(() =>
    getDefaultCategoryOrders(INITIAL_SCHEDULER_DATA_SOURCES)
  );
  const [filter, setFilter] = useState<'all' | SchedulerCategory>('all');
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [togglingSourceIds, setTogglingSourceIds] = useState<Set<string>>(new Set());
  const [togglingTaskIds, setTogglingTaskIds] = useState<Set<string>>(new Set());
  const [runningActionIds, setRunningActionIds] = useState<Set<string>>(new Set());
  const [configSource, setConfigSource] = useState<DataSource | null>(null);

  const loadScheduledTasks = useCallback(async () => {
    try {
      setLoading(true);
      const response = await schedulerAPI.getTasks();
      if (response.success && response.data) {
        // futu_themes 为系统内部任务，与桌面版同口径过滤
        setScheduledTasks(response.data.filter((task) => task.type !== 'futu_themes'));
      } else {
        message.error(response.message);
      }
    } catch (error) {
      console.error('Failed to load scheduled tasks:', error);
      message.error('Failed to load scheduled tasks');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadScheduledTasks();
  }, [loadScheduledTasks]);

  // 拉取用户已保存的卡片排序偏好（与桌面跨设备同步；移动端只读不拖拽）
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const remote = await schedulerAPI.getCardOrder();
      if (cancelled) return;
      if (!remote.data_fetch?.length && !remote.report_generation?.length) return;
      setCategoryOrder(() => {
        const defaults = getDefaultCategoryOrders(INITIAL_SCHEDULER_DATA_SOURCES);
        return {
          data_fetch: mergeCategoryOrder(remote.data_fetch, defaults.data_fetch),
          report_generation: mergeCategoryOrder(remote.report_generation, defaults.report_generation),
        };
      });
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const getRelatedTasks = useCallback(
    (sourceId: string): ScheduledTask[] =>
      scheduledTasks.filter((task) => isTaskRelatedToSource(sourceId, task.type)),
    [scheduledTasks]
  );

  /** 与桌面 syncDataSourceStatus 同口径：有关联任务时取 some(enabled)，无则保持静态默认 */
  const isSourceEnabled = useCallback(
    (source: DataSource): boolean => {
      if (source.id === 'eodhd_fundamentals') return true; // 仅手动触发，开关恒开
      const related = getRelatedTasks(source.id);
      return related.length > 0 ? related.some((task) => task.enabled) : source.enabled;
    },
    [getRelatedTasks]
  );

  /** 批量启停（与桌面 toggleDataSource 同口径） */
  const handleToggleSource = async (source: DataSource, enabled: boolean) => {
    if (source.id === 'eodhd_fundamentals') {
      message.info('EODHD Fundamentals only supports manual trigger updates, cannot be controlled by switch');
      return;
    }
    const related = getRelatedTasks(source.id);
    if (related.length === 0) {
      message.warning(`No scheduled tasks found for ${source.id}, please configure data fetch strategy first`);
      return;
    }

    setTogglingSourceIds((prev) => new Set(prev).add(source.id));
    try {
      let successCount = 0;
      let errorCount = 0;
      for (const task of related) {
        try {
          const result = enabled
            ? await schedulerAPI.enableTask(task.id)
            : await schedulerAPI.disableTask(task.id);
          if (result.success) {
            successCount++;
          } else {
            errorCount++;
          }
        } catch (error) {
          errorCount++;
          console.error(`Task ${task.id} operation error:`, error);
        }
      }

      if (successCount > 0) {
        await loadScheduledTasks();
        const messageText = `${enabled ? 'Enabled' : 'Disabled'} successfully: ${successCount} tasks`;
        message.success(errorCount > 0 ? `${messageText}, ${errorCount} tasks failed` : messageText);
      } else {
        message.error(`Operation failed: all ${related.length} tasks cannot be ${enabled ? 'enabled' : 'disabled'}`);
      }
    } finally {
      setTogglingSourceIds((prev) => {
        const next = new Set(prev);
        next.delete(source.id);
        return next;
      });
    }
  };

  /** 单任务启停 */
  const handleToggleTask = async (task: ScheduledTask, enabled: boolean) => {
    setTogglingTaskIds((prev) => new Set(prev).add(task.id));
    try {
      const result = enabled ? await schedulerAPI.enableTask(task.id) : await schedulerAPI.disableTask(task.id);
      if (result.success) {
        message.success(`${enabled ? 'Enabled' : 'Disabled'}: ${task.name}`);
        await loadScheduledTasks();
      } else {
        message.error(result.message || 'Operation failed');
      }
    } catch (error) {
      console.error('Failed to toggle task:', error);
      message.error('Operation failed');
    } finally {
      setTogglingTaskIds((prev) => {
        const next = new Set(prev);
        next.delete(task.id);
        return next;
      });
    }
  };

  /** 直跑类快捷操作（与桌面 executeQuickAction 同口径，移动端加确认弹窗防误触） */
  const handleRunAction = (action: QuickAction, source: DataSource) => {
    Modal.confirm({
      title: action.name,
      content: action.description,
      okText: 'Run',
      cancelText: 'Cancel',
      onOk: async () => {
        setRunningActionIds((prev) => new Set(prev).add(action.id));
        try {
          const response = await api.post(action.endpoint, action.params || {});
          message.success((response as { message?: string }).message || 'Operation successful');
          await loadScheduledTasks();
        } catch (error: any) {
          console.error('Quick action error:', error);
          const errorMessage =
            error.response?.data?.detail || (error instanceof Error ? error.message : 'Operation failed');
          message.error(errorMessage);
        } finally {
          setRunningActionIds((prev) => {
            const next = new Set(prev);
            next.delete(action.id);
            return next;
          });
        }
      },
    });
  };

  /** 打开调度配置弹窗（复用 DataSourceConfigModal，白名单与桌面同口径） */
  const openConfig = (source: DataSource) => {
    if (!CONFIGURABLE_SOURCE_IDS.has(source.id)) {
      message.info('Data source configuration feature coming soon');
      return;
    }
    setConfigSource(source);
  };

  /** 预填用的既有任务（与桌面 getExistingScheduledTask 同口径） */
  const getExistingTask = useCallback(
    (sourceId: string): ScheduledTask | undefined => {
      const related = getRelatedTasks(sourceId);
      return related.find((task) => task.enabled && task.cron_expression) || related.find((task) => task.cron_expression);
    },
    [getRelatedTasks]
  );

  const visibleSources = useMemo(() => {
    const byId = new Map(INITIAL_SCHEDULER_DATA_SOURCES.map((s) => [s.id, s]));
    const ordered: DataSource[] = [
      ...categoryOrder.data_fetch,
      ...categoryOrder.report_generation,
    ]
      .map((id) => byId.get(id))
      .filter((s): s is DataSource => !!s);
    if (filter === 'all') return ordered;
    return ordered.filter((s) => s.category === filter);
  }, [categoryOrder, filter]);

  const enabledCount = INITIAL_SCHEDULER_DATA_SOURCES.filter((s) => isSourceEnabled(s)).length;
  const enabledTaskCount = scheduledTasks.filter((t) => t.enabled).length;

  const renderSourceCard = (source: DataSource) => {
    const related = getRelatedTasks(source.id);
    const enabled = isSourceEnabled(source);
    const expanded = expandedId === source.id;
    const configureAction = source.quickActions.find(isConfigureAction);
    const runActions = source.quickActions.filter((a) => !isConfigureAction(a));
    const existingTask = getExistingTask(source.id);
    const switchLoading = togglingSourceIds.has(source.id);

    return (
      <div
        key={source.id}
        className="msp-card"
        style={{ backgroundColor: colors.cardBg, borderColor: colors.cardBorder }}
      >
        <div
          className="msp-card-head"
          onClick={() => setExpandedId(expanded ? null : source.id)}
          role="button"
          aria-expanded={expanded}
        >
          <div className="msp-card-title-row">
            <div className="msp-card-name-wrap">
              {source.category === 'data_fetch' ? (
                <CloudDownloadOutlined style={{ color: colors.primary, fontSize: 15 }} />
              ) : (
                <FileTextOutlined style={{ color: '#722ed1', fontSize: 15 }} />
              )}
              <Text strong style={{ color: colors.text, fontSize: 14 }} ellipsis>
                {source.name}
              </Text>
            </div>
            <Switch
              size="small"
              checked={enabled}
              loading={switchLoading}
              disabled={source.id === 'eodhd_fundamentals'}
              onChange={(checked, e) => {
                e.stopPropagation();
                handleToggleSource(source, checked);
              }}
            />
          </div>
          <div className="msp-card-desc" style={{ color: colors.textSecondary }}>
            {source.description}
          </div>
          <div className="msp-card-meta">
            <Tag style={{ marginRight: 6 }}>{CATEGORY_LABELS[source.category]}</Tag>
            {related.length > 0 ? (
              <>
                <Tag color={enabled ? 'success' : 'default'} style={{ marginRight: 6 }}>
                  {related.length} task{related.length > 1 ? 's' : ''}
                </Tag>
                <span className="msp-cron-chip" style={{ color: colors.textSecondary }}>
                  <ClockCircleOutlined style={{ marginRight: 4 }} />
                  {scheduleSummary(related.find((t) => t.cron_expression) || related[0])}
                </span>
              </>
            ) : (
              <span className="msp-cron-chip" style={{ color: colors.textSecondary }}>
                Not scheduled
              </span>
            )}
            <RightOutlined
              style={{ color: colors.textSecondary, fontSize: 12, marginLeft: 'auto', transition: 'transform 0.2s', transform: expanded ? 'rotate(90deg)' : 'none' }}
            />
          </div>
        </div>

        {expanded && (
          <div className="msp-card-body" style={{ borderTop: `1px solid ${colors.cardBorder}` }}>
            {related.length === 0 && (
              <div className="msp-empty" style={{ color: colors.textSecondary }}>
                No scheduled tasks yet — tap Configure to create one.
              </div>
            )}
            {related.map((task) => (
              <div key={task.id} className="msp-task-row" style={{ backgroundColor: colors.subBg }}>
                <div className="msp-task-info">
                  <div className="msp-task-name" style={{ color: colors.text }}>
                    {task.name}
                  </div>
                  <div className="msp-task-meta" style={{ color: colors.textSecondary }}>
                    <span className="msp-task-chip">
                      <ClockCircleOutlined style={{ marginRight: 3 }} />
                      {scheduleSummary(task)}
                    </span>
                    <span className="msp-task-chip">{task.timezone}</span>
                    <span className="msp-task-chip">next {formatTime(task.next_run)}</span>
                    <span className="msp-task-chip msp-count-ok">
                      <CheckCircleOutlined style={{ marginRight: 3 }} />
                      {task.success_count}
                    </span>
                    <span className="msp-task-chip msp-count-fail">
                      <ExclamationCircleOutlined style={{ marginRight: 3 }} />
                      {task.fail_count}
                    </span>
                  </div>
                </div>
                <div className="msp-task-ops">
                  {taskStatusTag(task.status)}
                  <Switch
                    size="small"
                    checked={task.enabled}
                    loading={togglingTaskIds.has(task.id)}
                    onChange={(checked) => handleToggleTask(task, checked)}
                  />
                </div>
              </div>
            ))}

            <div className="msp-actions">
              {configureAction && (
                <Button
                  size="small"
                  icon={<SettingOutlined />}
                  onClick={() => openConfig(source)}
                  style={{ color: colors.primary, borderColor: colors.primary }}
                >
                  Configure
                </Button>
              )}
              {runActions.map((action) => (
                <Button
                  key={action.id}
                  size="small"
                  icon={<PlayCircleOutlined />}
                  loading={runningActionIds.has(action.id)}
                  onClick={() => handleRunAction(action, source)}
                >
                  Run
                </Button>
              ))}
            </div>
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="msp-page" style={{ backgroundColor: colors.pageBg, minHeight: '100%' }}>
      {/* 顶部：标题 + 操作 */}
      <div className="msp-header" style={{ backgroundColor: colors.cardBg, borderBottom: `1px solid ${colors.cardBorder}` }}>
        <div className="msp-header-title-row">
          <Text strong style={{ color: colors.text, fontSize: 16 }}>
            Scheduler Management
          </Text>
          <div className="msp-header-btns">
            <Button
              type="text"
              size="small"
              icon={<LineChartOutlined />}
              onClick={() => navigate('/monitoring?tab=scheduler')}
            >
              History
            </Button>
            <Button type="text" size="small" icon={<ReloadOutlined />} onClick={() => loadScheduledTasks()} loading={loading}>
              Refresh
            </Button>
          </div>
        </div>
        {/* 统计行 */}
        <div className="msp-stats">
          <div className="msp-stat">
            <span className="msp-stat-num" style={{ color: colors.primary }}>{INITIAL_SCHEDULER_DATA_SOURCES.length}</span>
            <span className="msp-stat-label" style={{ color: colors.textSecondary }}>Sources</span>
          </div>
          <div className="msp-stat">
            <span className="msp-stat-num" style={{ color: '#52c41a' }}>{enabledCount}</span>
            <span className="msp-stat-label" style={{ color: colors.textSecondary }}>Enabled</span>
          </div>
          <div className="msp-stat">
            <span className="msp-stat-num" style={{ color: '#722ed1' }}>{scheduledTasks.length}</span>
            <span className="msp-stat-label" style={{ color: colors.textSecondary }}>
              Tasks ({enabledTaskCount} on)
            </span>
          </div>
        </div>
        {/* 分类筛选 */}
        <Segmented
          block
          value={filter}
          onChange={(value) => setFilter(value as 'all' | SchedulerCategory)}
          options={[
            { value: 'all', label: 'All' },
            { value: 'data_fetch', label: '数据获取' },
            { value: 'report_generation', label: '报告生成' },
          ]}
        />
      </div>

      {/* 卡片列表 */}
      <div className="msp-list">
        {loading && scheduledTasks.length === 0 ? (
          <div className="msp-loading">
            <Spin />
          </div>
        ) : (
          visibleSources.map(renderSourceCard)
        )}
      </div>

      {/* 调度配置弹窗（复用桌面组件） */}
      {configSource && (
        <DataSourceConfigModal
          open
          onCancel={() => setConfigSource(null)}
          onSuccess={() => {
            setConfigSource(null);
            loadScheduledTasks();
          }}
          dataSourceType={configSource.id as DataSourceConfigType}
          dataSourceName={configSource.name}
          initialCronExpression={getExistingTask(configSource.id)?.cron_expression}
          initialTimezone={getExistingTask(configSource.id)?.timezone}
        />
      )}
    </div>
  );
};

export default MobileSchedulerPage;

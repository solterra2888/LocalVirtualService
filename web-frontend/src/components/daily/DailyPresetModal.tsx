/**
 * Daily Preset 配置弹窗.
 *
 * 用户在 Daily 页面通过齿轮图标打开本弹窗, 多选 source_subtype 后保存.
 * 语义约定 (与 backend/api/daily.py 对齐):
 *   - 全空数组   → 清除 preset, 等同 "展示全部" (默认行为)
 *   - 9 项全选   → UX 上提示 "等同于展示全部, 将清除 preset", 保存时下发空数组
 *   - 部分选中   → 仅展示这些子类型 (列表 + chip 行均限制在该范围)
 *
 * 视觉: 复用 SourceBadge 做可勾选块, 比纯 Checkbox 更直观且与列表 chip 一致.
 */

import React, { useEffect, useMemo, useState } from 'react';
import { Button, Modal, Typography, message } from 'antd';
import {
  ALL_DAILY_SOURCE_SUBTYPES,
  DailySourceSubtype,
  updateDailyPreset,
} from '../../services/daily';
import SourceBadge from './SourceBadge';
import {
  SOURCE_BADGE_DISPLAY_ORDER,
  SOURCE_BADGE_STYLES,
} from './sourceBadgeConfig';
import { DAILY_THEME_COLORS } from './dailyTheme';

const { Text } = Typography;

interface DailyPresetModalProps {
  open: boolean;
  /** 当前已保存的 preset (传入时控件初始化值; 空数组 = 未设置) */
  value: DailySourceSubtype[];
  themeMode: 'light' | 'dark';
  /** 保存成功后回调，参数为后端实际写入的最新值 (可能是 [] 表示已清除) */
  onSaved: (next: DailySourceSubtype[]) => void;
  onCancel: () => void;
}

// 桌面端 NavBar 顺序: Upload 在前, Feed 在后. modal 也按此分组以减轻认知负担.
const UPLOAD_GROUP: DailySourceSubtype[] = SOURCE_BADGE_DISPLAY_ORDER.filter(
  (s) => ['document', 'image', 'audio', 'youtube', 'web', 'text', 'podcast'].includes(s)
);
const FEED_GROUP: DailySourceSubtype[] = SOURCE_BADGE_DISPLAY_ORDER.filter((s) =>
  ['twitter', 'news', 'youtube_feed', 'zhishixingqiu', 'weibo'].includes(s)
);

const DailyPresetModal: React.FC<DailyPresetModalProps> = ({
  open,
  value,
  themeMode,
  onSaved,
  onCancel,
}) => {
  const colors = DAILY_THEME_COLORS[themeMode];
  const [selected, setSelected] = useState<Set<DailySourceSubtype>>(
    () => new Set(value)
  );
  const [saving, setSaving] = useState(false);

  // 每次打开同步外部最新值，避免上次取消后残留状态
  useEffect(() => {
    if (open) setSelected(new Set(value));
  }, [open, value]);

  const allCount = ALL_DAILY_SOURCE_SUBTYPES.length;
  const selectedCount = selected.size;
  const isAll = selectedCount === allCount;
  const isNone = selectedCount === 0;

  // "全选 = 清除 preset" 的语义提示
  const hintText = useMemo(() => {
    if (isNone) return 'No preset · Daily will show all source types';
    if (isAll) return 'All types selected · equivalent to no preset (will clear)';
    return `${selectedCount} of ${allCount} types selected`;
  }, [isAll, isNone, selectedCount, allCount]);

  const toggle = (sub: DailySourceSubtype) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(sub)) next.delete(sub);
      else next.add(sub);
      return next;
    });
  };

  const selectAll = () => setSelected(new Set(ALL_DAILY_SOURCE_SUBTYPES));
  const clearAll = () => setSelected(new Set());

  const handleSave = async () => {
    // 全选 → 等同空数组 (清除 preset)
    const payload: DailySourceSubtype[] = isAll
      ? []
      : Array.from(selected);
    setSaving(true);
    try {
      const resp = await updateDailyPreset({ source_subtypes: payload });
      message.success(
        resp.source_subtypes.length === 0
          ? 'Preset cleared · showing all sources'
          : `Preset saved · ${resp.source_subtypes.length} types`
      );
      onSaved(resp.source_subtypes);
    } catch (e: any) {
      message.error(e?.message || 'Failed to save preset');
    } finally {
      setSaving(false);
    }
  };

  const renderGroup = (
    groupTitle: string,
    list: DailySourceSubtype[]
  ) => (
    <div style={{ marginBottom: 18 }}>
      <Text
        style={{
          color: colors.textMuted,
          fontSize: 12,
          letterSpacing: 0.5,
          textTransform: 'uppercase',
          display: 'block',
          marginBottom: 8,
        }}
      >
        {groupTitle}
      </Text>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
        {list.map((sub) => {
          const cfg = SOURCE_BADGE_STYLES[sub];
          return (
            <div
              key={sub}
              onClick={() => toggle(sub)}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 8,
                padding: '8px 12px',
                borderRadius: 6,
                cursor: 'pointer',
                background: selected.has(sub)
                  ? (themeMode === 'dark'
                      ? 'rgba(33, 150, 243, 0.12)'
                      : '#e8f4ff')
                  : 'transparent',
                border: `1px solid ${
                  selected.has(sub)
                    ? (themeMode === 'dark' ? 'rgba(33, 150, 243, 0.35)' : '#91caff')
                    : colors.cardBorder
                }`,
                transition: 'all 0.15s',
                userSelect: 'none',
                minWidth: 140,
              }}
            >
              <SourceBadge subtype={sub} themeMode={themeMode} compact />
              <Text style={{ color: colors.text, fontSize: 13 }}>
                {cfg.fullName}
              </Text>
            </div>
          );
        })}
      </div>
    </div>
  );

  return (
    <Modal
      open={open}
      title="Daily Preset"
      onCancel={onCancel}
      width={640}
      destroyOnClose
      footer={[
        <Button key="clear" type="text" onClick={clearAll} disabled={isNone}>
          Clear
        </Button>,
        <Button key="all" type="text" onClick={selectAll} disabled={isAll}>
          Select all
        </Button>,
        <Button key="cancel" onClick={onCancel}>
          Cancel
        </Button>,
        <Button key="save" type="primary" loading={saving} onClick={handleSave}>
          Save
        </Button>,
      ]}
    >
      <Text style={{ color: colors.textMuted, fontSize: 13, display: 'block', marginBottom: 16 }}>
        Choose which source types appear on your Daily page. Filter chips below the search bar will
        also be limited to your selection. Leave empty (or select all) to show everything.
      </Text>

      {renderGroup('Upload sources', UPLOAD_GROUP)}
      {renderGroup('Subscriptions', FEED_GROUP)}

      <div
        style={{
          marginTop: 8,
          padding: '8px 12px',
          background: themeMode === 'dark' ? 'rgba(255,255,255,0.04)' : '#f5f5f5',
          border: `1px dashed ${colors.cardBorder}`,
          borderRadius: 4,
          fontSize: 12,
          color: colors.textMuted,
        }}
      >
        {hintText}
      </div>
    </Modal>
  );
};

export default DailyPresetModal;

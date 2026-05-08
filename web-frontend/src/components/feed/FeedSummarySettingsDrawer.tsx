/**
 * FeedSummarySettingsDrawer (PR-N3)
 *
 * Feeds 页右上角齿轮 → 用户对每条 feed 的个性化摘要配置:
 *   - 按渠道 (Twitter / YouTube) 独立开关
 *   - 选用户的私有模板 (复用 UploadAnalysisTemplatePicker)
 *   - 长度门槛 (字符数, 不填走 env 默认)
 *
 * 后端: /api/feeds/summary-settings/{channel}
 *
 * 注意: 当前集群 FEED_SUMMARY_AUTO_FANOUT_ENABLED=false (默认),
 * 个性化摘要为纯手动模式. 这里的开关仅在管理员将该 env 设为 true 后才会控制自动派发.
 * 用户应在文章页点击 "Generate AI Analysis" 手动触发单条个性化.
 * 本组件保留用于保存模板偏好与长度门槛 (手动触发时也会使用这些设置).
 */

import React, { useCallback, useEffect, useState } from 'react';
import {
  Alert,
  Button,
  Drawer,
  Divider,
  InputNumber,
  message,
  Space,
  Spin,
  Tag,
  Typography,
} from 'antd';
import { SettingOutlined } from '@ant-design/icons';
import {
  feedSummarySettingsAPI,
  FeedSummaryChannel,
  FeedSummarySettingItem,
} from '../../services/feedSummarySettings';
import UploadAnalysisTemplatePicker, {
  UploadAnalysisTemplatePickerValue,
} from '../UploadAnalysisTemplatePicker';

const { Text, Title } = Typography;

// ── 渠道元信息 ────────────────────────────────────────────────────────

interface ChannelMeta {
  channel: FeedSummaryChannel;
  title: string;
  moduleCode: string;
  label: string;
  hint: string;
  defaultMinLengthHint: string;
}

const CHANNELS: ChannelMeta[] = [
  {
    channel: 'twitter',
    title: 'Twitter / X Feed Summary',
    moduleCode: 'feed_summary',
    label: 'Twitter Summary Template',
    hint:
      '选择你的私有模板与长度门槛. 在文章页点击 "Generate AI Analysis" 手动触发单条个性化摘要. 当集群开启自动派发 (FEED_SUMMARY_AUTO_FANOUT_ENABLED=true) 时, 此开关还会控制是否对每条新 Twitter feed 自动运行.',
    defaultMinLengthHint: '默认 50 字 (env AUTO_FEED_TAG_MIN_LENGTH_TWITTER)',
  },
  {
    channel: 'youtube',
    title: 'YouTube Video Summary',
    moduleCode: 'youtube_summary',
    label: 'YouTube Summary Template',
    hint:
      '选择你的私有模板与长度门槛. 在文章页点击 "Generate AI Analysis" 手动触发单条个性化摘要. 当集群开启自动派发 (FEED_SUMMARY_AUTO_FANOUT_ENABLED=true) 时, 此开关还会控制是否对每条含字幕的 YouTube 视频自动运行.',
    defaultMinLengthHint: '默认 200 字 (env AUTO_FEED_TAG_MIN_LENGTH_YOUTUBE)',
  },
];

// ── 单渠道 Section ─────────────────────────────────────────────────────

interface ChannelSectionProps {
  meta: ChannelMeta;
  setting: FeedSummarySettingItem;
  saving: boolean;
  onSave: (
    channel: FeedSummaryChannel,
    payload: {
      enabled: boolean;
      template_id: number | null;
      min_length: number | null;
    },
  ) => Promise<void>;
}

const ChannelSection: React.FC<ChannelSectionProps> = ({
  meta,
  setting,
  saving,
  onSave,
}) => {
  // picker 的 value 用本地 state, 改完后点 Save 才真正 PUT
  const [pickerValue, setPickerValue] = useState<UploadAnalysisTemplatePickerValue>({
    enabled: setting.enabled,
    templateId: setting.template_id ?? undefined,
  });
  const [minLength, setMinLength] = useState<number | null>(setting.min_length);
  const [dirty, setDirty] = useState(false);

  // 父侧 setting 变化 (如保存成功后回写) → 同步本地
  useEffect(() => {
    setPickerValue({
      enabled: setting.enabled,
      templateId: setting.template_id ?? undefined,
    });
    setMinLength(setting.min_length);
    setDirty(false);
  }, [setting]);

  const handlePickerChange = (v: UploadAnalysisTemplatePickerValue) => {
    setPickerValue(v);
    setDirty(true);
  };

  const handleMinLengthChange = (v: number | null) => {
    setMinLength(v);
    setDirty(true);
  };

  const handleSave = async () => {
    await onSave(meta.channel, {
      enabled: pickerValue.enabled !== false,
      template_id: pickerValue.templateId ?? null,
      min_length: minLength ?? null,
    });
    setDirty(false);
  };

  const enabled = pickerValue.enabled !== false;

  return (
    <div
      style={{
        padding: '16px 0',
      }}
    >
      <Space align="baseline" style={{ marginBottom: 4 }}>
        <Title level={5} style={{ margin: 0 }}>{meta.title}</Title>
        {enabled ? (
          <Tag color="green" style={{ marginLeft: 4 }}>Auto-personalize</Tag>
        ) : (
          <Tag style={{ marginLeft: 4 }}>Off</Tag>
        )}
      </Space>

      <Text
        type="secondary"
        style={{ display: 'block', fontSize: 12, marginBottom: 12 }}
      >
        {meta.hint}
      </Text>

      <UploadAnalysisTemplatePicker
        value={pickerValue}
        onChange={handlePickerChange}
        mode="optional"
        moduleCode={meta.moduleCode}
        label={meta.label}
        hint=""
        showVariablesEditor={false}
        disabled={saving}
      />

      <div style={{ marginTop: 12 }}>
        <Text style={{ fontSize: 12, marginRight: 8 }}>
          Min content length (chars):
        </Text>
        <InputNumber
          min={0}
          step={50}
          placeholder="使用默认"
          value={minLength ?? undefined}
          onChange={(v) => handleMinLengthChange(v ?? null)}
          disabled={saving || !enabled}
          style={{ width: 130 }}
        />
        <Text type="secondary" style={{ fontSize: 11, marginLeft: 8 }}>
          {meta.defaultMinLengthHint}
        </Text>
      </div>

      <div style={{ marginTop: 12, textAlign: 'right' }}>
        <Button
          type="primary"
          size="small"
          disabled={!dirty || saving}
          loading={saving}
          onClick={handleSave}
        >
          Save
        </Button>
      </div>
    </div>
  );
};

// ── Drawer 主组件 ──────────────────────────────────────────────────────

export interface FeedSummarySettingsDrawerProps {
  open: boolean;
  onClose: () => void;
}

const FeedSummarySettingsDrawer: React.FC<FeedSummarySettingsDrawerProps> = ({
  open,
  onClose,
}) => {
  const [loading, setLoading] = useState(false);
  const [savingChannel, setSavingChannel] = useState<FeedSummaryChannel | null>(null);
  const [items, setItems] = useState<FeedSummarySettingItem[]>([]);

  const loadSettings = useCallback(async () => {
    setLoading(true);
    try {
      const res = await feedSummarySettingsAPI.list();
      setItems(res.items || []);
    } catch (err: any) {
      console.error('加载 feed summary settings 失败', err);
      message.error(err?.message || '加载设置失败');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (open) {
      loadSettings();
    }
  }, [open, loadSettings]);

  const handleSave = useCallback(
    async (
      channel: FeedSummaryChannel,
      payload: {
        enabled: boolean;
        template_id: number | null;
        min_length: number | null;
      },
    ) => {
      setSavingChannel(channel);
      try {
        const updated = await feedSummarySettingsAPI.update(channel, payload);
        setItems((prev) =>
          prev.map((it) => (it.channel_type === channel ? updated : it)),
        );
        message.success(`${channel} 摘要设置已更新`);
      } catch (err: any) {
        console.error('保存 feed summary settings 失败', err);
        message.error(err?.response?.data?.detail || err?.message || '保存失败');
      } finally {
        setSavingChannel(null);
      }
    },
    [],
  );

  const findItem = (channel: FeedSummaryChannel): FeedSummarySettingItem => {
    const found = items.find((it) => it.channel_type === channel);
    if (found) return found;
    return {
      id: null,
      channel_type: channel,
      enabled: false,
      template_id: null,
      min_length: null,
      created_at: null,
      updated_at: null,
    };
  };

  return (
    <Drawer
      title={
        <Space>
          <SettingOutlined />
          <span>Personalized Feed Summary Settings</span>
        </Space>
      }
      width={520}
      open={open}
      onClose={onClose}
      destroyOnClose
    >
      <Alert
        type="warning"
        showIcon
        message="个性化摘要为手动触发模式"
        description={
          <span style={{ fontSize: 12 }}>
            当前集群已关闭自动个性化 AI Analysis (<code>FEED_SUMMARY_AUTO_FANOUT_ENABLED=false</code>).
            请在<b>文章页点击 "Generate AI Analysis"</b> 手动触发单条分析.
            下方设置用于保存你的模板偏好与长度门槛, 手动触发时会自动使用这些配置.
            如需为所有新 Feed 自动运行, 请联系管理员开启集群开关.
          </span>
        }
        style={{ marginBottom: 16 }}
      />

      {loading ? (
        <div style={{ display: 'flex', justifyContent: 'center', padding: 60 }}>
          <Spin />
        </div>
      ) : (
        CHANNELS.map((meta, idx) => (
          <React.Fragment key={meta.channel}>
            {idx > 0 && <Divider style={{ margin: '0' }} />}
            <ChannelSection
              meta={meta}
              setting={findItem(meta.channel)}
              saving={savingChannel === meta.channel}
              onSave={handleSave}
            />
          </React.Fragment>
        ))
      )}
    </Drawer>
  );
};

export default FeedSummarySettingsDrawer;

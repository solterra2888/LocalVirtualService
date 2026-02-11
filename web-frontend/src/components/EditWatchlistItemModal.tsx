import React, { useEffect } from 'react';
import { Modal, Typography, Button, Space, message, Form, Radio, Input } from 'antd';
import {
  EditOutlined,
  StarFilled,
  StarOutlined
} from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { dashboardAPI, type WatchlistItem } from '../services/dashboard';

const { Text } = Typography;

const THEME_COLORS = {
  light: {
    cardBg: '#fff',
    cardBorder: 'rgba(0, 0, 0, 0.06)',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
  },
  dark: {
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
  },
} as const;

const CARD_STYLE_BEHANCE_DARK = {
  borderRadius: 12,
  boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
};

// 渲染星星图标的辅助函数
const renderStars = (count: number, filled: boolean = true) => {
  const stars = [];
  for (let i = 0; i < count; i++) {
    stars.push(
      filled ? (
        <StarFilled key={i} style={{ color: '#faad14', fontSize: '14px' }} />
      ) : (
        <StarOutlined key={i} style={{ color: '#d9d9d9', fontSize: '14px' }} />
      )
    );
  }
  return <Space size={2}>{stars}</Space>;
};

interface EditWatchlistItemModalProps {
  open: boolean;
  item: WatchlistItem | null;
  onCancel: () => void;
  onSuccess: () => void;
}

const EditWatchlistItemModal: React.FC<EditWatchlistItemModalProps> = ({
  open,
  item,
  onCancel,
  onSuccess
}) => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const [form] = Form.useForm();
  const [updating, setUpdating] = React.useState<boolean>(false);

  // 当item变化时，更新表单
  useEffect(() => {
    if (item && open) {
      form.setFieldsValue({
        priority: item.priority || 1,
        notes: item.notes || ''
      });
    }
  }, [item, open, form]);

  // 重置表单
  const resetForm = () => {
    form.resetFields();
  };

  // 更新watchlist项
  const handleUpdate = async (values: any) => {
    if (!item) {
      message.error('No item selected');
      return;
    }

    setUpdating(true);
    try {
      const params = {
        priority: values.priority || item.priority,
        notes: values.notes || ''
      };

      const result = await dashboardAPI.updateWatchlistItem(item.watchlist_id, params);
      message.success(result.message);
      onSuccess();
      resetForm();
      onCancel();
    } catch (error: any) {
      message.error('Failed to update watchlist item');
      console.error('Failed to update watchlist item:', error);
    } finally {
      setUpdating(false);
    }
  };

  if (!item) {
    return null;
  }

  const modalStyles = isDark
    ? {
        content: {
          borderRadius: 12,
          boxShadow: CARD_STYLE_BEHANCE_DARK.boxShadow,
          border: CARD_STYLE_BEHANCE_DARK.border,
          backgroundColor: colors.cardBg,
        },
        header: {
          color: colors.text,
          borderBottomColor: colors.cardBorder,
        },
        body: { color: colors.text },
      }
    : undefined;

  return (
    <Modal
      title={
        <Space style={{ color: 'inherit' }}>
          <EditOutlined />
          <span>Edit Watchlist Item</span>
        </Space>
      }
      open={open}
      centered
      onCancel={() => {
        resetForm();
        onCancel();
      }}
      footer={null}
      width={600}
      destroyOnClose
      styles={modalStyles}
    >
      <div style={{ marginBottom: '16px' }}>
        <Text strong>Stock:</Text>
        <div style={{ marginTop: '8px' }}>
          <Text>
            {item.symbol} - {item.company || item.custom_name || item.symbol}
          </Text>
        </div>
      </div>
      
      <Form
        form={form}
        layout="vertical"
        onFinish={handleUpdate}
      >
        <Form.Item
          name="priority"
          label="Priority"
          tooltip="Select priority level: 1 star = Low, 2 stars = Medium, 3 stars = High"
        >
          <Radio.Group>
            <Space direction="vertical" size="small">
              <Radio value={1}>
                <Space>
                  {renderStars(1)}
                  <Text>Low Priority</Text>
                </Space>
              </Radio>
              <Radio value={2}>
                <Space>
                  {renderStars(2)}
                  <Text>Medium Priority</Text>
                </Space>
              </Radio>
              <Radio value={3}>
                <Space>
                  {renderStars(3)}
                  <Text>High Priority</Text>
                </Space>
              </Radio>
            </Space>
          </Radio.Group>
        </Form.Item>

        <Form.Item
          name="notes"
          label="Notes"
        >
          <Input.TextArea 
            rows={4} 
            placeholder="Add some notes..."
          />
        </Form.Item>

        <Form.Item style={{ marginBottom: 0, textAlign: 'right' }}>
          <Space>
            <Button
              onClick={() => {
                resetForm();
                onCancel();
              }}
              style={isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined}
            >
              Cancel
            </Button>
            <Button
              type="primary"
              htmlType="submit"
              loading={updating}
              icon={<EditOutlined />}
              style={isDark ? { background: '#2196F3', borderColor: '#2196F3' } : undefined}
            >
              Update
            </Button>
          </Space>
        </Form.Item>
      </Form>
    </Modal>
  );
};

export default EditWatchlistItemModal;


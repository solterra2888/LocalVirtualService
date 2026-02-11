import React, { useState, useEffect } from 'react';
import { Modal, Form, Input, Select, Button, Space, message, Divider, Tag, ColorPicker, Spin } from 'antd';
import { PlusOutlined, FolderOutlined } from '@ant-design/icons';
import type { Color } from 'antd/es/color-picker';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { dashboardAPI, CustomGroup } from '../services/dashboard';

const { Option } = Select;

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

interface AddToCustomGroupModalProps {
  visible: boolean;
  selectedWatchlistIds: number[];
  onClose: () => void;
  onSuccess: () => void;
}

const AddToCustomGroupModal: React.FC<AddToCustomGroupModalProps> = ({
  visible,
  selectedWatchlistIds,
  onClose,
  onSuccess,
}) => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [customGroups, setCustomGroups] = useState<CustomGroup[]>([]);
  const [loadingGroups, setLoadingGroups] = useState(false);
  const [isCreatingNew, setIsCreatingNew] = useState(false);
  const [newGroupColor, setNewGroupColor] = useState<string>('#1890ff');

  // 加载自定义组列表
  useEffect(() => {
    if (visible) {
      loadCustomGroups();
    }
  }, [visible]);

  const loadCustomGroups = async () => {
    try {
      setLoadingGroups(true);
      const groups = await dashboardAPI.getCustomGroups();
      setCustomGroups(groups);
    } catch (error) {
      console.error('Failed to load custom groups:', error);
      message.error('Failed to load custom groups');
    } finally {
      setLoadingGroups(false);
    }
  };

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields();
      setLoading(true);

      if (isCreatingNew) {
        // 创建新组并添加tickers
        const createResult = await dashboardAPI.createCustomGroup({
          group_name: values.new_group_name,
          description: values.description || '',
          color: newGroupColor,
        });

        const groupId = createResult.group.id;

        // 添加tickers到新组
        const addResult = await dashboardAPI.addTickersToCustomGroup(
          groupId,
          selectedWatchlistIds
        );

        message.success(
          `Created group "${createResult.group.group_name}" and added ${addResult.added_count} ticker(s)`
        );
      } else {
        // 添加到现有组
        const groupId = values.existing_group_id;
        const addResult = await dashboardAPI.addTickersToCustomGroup(
          groupId,
          selectedWatchlistIds
        );

        message.success(
          `Added ${addResult.added_count} ticker(s) to "${addResult.group_name}"${
            addResult.skipped_count > 0 
              ? ` (${addResult.skipped_count} already in group)` 
              : ''
          }`
        );
      }

      form.resetFields();
      setIsCreatingNew(false);
      setNewGroupColor('#1890ff');
      onSuccess();
      onClose();
    } catch (error: any) {
      console.error('Failed to add to custom group:', error);
      if (error.response?.data?.detail) {
        message.error(error.response.data.detail);
      } else {
        message.error('Failed to add to custom group');
      }
    } finally {
      setLoading(false);
    }
  };

  const handleCancel = () => {
    form.resetFields();
    setIsCreatingNew(false);
    setNewGroupColor('#1890ff');
    onClose();
  };

  const handleColorChange = (color: Color) => {
    setNewGroupColor(color.toHexString());
  };

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
          <FolderOutlined />
          <span>Add to Custom Group</span>
        </Space>
      }
      open={visible}
      onCancel={handleCancel}
      footer={[
        <Button
          key="cancel"
          onClick={handleCancel}
          style={isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined}
        >
          Cancel
        </Button>,
        <Button
          key="submit"
          type="primary"
          loading={loading}
          onClick={handleSubmit}
          disabled={selectedWatchlistIds.length === 0}
          style={isDark ? { background: '#2196F3', borderColor: '#2196F3' } : undefined}
        >
          Add to Group
        </Button>,
      ]}
      width={560}
      styles={modalStyles}
    >
      <div style={{ marginBottom: 16 }}>
        <Space>
          <Tag color="blue">{selectedWatchlistIds.length} ticker(s) selected</Tag>
        </Space>
      </div>

      <Divider style={{ margin: '16px 0', borderColor: colors.cardBorder }} />

      <Form form={form} layout="vertical">
        <Form.Item label="Select Option">
          <Space direction="vertical" style={{ width: '100%' }}>
            <Button
              type={!isCreatingNew ? 'primary' : 'default'}
              icon={<FolderOutlined />}
              onClick={() => setIsCreatingNew(false)}
              block
            >
              Add to Existing Group
            </Button>
            <Button
              type={isCreatingNew ? 'primary' : 'default'}
              icon={<PlusOutlined />}
              onClick={() => setIsCreatingNew(true)}
              block
            >
              Create New Group
            </Button>
          </Space>
        </Form.Item>

        <Divider style={{ margin: '16px 0', borderColor: colors.cardBorder }} />

        {!isCreatingNew ? (
          // 选择现有组
          <Form.Item
            name="existing_group_id"
            label="Select Group"
            rules={[{ required: !isCreatingNew, message: 'Please select a group' }]}
          >
            <Select
              placeholder="Select a custom group"
              loading={loadingGroups}
              notFoundContent={
                loadingGroups ? <Spin size="small" /> : 'No custom groups found'
              }
              size="large"
            >
              {customGroups.map((group) => (
                <Option key={group.id} value={group.id}>
                  <Space>
                    <div
                      style={{
                        width: 12,
                        height: 12,
                        borderRadius: '50%',
                        backgroundColor: group.color || '#1890ff',
                      }}
                    />
                    <span>{group.group_name}</span>
                    <Tag color="default">{group.member_count || 0} members</Tag>
                  </Space>
                </Option>
              ))}
            </Select>
          </Form.Item>
        ) : (
          // 创建新组
          <>
            <Form.Item
              name="new_group_name"
              label="Group Name"
              rules={[
                { required: isCreatingNew, message: 'Please enter a group name' },
                { max: 100, message: 'Group name must be less than 100 characters' },
              ]}
            >
              <Input
                placeholder="e.g., AI Data Center, AI Chips"
                size="large"
                maxLength={100}
              />
            </Form.Item>

            <Form.Item name="description" label="Description (Optional)">
              <Input.TextArea
                placeholder="Add a description for this group"
                rows={3}
                maxLength={500}
              />
            </Form.Item>

            <Form.Item label="Color">
              <ColorPicker
                value={newGroupColor}
                onChange={handleColorChange}
                showText
                presets={[
                  {
                    label: 'Recommended',
                    colors: [
                      '#1890ff',
                      '#52c41a',
                      '#faad14',
                      '#f5222d',
                      '#722ed1',
                      '#13c2c2',
                      '#eb2f96',
                      '#fa8c16',
                    ],
                  },
                ]}
              />
            </Form.Item>
          </>
        )}
      </Form>
    </Modal>
  );
};

export default AddToCustomGroupModal;


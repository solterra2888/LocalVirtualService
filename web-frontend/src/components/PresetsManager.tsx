import React, { useState, useEffect } from 'react';
import {
  Modal,
  Table,
  Button,
  Space,
  message,
  Popconfirm,
  Tag,
  Tooltip,
  Form,
  Input,
  Tabs,
  Badge,
  Radio
} from 'antd';
import {
  EditOutlined,
  DeleteOutlined,
  PlusOutlined,
  SettingOutlined,
  StarFilled,
  StarOutlined,
  CopyOutlined
} from '@ant-design/icons';
import FilterConfigEditor from './FilterConfigEditor';
import { newsPresetsAPI } from '../services/newsPresets';
import { NewsFilterPreset, CreatePresetRequest, UpdatePresetRequest, FilterConfig } from '../types/newsPresets';
import { useAuthStore } from '../store/authStore';
import type { ColumnsType } from 'antd/es/table';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { getModalStyles } from '../utils/modalStyles';

const { TextArea } = Input;
const { TabPane } = Tabs;

interface PresetsManagerProps {
  visible: boolean;
  onClose: () => void;
}

const PresetsManager: React.FC<PresetsManagerProps> = ({
  visible,
  onClose
}) => {
  const { themeMode } = useDashboardSettings();
  const { user } = useAuthStore();
  const [presets, setPresets] = useState<NewsFilterPreset[]>([]);
  const [loading, setLoading] = useState(false);
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [editingPreset, setEditingPreset] = useState<NewsFilterPreset | null>(null);
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [editForm] = Form.useForm();
  const [createForm] = Form.useForm();
  
  // 用于创建预设的 filter_config
  const [newFilterConfig, setNewFilterConfig] = useState<FilterConfig>({});
  // 用于编辑预设的 filter_config
  const [editFilterConfig, setEditFilterConfig] = useState<FilterConfig>({});

  // 加载所有预设列表（包括系统预设和用户自定义预设）
  const loadAllPresets = async () => {
    setLoading(true);
    try {
      const response = await newsPresetsAPI.getPresets({ include_system: true });
      setPresets(response.presets);
    } catch (error: any) {
      console.error('Failed to load presets:', error);
      message.error('Failed to load presets');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (visible) {
      loadAllPresets();
    }
  }, [visible]);

  // 创建预设（仅管理员可以创建系统预设）
  const handleCreatePreset = async (e?: any) => {
    try {
      const values = await createForm.validateFields();
      
      const request: CreatePresetRequest = {
        preset_name: values.preset_name,
        description: values.description,
        filter_config: newFilterConfig,
        scope_type: 'global',
        is_default: false,
        is_favorite: false,
        // Admin可以选择创建系统预设或自定义预设
        is_system_preset: user?.is_admin ? (values.preset_type === 'system') : false
      };
      
      await newsPresetsAPI.createPreset(request);
      message.success('Preset created successfully');
      setCreateModalVisible(false);
      createForm.resetFields();
      setNewFilterConfig({});
      loadAllPresets();
    } catch (error: any) {
      console.error('Failed to create preset:', error);
      message.error(error.message || 'Failed to create preset');
    }
  };

  // 打开编辑对话框
  const handleEdit = (preset: NewsFilterPreset) => {
    setEditingPreset(preset);
    setEditFilterConfig(preset.filter_config || {});
    editForm.setFieldsValue({
      preset_name: preset.preset_name,
      description: preset.description,
      sort_order: preset.sort_order
    });
    setEditModalVisible(true);
  };

  // 更新预设
  const handleUpdatePreset = async () => {
    if (!editingPreset) return;
    
    try {
      const values = await editForm.validateFields();
      const request: UpdatePresetRequest = {
        preset_name: values.preset_name,
        description: values.description,
        sort_order: values.sort_order,
        filter_config: editFilterConfig
      };
      
      await newsPresetsAPI.updatePreset(editingPreset.id, request);
      message.success('Preset updated successfully');
      setEditModalVisible(false);
      setEditingPreset(null);
      setEditFilterConfig({});
      editForm.resetFields();
      loadAllPresets();
    } catch (error: any) {
      console.error('Failed to update preset:', error);
      message.error('Failed to update preset');
    }
  };

  // 删除预设（权限检查由后端处理）
  const handleDelete = async (preset: NewsFilterPreset) => {
    try {
      await newsPresetsAPI.deletePreset(preset.id);
      message.success('Preset deleted successfully');
      loadAllPresets();
    } catch (error: any) {
      console.error('Failed to delete preset:', error);
      message.error(error.message || 'Failed to delete preset');
    }
  };

  // 切换收藏状态
  const handleToggleFavorite = async (preset: NewsFilterPreset) => {
    try {
      await newsPresetsAPI.toggleFavorite(preset);
      message.success(preset.is_favorite ? 'Removed from favorites' : 'Added to favorites');
      loadAllPresets();
    } catch (error: any) {
      console.error('Failed to toggle favorite:', error);
      message.error('Failed to toggle favorite');
    }
  };

  // 复制预设
  const handleDuplicate = async (preset: NewsFilterPreset) => {
    const newName = `${preset.preset_name} (Copy)`;
    try {
      await newsPresetsAPI.duplicatePreset(preset.id, newName);
      message.success('Preset duplicated successfully');
      loadAllPresets();
    } catch (error: any) {
      console.error('Failed to duplicate preset:', error);
      message.error('Failed to duplicate preset');
    }
  };

  // 检查是否可以编辑/删除预设
  const canModifyPreset = (preset: NewsFilterPreset): boolean => {
    // 系统预设只有管理员可以修改
    if (preset.preset_type === 'system') {
      return user?.is_admin || false;
    }
    // 用户自定义预设，只有创建者可以修改
    return preset.user_id === user?.id;
  };

  // 表格列定义
  const columns: ColumnsType<NewsFilterPreset> = [
    {
      title: 'Type',
      dataIndex: 'preset_type',
      key: 'preset_type',
      width: 80,
      render: (type: string) => (
        <Tag color={type === 'system' ? 'blue' : 'green'} style={{ fontSize: '10px' }}>
          {type === 'system' ? 'System' : 'Custom'}
        </Tag>
      ),
      filters: [
        { text: 'System', value: 'system' },
        { text: 'Custom', value: 'custom' }
      ],
      onFilter: (value, record) => record.preset_type === value,
    },
    {
      title: 'Name',
      dataIndex: 'preset_name',
      key: 'preset_name',
      width: 200,
      render: (name: string, record: NewsFilterPreset) => (
        <Space>
          <strong>{name}</strong>
          {record.is_favorite && <StarFilled style={{ color: '#faad14', fontSize: '12px' }} />}
        </Space>
      )
    },
    {
      title: 'Description',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true
    },
    {
      title: 'Filter Config',
      dataIndex: 'filter_config',
      key: 'filter_config',
      width: 300,
      render: (config: FilterConfig) => {
        const tags: string[] = [];
        if (config.categories?.length) tags.push(`Categories: ${config.categories.length}`);
        if (config.event_types?.length) tags.push(`Events: ${config.event_types.length}`);
        if (config.urgency?.length) tags.push(`Urgency: ${config.urgency.length}`);
        if (config.related_sectors?.length) tags.push(`Sectors: ${config.related_sectors.length}`);
        if (config.related_countries?.length) tags.push(`Countries: ${config.related_countries.length}`);
        
        return (
          <Space size={[4, 4]} wrap>
            {tags.map(tag => (
              <Tag key={tag} style={{ fontSize: '10px' }}>{tag}</Tag>
            ))}
          </Space>
        );
      }
    },
    {
      title: 'Usage',
      dataIndex: 'usage_count',
      key: 'usage_count',
      width: 80,
      align: 'center',
      sorter: (a, b) => a.usage_count - b.usage_count,
      render: (count: number) => (
        <Badge count={count} showZero style={{ backgroundColor: '#52c41a' }} />
      )
    },
    {
      title: 'Order',
      dataIndex: 'sort_order',
      key: 'sort_order',
      width: 80,
      align: 'center',
      sorter: (a, b) => a.sort_order - b.sort_order,
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 160,
      fixed: 'right',
      render: (_, record) => {
        const canModify = canModifyPreset(record);
        return (
          <Space size="small">
            <Tooltip title={record.is_favorite ? 'Remove from favorites' : 'Add to favorites'}>
              <Button
                type="text"
                size="small"
                icon={record.is_favorite ? <StarFilled style={{ color: '#faad14' }} /> : <StarOutlined />}
                onClick={() => handleToggleFavorite(record)}
                disabled={!canModify}
              />
            </Tooltip>
            <Tooltip title="Duplicate">
              <Button
                type="text"
                size="small"
                icon={<CopyOutlined />}
                onClick={() => handleDuplicate(record)}
              />
            </Tooltip>
            <Tooltip title={canModify ? 'Edit' : 'No permission to edit'}>
              <Button
                type="text"
                size="small"
                icon={<EditOutlined />}
                onClick={() => handleEdit(record)}
                disabled={!canModify}
              />
            </Tooltip>
            <Popconfirm
              title={`Delete ${record.preset_type} preset`}
              description={`Are you sure to delete "${record.preset_name}"?${record.preset_type === 'system' ? ' This will affect all users.' : ''}`}
              onConfirm={() => handleDelete(record)}
              okText="Delete"
              cancelText="Cancel"
              okButtonProps={{ danger: true }}
              disabled={!canModify}
            >
              <Tooltip title={canModify ? 'Delete' : 'No permission to delete'}>
                <Button
                  type="text"
                  size="small"
                  danger
                  icon={<DeleteOutlined />}
                  disabled={!canModify}
                />
              </Tooltip>
            </Popconfirm>
          </Space>
        );
      }
    }
  ];

  return (
    <>
      <Modal
        title={
          <Space>
            <SettingOutlined />
            <span>Presets Management</span>
          </Space>
        }
        open={visible}
        onCancel={onClose}
        footer={null}
        width={1200}
        style={{ top: 20 }}
        styles={getModalStyles(themeMode)}
      >
        <div style={{ marginBottom: 16 }}>
          <Space>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setCreateModalVisible(true)}
            >
              Create {user?.is_admin ? 'Preset' : 'Custom Preset'}
            </Button>

          </Space>
        </div>

        <Table
          columns={columns}
          dataSource={presets}
          loading={loading}
          rowKey="id"
          size="small"
          pagination={{
            pageSize: 10,
            showSizeChanger: true,
            showTotal: (total) => `Total ${total} presets`
          }}
        />
      </Modal>

      {/* 创建预设对话框 */}
      <Modal
        title={`Create ${user?.is_admin ? '' : 'Custom '}Preset`}
        open={createModalVisible}
        onCancel={() => {
          setCreateModalVisible(false);
          createForm.resetFields();
          setNewFilterConfig({});
        }}
        onOk={handleCreatePreset}
        width={800}
        okText="Create"
        styles={getModalStyles(themeMode)}
      >
        <Tabs defaultActiveKey="basic">
          <TabPane tab="Basic Info" key="basic">
            <Form
              form={createForm}
              layout="vertical"
              style={{ marginTop: '16px' }}
              initialValues={{ preset_type: 'custom' }}
            >
              {user?.is_admin && (
                <Form.Item
                  name="preset_type"
                  label="Preset Type"
                  rules={[{ required: true, message: 'Please select preset type' }]}
                >
                  <Radio.Group>
                    <Radio value="custom">Custom Preset (User-specific)</Radio>
                    <Radio value="system">System Preset (Shared with all users)</Radio>
                  </Radio.Group>
                </Form.Item>
              )}
              
              <Form.Item
                name="preset_name"
                label="Preset Name"
                rules={[{ required: true, message: 'Please enter preset name' }]}
              >
                <Input placeholder="e.g., ⭐ Important News" />
              </Form.Item>

              <Form.Item
                name="description"
                label="Description"
              >
                <TextArea rows={3} placeholder="Describe what this preset filters" />
              </Form.Item>
            </Form>
          </TabPane>
          
          <TabPane tab="Filter Configuration" key="filter">
            <FilterConfigEditor
              value={newFilterConfig}
              onChange={setNewFilterConfig}
            />
          </TabPane>
        </Tabs>
      </Modal>

      {/* 编辑预设对话框 */}
      <Modal
        title={`Edit ${editingPreset?.preset_type === 'system' ? 'System' : 'Custom'} Preset`}
        open={editModalVisible}
        onCancel={() => {
          setEditModalVisible(false);
          setEditingPreset(null);
          setEditFilterConfig({});
          editForm.resetFields();
        }}
        onOk={handleUpdatePreset}
        okText="Update"
        width={800}
        styles={getModalStyles(themeMode)}
      >
        <Tabs defaultActiveKey="basic">
          <TabPane tab="Basic Info" key="basic">
            <Form
              form={editForm}
              layout="vertical"
              style={{ marginTop: '16px' }}
            >
              <Form.Item
                name="preset_name"
                label="Preset Name"
                rules={[{ required: true, message: 'Please enter preset name' }]}
              >
                <Input />
              </Form.Item>

              <Form.Item
                name="description"
                label="Description"
              >
                <TextArea rows={3} />
              </Form.Item>

              <Form.Item
                name="sort_order"
                label="Display Order"
              >
                <Input type="number" />
              </Form.Item>
            </Form>
          </TabPane>
          
          <TabPane tab="Filter Configuration" key="filter">
            <FilterConfigEditor
              value={editFilterConfig}
              onChange={setEditFilterConfig}
            />
          </TabPane>
        </Tabs>
      </Modal>
    </>
  );
};

export default PresetsManager;


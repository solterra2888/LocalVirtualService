import React, { useState, useEffect } from 'react';
import {
  Modal,
  Table,
  Button,
  Space,
  Form,
  Input,
  Switch,
  Tag,
  message,
  Popconfirm,
  Drawer,
  Row,
  Col,
  Typography,
  Alert,
  Card,
  ColorPicker,
  Divider
} from 'antd';
import {
  SettingOutlined,
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  TagOutlined,
  ReloadOutlined,
  BulbOutlined,
  RedoOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import { newsAPI } from '../services/news';
import type { NewsTheme, CreateThemeParams, UpdateThemeParams } from '../types/news';

const { Text, Title } = Typography;
const { TextArea } = Input;

interface ThemeManagementModalProps {
  open: boolean;
  onCancel: () => void;
  onSuccess: () => void;
}

interface ThemeManagementState {
  loading: boolean;
  themes: NewsTheme[];
  selectedTheme: NewsTheme | null;
  drawerVisible: boolean;
  drawerMode: 'create' | 'edit' | 'keywords';
  keywordsList: string[];
}

const ThemeManagementModal: React.FC<ThemeManagementModalProps> = ({ open, onCancel, onSuccess }) => {
  const [form] = Form.useForm();
  const [keywordForm] = Form.useForm();
  const [state, setState] = useState<ThemeManagementState>({
    loading: false,
    themes: [],
    selectedTheme: null,
    drawerVisible: false,
    drawerMode: 'create',
    keywordsList: []
  });

  // Load theme data
  const loadThemes = async () => {
    setState(prev => ({ ...prev, loading: true }));
    try {
      const response = await newsAPI.getThemes();
      setState(prev => ({
        ...prev,
        themes: response.data || [],
        loading: false
      }));
    } catch (error) {
      console.error('Failed to get themes:', error);
      message.error('Failed to get themes');
      setState(prev => ({ ...prev, loading: false }));
    }
  };

  // Initialize default themes
  const initializeDefaultThemes = async () => {
    setState(prev => ({ ...prev, loading: true }));
    try {
      const response = await newsAPI.initializeDefaultThemes();
      if (response.success) {
        message.success(`Initialization successful, created ${response.data.created_count} default themes`);
        loadThemes();
      } else {
        message.error(response.message || 'Initialization failed');
      }
    } catch (error) {
      console.error('Failed to initialize themes:', error);
      message.error('Failed to initialize themes');
    } finally {
      setState(prev => ({ ...prev, loading: false }));
    }
  };

  // Create theme
  const createTheme = async (values: CreateThemeParams) => {
    try {
      const response = await newsAPI.createTheme(values);
      if (response.success) {
        message.success('Theme created successfully');
        loadThemes();
        closeDrawer();
      } else {
        message.error(response.message || 'Creation failed');
      }
    } catch (error) {
      console.error('Failed to create theme:', error);
      message.error('Failed to create theme');
    }
  };

  // Update theme
  const updateTheme = async (themeId: number, values: UpdateThemeParams) => {
    try {
      const response = await newsAPI.updateTheme(themeId, values);
      if (response.success) {
        message.success('Theme updated successfully');
        loadThemes();
        closeDrawer();
      } else {
        message.error(response.message || 'Update failed');
      }
    } catch (error) {
      console.error('Failed to update theme:', error);
      message.error('Failed to update theme');
    }
  };

  // Delete theme
  const deleteTheme = async (themeId: number) => {
    try {
      const response = await newsAPI.deleteTheme(themeId);
      if (response.success) {
        message.success('Theme deleted successfully');
        loadThemes();
      } else {
        message.error(response.message || 'Delete failed');
      }
    } catch (error) {
      console.error('Failed to delete theme:', error);
      message.error('Failed to delete theme');
    }
  };

  // Reset theme to default state
  const resetTheme = async (themeId: number) => {
    try {
      const response = await newsAPI.resetThemeToDefault(themeId);
      if (response.success) {
        message.success('Theme reset successfully');
        loadThemes();
      } else {
        message.error(response.message || 'Reset failed');
      }
    } catch (error) {
      console.error('Failed to reset theme:', error);
      message.error('Failed to reset theme');
    }
  };

  // Add keywords
  const addKeywords = async (themeId: number, keywords: string, weights?: string) => {
    try {
      const response = await newsAPI.addThemeKeywords(themeId, { keywords, weights });
      if (response.success) {
        message.success('Keywords added successfully');
        loadThemes();
        if (state.selectedTheme?.id === themeId) {
          // Update selected theme data
          const updatedTheme = state.themes.find(t => t.id === themeId);
          if (updatedTheme) {
            setState(prev => ({ ...prev, selectedTheme: updatedTheme }));
          }
        }
      } else {
        message.error(response.message || 'Add failed');
      }
    } catch (error) {
      console.error('Failed to add keywords:', error);
      message.error('Failed to add keywords');
    }
  };

  // Remove keywords
  const removeKeywords = async (themeId: number, keywords: string) => {
    try {
      const response = await newsAPI.removeThemeKeywords(themeId, keywords);
      if (response.success) {
        message.success('Keywords removed successfully');
        loadThemes();
        if (state.selectedTheme?.id === themeId) {
          // Update selected theme data
          const updatedTheme = state.themes.find(t => t.id === themeId);
          if (updatedTheme) {
            setState(prev => ({ ...prev, selectedTheme: updatedTheme }));
          }
        }
      } else {
        message.error(response.message || 'Remove failed');
      }
    } catch (error) {
      console.error('Failed to remove keywords:', error);
      message.error('Failed to remove keywords');
    }
  };

  // Open drawer
  const openDrawer = (mode: 'create' | 'edit' | 'keywords', theme?: NewsTheme) => {
    setState(prev => ({
      ...prev,
      drawerVisible: true,
      drawerMode: mode,
      selectedTheme: theme || null
    }));

    if (mode === 'edit' && theme) {
      form.setFieldsValue({
        topic_name: theme.topic_name,
        description: theme.description,
        is_active: theme.is_active,
        color: theme.color
      });
    } else if (mode === 'keywords' && theme) {
      setState(prev => ({ ...prev, keywordsList: [...theme.keywords] }));
    } else {
      form.resetFields();
      keywordForm.resetFields();
    }
  };

  // Close drawer
  const closeDrawer = () => {
    setState(prev => ({
      ...prev,
      drawerVisible: false,
      selectedTheme: null,
      keywordsList: []
    }));
    form.resetFields();
    keywordForm.resetFields();
  };

  // Handle form submission
  const handleSubmit = async (values: any) => {
    if (state.drawerMode === 'create') {
      await createTheme(values);
    } else if (state.drawerMode === 'edit' && state.selectedTheme) {
      await updateTheme(state.selectedTheme.id, values);
    }
  };

  // Handle keyword operations
  const handleKeywordAction = async (values: any) => {
    if (!state.selectedTheme) return;

    if (values.action === 'add') {
      await addKeywords(state.selectedTheme.id, values.keywords, values.weights);
      keywordForm.resetFields();
    }
  };

  useEffect(() => {
    if (open) {
      loadThemes();
    }
  }, [open]);

  const columns: ColumnsType<NewsTheme> = [
    {
      title: 'Topic Name',
      dataIndex: 'topic_name',
      key: 'topic_name',
      width: 200,
      ellipsis: true,
      render: (name: string, record: NewsTheme) => (
        <Space>
          <span style={{ color: record.color || '#1890ff' }}>●</span>
          <Text strong={record.is_active} style={{ fontSize: '11px' }}>{name}</Text>
          {!record.is_active && <Tag color="red" style={{ fontSize: '10px' }}>Inactive</Tag>}
        </Space>
      )
    },
    {
      title: 'Description',
      dataIndex: 'description',
      key: 'description',
      width: 250,
      ellipsis: true,
      render: (text: string) => (
        <Text style={{ fontSize: '11px' }}>{text}</Text>
      )
    },
    {
      title: 'Keywords Count',
      key: 'keywords_count',
      width: 120,
      render: (_, record: NewsTheme) => (
        <Tag color="blue" style={{ fontSize: '10px' }}>{record.keywords.length}</Tag>
      )
    },
    {
      title: 'Status',
      dataIndex: 'is_active',
      key: 'is_active',
      width: 80,
      render: (active: boolean) => (
        <Tag color={active ? 'green' : 'red'} style={{ fontSize: '10px' }}>
          {active ? 'Active' : 'Inactive'}
        </Tag>
      )
    },
    {
      title: 'Created At',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 150,
      render: (date: string) => (
        <Text style={{ fontSize: '11px' }}>
          {date ? new Date(date).toLocaleString() : '-'}
        </Text>
      )
    },
    {
      title: 'Actions',
      key: 'action',
      width: 120,
      render: (_, record: NewsTheme) => (
        <Space size={4}>
          <Button
            size="small"
            icon={<TagOutlined />}
            onClick={() => openDrawer('keywords', record)}
            title="Manage Keywords"
            style={{ fontSize: '10px', padding: '4px', minWidth: '24px' }}
          />
          <Button
            size="small"
            icon={<EditOutlined />}
            onClick={() => openDrawer('edit', record)}
            title="Edit Theme"
            style={{ fontSize: '10px', padding: '4px', minWidth: '24px' }}
          />
          <Button
            size="small"
            icon={<RedoOutlined />}
            onClick={() => resetTheme(record.id)}
            title="Reset to default state"
            style={{ fontSize: '10px', padding: '4px', minWidth: '24px' }}
          />
          <Popconfirm
            title="Are you sure you want to delete this theme?"
            description="Once deleted, it cannot be recovered and related keywords will also be deleted."
            onConfirm={() => deleteTheme(record.id)}
            okText="Confirm"
            cancelText="Cancel"
          >
            <Button
              size="small"
              danger
              icon={<DeleteOutlined />}
              title="Delete Theme"
              style={{ fontSize: '10px', padding: '4px', minWidth: '24px' }}
            />
          </Popconfirm>
        </Space>
      )
    }
  ];

  const renderDrawerContent = () => {
    if (state.drawerMode === 'keywords') {
      return (
        <div>
          <Title level={4} style={{ fontSize: '14px' }}>Manage Keywords</Title>
          <Text type="secondary" style={{ fontSize: '11px' }}>Theme: {state.selectedTheme?.topic_name}</Text>
          
          <Divider />
          
          {/* Add keywords form */}
          <Card title={<Text style={{ fontSize: '11px' }}>Add Keywords</Text>} size="small" style={{ marginBottom: 16 }}>
            <Form
              form={keywordForm}
              layout="vertical"
              onFinish={handleKeywordAction}
            >
              <Form.Item name="action" initialValue="add" style={{ display: 'none' }}>
                <Input />
              </Form.Item>
              <Row gutter={16}>
                <Col span={16}>
                  <Form.Item
                    name="keywords"
                    label={<Text style={{ fontSize: '11px' }}>Keywords</Text>}
                    rules={[{ required: true, message: 'Please enter keywords' }]}
                  >
                    <TextArea
                      placeholder="Enter keywords, separate multiple with commas"
                      rows={3}
                      style={{ fontSize: '11px' }}
                    />
                  </Form.Item>
                </Col>
                <Col span={8}>
                  <Form.Item
                    name="weights"
                    label={<Text style={{ fontSize: '11px' }}>Weights (Optional)</Text>}
                    tooltip="Weight values, separate multiple with commas, default is 1.0"
                  >
                    <Input 
                      placeholder="e.g.: 1.0,2.0,1.5" 
                      style={{ fontSize: '11px' }}
                    />
                  </Form.Item>
                </Col>
              </Row>
              <Button type="primary" htmlType="submit" icon={<PlusOutlined />} style={{ fontSize: '11px' }}>
                Add Keywords
              </Button>
            </Form>
          </Card>
          
          {/* Existing keywords list */}
          <Card title={<Text style={{ fontSize: '11px' }}>Existing Keywords</Text>} size="small">
            {state.selectedTheme?.keywords.length ? (
              <div>
                {state.selectedTheme.keywords.map((keyword, index) => (
                  <Tag
                    key={index}
                    closable
                    onClose={() => removeKeywords(state.selectedTheme!.id, keyword)}
                    style={{ marginBottom: 8, fontSize: '10px' }}
                  >
                    {keyword}
                  </Tag>
                ))}
              </div>
            ) : (
              <Text type="secondary" style={{ fontSize: '11px' }}>No keywords yet</Text>
            )}
          </Card>
        </div>
      );
    }

    return (
      <Form
        form={form}
        layout="vertical"
        onFinish={handleSubmit}
      >
        <Form.Item
          name="topic_name"
          label={<Text style={{ fontSize: '11px' }}>Topic Name</Text>}
          rules={[{ required: true, message: 'Please enter topic name' }]}
        >
          <Input 
            placeholder="Enter topic name" 
            style={{ fontSize: '11px' }}
          />
        </Form.Item>
        
        <Form.Item
          name="description"
          label={<Text style={{ fontSize: '11px' }}>Description</Text>}
        >
          <TextArea 
            placeholder="Enter topic description" 
            rows={3} 
            style={{ fontSize: '11px' }}
          />
        </Form.Item>
        
        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              name="is_active"
              label={<Text style={{ fontSize: '11px' }}>Status</Text>}
              valuePropName="checked"
              initialValue={true}
            >
              <Switch checkedChildren="Active" unCheckedChildren="Inactive" />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              name="color"
              label={<Text style={{ fontSize: '11px' }}>Theme Color</Text>}
            >
              <ColorPicker format="hex" />
            </Form.Item>
          </Col>
        </Row>
        
        {state.drawerMode === 'create' && (
          <Form.Item
            name="keywords"
            label={<Text style={{ fontSize: '11px' }}>Initial Keywords</Text>}
            tooltip="You can add some initial keywords when creating a theme, separate multiple with commas"
          >
            <TextArea 
              placeholder="Enter initial keywords, separate multiple with commas" 
              rows={3} 
              style={{ fontSize: '11px' }}
            />
          </Form.Item>
        )}
        
        <Space>
          <Button type="primary" htmlType="submit" style={{ fontSize: '11px' }}>
            {state.drawerMode === 'create' ? 'Create' : 'Update'}
          </Button>
          <Button onClick={closeDrawer} style={{ fontSize: '11px' }}>Cancel</Button>
        </Space>
      </Form>
    );
  };

  return (
    <>
      <style>
        {`
          .ant-table-thead > tr > th {
            font-size: 11px !important;
            padding: 8px 4px !important;
          }
          .ant-table-tbody > tr > td {
            font-size: 11px !important;
            padding: 6px 4px !important;
          }
          .ant-form-item-label > label {
            font-size: 11px !important;
          }
          .ant-card-head-title {
            font-size: 11px !important;
          }
          .ant-alert-message {
            font-size: 11px !important;
          }
          .ant-alert-description {
            font-size: 11px !important;
          }
          .ant-btn {
            font-size: 11px !important;
          }
          .ant-input {
            font-size: 11px !important;
          }
          .ant-input::placeholder {
            font-size: 11px !important;
          }
          .ant-select-selection-item {
            font-size: 11px !important;
          }
          .ant-select-selection-placeholder {
            font-size: 11px !important;
          }
          .ant-tag {
            font-size: 10px !important;
          }
          .ant-drawer-title {
            font-size: 12px !important;
          }
          .ant-modal-title {
            font-size: 14px !important;
          }
        `}
      </style>
      <Modal
        title={
          <Space>
            <SettingOutlined />
            <Text style={{ fontSize: '14px' }}>Highlight Setting</Text>
          </Space>
        }
        open={open}
        onCancel={onCancel}
        footer={[
          <Button key="close" onClick={onCancel} style={{ fontSize: '11px' }}>
            Close
          </Button>
        ]}
        width={1000}
        style={{ top: 20 }}
        bodyStyle={{ maxHeight: '80vh', overflow: 'auto' }}
      >
        <div style={{ marginBottom: 16 }}>
          <Space wrap>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => openDrawer('create')}
              size="small"
              style={{ fontSize: '11px' }}
            >
              New Theme
            </Button>
            <Button
              icon={<BulbOutlined />}
              onClick={initializeDefaultThemes}
              loading={state.loading}
              size="small"
              style={{ fontSize: '11px' }}
            >
              Initialize Default Themes
            </Button>
            <Button
              icon={<ReloadOutlined />}
              onClick={loadThemes}
              loading={state.loading}
              size="small"
              style={{ fontSize: '11px' }}
            >
              Refresh
            </Button>
          </Space>
        </div>

        <Alert
          message={<Text style={{ fontSize: '11px' }}>Keyword Highlight Guide</Text>}
          description={
            <Text style={{ fontSize: '11px' }}>
              Create themes with keywords to automatically highlight matching words in news content. Each theme can have a custom color, and keywords will be highlighted with that color when found in the original news text.
            </Text>
          }
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
        />

        <Table
          columns={columns}
          dataSource={state.themes}
          rowKey="id"
          loading={state.loading}
          pagination={{ pageSize: 10 }}
          size="small"
          scroll={{ x: 'max-content' }}
          style={{ fontSize: '11px' }}
        />
      </Modal>

      <Drawer
        title={
          <Text style={{ fontSize: '12px' }}>
            {state.drawerMode === 'create' ? 'New Theme' :
            state.drawerMode === 'edit' ? 'Edit Theme' : 'Manage Keywords'}
          </Text>
        }
        open={state.drawerVisible}
        onClose={closeDrawer}
        width={500}
      >
        {renderDrawerContent()}
      </Drawer>
    </>
  );
};

export default ThemeManagementModal;

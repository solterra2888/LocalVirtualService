import React, { useState, useEffect } from 'react';
import {
  Card,
  Button,
  Table,
  Tag,
  Space,
  Typography,
  Modal,
  Form,
  Input,
  message,
  Popconfirm,
  Descriptions,
  List,
  Tooltip,
  InputNumber,
  Tabs,
  Row,
  Col,
  Select,
  ConfigProvider,
  theme as antdTheme
} from 'antd';
import {
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  EyeOutlined,
  StarOutlined,
  ReloadOutlined
} from '@ant-design/icons';
import { tasksAPI } from '../services/tasks';
import { getErrorMessage } from '../utils/errorHandler';
import { getModalStyles } from '../utils/modalStyles';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Title, Text } = Typography;

const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    codeBlockBg: '#f5f5f5',
    selectedBorder: '#1890ff',
    inputBg: '#f5f5f5',
  },
  dark: {
    pageBg: '#1E202B',
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
    codeBlockBg: '#1E202B',
    selectedBorder: '#2196F3',
    inputBg: '#1E202B',
    hoverBg: 'rgba(255, 255, 255, 0.06)',
  },
} as const;

const CARD_STYLE_BEHANCE = {
  borderRadius: 12,
  boxShadow: '0 4px 24px rgba(0, 0, 0, 0.12)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
};
const CARD_STYLE_BEHANCE_DARK = {
  borderRadius: 12,
  boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
};

const { TabPane } = Tabs;

interface TaskType {
  id: string;
  name: string;
  token: string;
  creator: string;
  created_at: string;
  remarks?: string;
  input_fields: Array<{
    name: string;
    max_length: number;
  }>;
  is_default: boolean;
  agent_type: 'chatbot' | 'workflow';
}



interface TaskFormData {
  name: string;
  token: string;
  remarks: string;
  agent_type: 'chatbot' | 'workflow';
  input_fields: Array<{
    name: string;
    max_length: number;
  }>;
}

const TasksPage: React.FC = () => {
  const { user } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const cardStyle = isDark ? CARD_STYLE_BEHANCE_DARK : CARD_STYLE_BEHANCE;
  const [taskTypes, setTaskTypes] = useState<TaskType[]>([]);
  const [loading, setLoading] = useState(false);
  const [taskFormVisible, setTaskFormVisible] = useState(false);
  const [editingTask, setEditingTask] = useState<TaskType | null>(null);
  const [viewingTask, setViewingTask] = useState<TaskType | null>(null);
  const [taskDetailVisible, setTaskDetailVisible] = useState(false);
  const [form] = Form.useForm();
  const [inputFieldsForm] = Form.useForm();
  
  // 添加操作状态防护
  const [submittingTask, setSubmittingTask] = useState(false);
  const [deletingTaskId, setDeletingTaskId] = useState<string | null>(null);

  useEffect(() => {
    loadTaskTypes();
  }, []);

  const loadTaskTypes = async (showError = true) => {
    setLoading(true);
    try {
      const types = await tasksAPI.getTaskTypes();
      setTaskTypes(types);
    } catch (error: any) {
      console.error('加载RC Agent失败:', error);
      
      // 在控制台输出详细错误信息
      console.error('详细错误信息:', {
        status: error.response?.status,
        statusText: error.response?.statusText,
        data: error.response?.data,
        config: error.config,
        message: error.message
      });
      
      // 只有在显示错误时才显示错误消息
      if (showError) {
        const errorMessage = getErrorMessage(error, '加载RC Agent失败');
        message.error(errorMessage);
      }
    } finally {
      setLoading(false);
    }
  };



  const handleCreateTask = () => {
    setEditingTask(null);
    form.resetFields();
    inputFieldsForm.resetFields();
    setTaskFormVisible(true);
  };

  const handleEditTask = (task: TaskType) => {
    setEditingTask(task);
    form.setFieldsValue({
      name: task.name,
      token: task.token,
      remarks: task.remarks,
      agent_type: task.agent_type || 'chatbot'
    });
    
    // 设置自定义输入字段
    inputFieldsForm.setFieldsValue({
      input_fields: task.input_fields
    });
    
    setTaskFormVisible(true);
  };

  const handleDeleteTask = async (task: TaskType) => {
    if (deletingTaskId === task.id) return; // 防止重复删除
    
    setDeletingTaskId(task.id);
    try {
      await tasksAPI.deleteTaskType(task.id);
      message.success('RC Agent删除成功');
      loadTaskTypes();
    } catch (error: any) {
      console.error('删除RC Agent失败:', error);
      const errorMessage = getErrorMessage(error, '删除RC Agent失败');
      message.error(errorMessage);
    } finally {
      setDeletingTaskId(null);
    }
  };

  const handleViewTask = (task: TaskType) => {
    setViewingTask(task);
    setTaskDetailVisible(true);
  };

  const handleSubmitTask = async () => {
    if (submittingTask) return; // 防止重复提交
    
    setSubmittingTask(true);
    try {
      const formData = await form.validateFields();
      const inputFields = inputFieldsForm.getFieldsValue().input_fields || [];
      
      const taskData: TaskFormData = {
        ...formData,
        input_fields: inputFields.filter((field: any) => field && field.name)
      };

      if (editingTask) {
        await tasksAPI.updateTaskType(editingTask.id, taskData);
        message.success('RC Agent更新成功');
      } else {
        await tasksAPI.createTaskType(taskData);
        message.success('RC Agent创建成功');
      }
      
      setTaskFormVisible(false);
      loadTaskTypes();
    } catch (error: any) {
      console.error(editingTask ? '更新RC Agent失败:' : '创建RC Agent失败:', error);
      const errorMessage = getErrorMessage(error, editingTask ? '更新RC Agent失败' : '创建RC Agent失败');
      message.error(errorMessage);
    } finally {
      setSubmittingTask(false);
    }
  };



  const taskTypeColumns = [
    {
      title: 'Task Name',
      dataIndex: 'name',
      key: 'name',
      render: (text: string, record: TaskType) => (
        <Space>
          {record.is_default && <StarOutlined style={{ color: '#faad14' }} />}
          <strong>{text}</strong>
          {record.is_default && <Tag color="blue">Default</Tag>}
        </Space>
      )
    },
    {
      title: 'Token',
      dataIndex: 'token',
      key: 'token',
      render: (token: string) => (
        token ? `${token.substring(0, 8)}...${token.substring(token.length - 8)}` : '-'
      )
    },
    {
      title: 'Agent Type',
      dataIndex: 'agent_type',
      key: 'agent_type',
      render: (agent_type: string) => (
        <Tag color={agent_type === 'workflow' ? 'green' : 'blue'}>
          {agent_type === 'workflow' ? 'Workflow' : 'Chatbot'}
        </Tag>
      )
    },
    {
      title: 'Creator',
      dataIndex: 'creator',
      key: 'creator'
    },
    {
      title: 'Created Time',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (date: string) => date ? new Date(date).toLocaleString() : '-'
    },
    {
      title: 'Input Fields',
      dataIndex: 'input_fields',
      key: 'input_fields',
      render: (fields: Array<{ name: string; max_length: number }>) => (
        <Space>
          <Text>{fields.length} fields</Text>
          {fields.length > 0 && (
            <Tooltip title={fields.map(f => f.name).join(', ')}>
              <Button type="text" size="small" icon={<EyeOutlined />} />
            </Tooltip>
          )}
        </Space>
      )
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_: any, record: TaskType) => {
        // 检查用户是否有权限编辑/删除此AI Agent
        const canEdit = user?.is_admin || record.creator === user?.username;
        const canDelete = user?.is_admin || (record.creator === user?.username && !record.is_default);
        
        return (
          <Space>
            <Button
              type="text"
              icon={<EyeOutlined />}
              onClick={() => handleViewTask(record)}
              size="small"
            >
              View
            </Button>
            {canEdit && (
              <Button
                type="text"
                icon={<EditOutlined />}
                onClick={() => handleEditTask(record)}
                size="small"
              >
                Edit
              </Button>
            )}
            {canDelete && (
              <Popconfirm
                title="Are you sure to delete this AI Agent?"
                onConfirm={() => handleDeleteTask(record)}
                okText="Confirm"
                cancelText="Cancel"
              >
                <Button
                  type="text"
                  danger
                  icon={<DeleteOutlined />}
                  size="small"
                  loading={deletingTaskId === record.id}
                  disabled={deletingTaskId === record.id}
                >
                  Delete
                </Button>
              </Popconfirm>
            )}
          </Space>
        );
      }
    }
  ];



  const modalStyles = getModalStyles(themeMode);

  const renderTaskForm = () => (
    <Modal
      title={editingTask ? 'Edit AI Agent' : 'Create AI Agent'}
      open={taskFormVisible}
      onOk={handleSubmitTask}
      onCancel={() => setTaskFormVisible(false)}
      width={800}
      okText="Save"
      cancelText="Cancel"
      confirmLoading={submittingTask}
      styles={modalStyles}
      okButtonProps={{
        style: {
          borderRadius: 8,
          fontWeight: 500,
          ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}),
        },
      }}
      cancelButtonProps={{
        style: isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined,
      }}
    >
      <Tabs defaultActiveKey="basic">
        <TabPane tab="Basic Information" key="basic">
          <Form form={form} layout="vertical">
            <Form.Item
              label="Task Name"
              name="name"
              rules={[{ required: true, message: 'Please enter task name' }]}
            >
              <Input
                placeholder="Enter task name"
                maxLength={100}
              />
            </Form.Item>
            
            <Form.Item
              label="AI Agent Token"
              name="token"
              rules={[{ required: true, message: 'Please enter AI Agent Token' }]}
            >
              <Input.Password
                placeholder="Enter AI Agent Token"
                maxLength={200}
              />
            </Form.Item>
            
            <Form.Item
              label="Agent Type"
              name="agent_type"
              rules={[{ required: true, message: 'Please select agent type' }]}
              initialValue="chatbot"
            >
              <Select placeholder="Select Agent Type">
                <Select.Option value="chatbot">Chatbot</Select.Option>
                <Select.Option value="workflow">Workflow</Select.Option>
              </Select>
            </Form.Item>
            
            <Form.Item
              label="Remarks"
              name="remarks"
            >
              <Input.TextArea
                placeholder="Enter remarks (optional)"
                rows={3}
                maxLength={500}
              />
            </Form.Item>
          </Form>
        </TabPane>
        
        <TabPane tab="Custom Input Fields" key="fields">
          <Form form={inputFieldsForm} layout="vertical">
            <Form.List name="input_fields">
              {(fields, { add, remove }) => (
                <>
                  {fields.map(({ key, name, ...restField }) => (
                    <Card
                      key={key}
                      size="small"
                      style={{
                        marginBottom: 16,
                        borderRadius: 8,
                        backgroundColor: colors.codeBlockBg,
                        border: isDark ? `1px solid ${colors.cardBorder}` : undefined,
                      }}
                    >
                      <Row gutter={16}>
                        <Col span={12}>
                          <Form.Item
                            {...restField}
                            name={[name, 'name']}
                            label="Field Name"
                            rules={[{ required: true, message: 'Please enter field name' }]}
                          >
                            <Input placeholder="Enter field name in English (e.g.: text, paragraph)" />
                          </Form.Item>
                        </Col>
                        <Col span={8}>
                          <Form.Item
                            {...restField}
                            name={[name, 'max_length']}
                            label="Max Length"
                            rules={[{ required: true, message: 'Please set max length' }]}
                          >
                            <InputNumber
                              placeholder="Max length"
                              min={1}
                              max={10000}
                              style={{ width: '100%' }}
                            />
                          </Form.Item>
                        </Col>
                        <Col span={4}>
                          <Form.Item label=" ">
                            <Button
                              type="text"
                              danger
                              icon={<DeleteOutlined />}
                              onClick={() => remove(name)}
                            >
                              Delete
                            </Button>
                          </Form.Item>
                        </Col>
                      </Row>
                    </Card>
                  ))}
                  <Form.Item>
                    <Button
                      type="dashed"
                      onClick={() => add({ name: '', max_length: 1000 })}
                      block
                      icon={<PlusOutlined />}
                    >
                      Add Input Field
                    </Button>
                  </Form.Item>
                </>
              )}
            </Form.List>
          </Form>
        </TabPane>
      </Tabs>
    </Modal>
  );

  const renderTaskDetail = () => (
    <Modal
      title="Task Details"
      open={taskDetailVisible}
      onCancel={() => setTaskDetailVisible(false)}
      footer={null}
      width={600}
      styles={modalStyles}
    >
      {viewingTask && (
        <Descriptions bordered size="small">
          <Descriptions.Item label="Task Name" span={3}>
            <Space>
              {viewingTask.name}
              {viewingTask.is_default && <Tag color="blue">Default</Tag>}
            </Space>
          </Descriptions.Item>
          <Descriptions.Item label="Token" span={3}>
            {viewingTask.token ? `${viewingTask.token.substring(0, 8)}...${viewingTask.token.substring(viewingTask.token.length - 8)}` : '-'}
          </Descriptions.Item>
          <Descriptions.Item label="Agent Type" span={3}>
            <Tag color={viewingTask.agent_type === 'workflow' ? 'green' : 'blue'}>
              {viewingTask.agent_type === 'workflow' ? 'Workflow' : 'Chatbot'}
            </Tag>
          </Descriptions.Item>
          <Descriptions.Item label="Creator" span={3}>
            {viewingTask.creator || '-'}
          </Descriptions.Item>
          <Descriptions.Item label="Created Time" span={3}>
            {viewingTask.created_at ? new Date(viewingTask.created_at).toLocaleString() : '-'}
          </Descriptions.Item>
          <Descriptions.Item label="Remarks" span={3}>
            {viewingTask.remarks || '-'}
          </Descriptions.Item>
          <Descriptions.Item label="Custom Input Fields" span={3}>
            {viewingTask.input_fields.length > 0 ? (
              <List
                size="small"
                dataSource={viewingTask.input_fields}
                renderItem={(field) => (
                  <List.Item>
                    <Space>
                      <Text strong>{field.name}</Text>
                      <Text type="secondary">Max length: {field.max_length}</Text>
                    </Space>
                  </List.Item>
                )}
              />
            ) : (
              <Text type="secondary">No custom input fields</Text>
            )}
          </Descriptions.Item>
        </Descriptions>
      )}
    </Modal>
  );



  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: colors.cardBg,
            colorBgElevated: colors.cardBg,
            colorBorderSecondary: colors.cardBorder,
            colorPrimary: '#2196F3',
            colorText: colors.text,
            colorTextSecondary: colors.textSecondary,
            colorFillQuaternary: colors.inputBg ?? '#1E202B',
            colorBgLayout: colors.pageBg,
            borderRadius: 8,
            borderRadiusLG: 12,
          },
        }),
      }}
    >
    <div style={{ padding: '12px 40px 32px', backgroundColor: colors.pageBg, minHeight: '100%' }}>
      <div className="page-header" style={{ marginBottom: 20 }}>
        <div className="flex-between">
          <div style={{ flex: 1, marginRight: 24 }}>
            <Title level={3} className="page-title" style={{ color: colors.text, marginTop: 0, marginBottom: 4 }}>
              AI Agent Settings
            </Title>
            <div className="page-description" style={{ color: colors.textSecondary, marginBottom: 20 }}>
              Configure and manage AI agents that integrate with third-party LLMOps platforms.
              These agents leverage external chatbot and workflow services to provide intelligent
              automation capabilities. Before configuring agents here, ensure you have already
              set up the corresponding chatbots or workflows on the Dify platform, as this
              interface serves as the configuration and invocation layer for those external services.
            </div>
          </div>
          <Space>
            <Button
              icon={<ReloadOutlined />}
              onClick={() => loadTaskTypes(true)}
              loading={loading}
              style={isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined}
            >
              Refresh
            </Button>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={handleCreateTask}
              style={{
                borderRadius: 10,
                fontWeight: 500,
                height: 36,
                paddingLeft: 16,
                paddingRight: 16,
                ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}),
              }}
            >
              Create Task
            </Button>
          </Space>
        </div>
      </div>

      <div className="page-content">
        <Tabs defaultActiveKey="types">
          <TabPane tab="AI Agents" key="types">
            <Card
              style={{ ...cardStyle, background: colors.cardBg }}
              bodyStyle={{ padding: '20px 24px' }}
            >
              <Table
                columns={taskTypeColumns}
                dataSource={taskTypes}
                rowKey="id"
                loading={loading}
                pagination={{
                  pageSize: 10,
                  showSizeChanger: true,
                  showQuickJumper: true,
                  showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} items`
                }}
              />
            </Card>
          </TabPane>
        </Tabs>
      </div>

      {renderTaskForm()}
      {renderTaskDetail()}
    </div>
    </ConfigProvider>
  );
};

export default TasksPage; 
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
  Divider,
  Select
} from 'antd';
import {
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  EyeOutlined,
  SettingOutlined,
  StarOutlined,
  ReloadOutlined
} from '@ant-design/icons';
import { useAuthStore } from '../store/authStore';
import { tasksAPI } from '../services/tasks';
import { getErrorMessage } from '../utils/errorHandler';

const { Title, Text } = Typography;
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

  const loadTaskTypes = async () => {
    setLoading(true);
    try {
      const types = await tasksAPI.getTaskTypes();
      setTaskTypes(types);
    } catch (error: any) {
      console.error('加载RC Agent失败:', error);
      
      const errorMessage = getErrorMessage(error, '加载RC Agent失败');
      message.error(errorMessage);
      
      // 在控制台输出详细错误信息
      console.error('详细错误信息:', {
        status: error.response?.status,
        statusText: error.response?.statusText,
        data: error.response?.data,
        config: error.config,
        message: error.message
      });
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
      render: (_: any, record: TaskType) => (
        <Space>
          <Button
            type="text"
            icon={<EyeOutlined />}
            onClick={() => handleViewTask(record)}
            size="small"
          >
            View
          </Button>
          <Button
            type="text"
            icon={<EditOutlined />}
            onClick={() => handleEditTask(record)}
            size="small"
          >
            Edit
          </Button>
          {!record.is_default && (
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
      )
    }
  ];



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
                    <Card key={key} size="small" style={{ marginBottom: 16 }}>
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
    <div>
      <div className="page-header">
        <div className="flex-between">
          <div>
            <Title level={3} className="page-title">Task Management</Title>
            <div className="page-description">Manage AI Agents</div>
          </div>
          <Space>
            <Button icon={<ReloadOutlined />} onClick={loadTaskTypes} loading={loading}>
              Refresh
            </Button>
            <Button type="primary" icon={<PlusOutlined />} onClick={handleCreateTask}>
              Create Task
            </Button>
          </Space>
        </div>
      </div>

      <div className="page-content">
        <Tabs defaultActiveKey="types">
                      <TabPane tab="AI Agents" key="types">
            <Card>
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
  );
};

export default TasksPage; 
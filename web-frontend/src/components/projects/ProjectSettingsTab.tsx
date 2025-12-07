/**
 * Project Settings Tab Component
 * 项目设置Tab - 项目编辑、删除、危险操作
 */

import React, { useState } from 'react';
import {
  Card,
  Form,
  Input,
  Select,
  Button,
  Space,
  message,
  Modal,
  Alert,
  Typography
} from 'antd';
import {
  SaveOutlined,
  DeleteOutlined,
  WarningOutlined,
  FolderOpenOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { useNavigate } from 'react-router-dom';

import {
  InvestmentProject,
  ProjectUpdateRequest,
  PROJECT_STATUS_OPTIONS,
  PROJECT_PRIORITY_OPTIONS
} from '../../types/project';
import { updateProject, deleteProject } from '../../services/projects';

const { TextArea } = Input;
const { Option } = Select;
const { Paragraph } = Typography;

interface ProjectSettingsTabProps {
  project: InvestmentProject;
  permissions: any;
  onUpdate: () => void;
}

const ProjectSettingsTab: React.FC<ProjectSettingsTabProps> = ({
  project,
  permissions,
  onUpdate
}) => {
  const navigate = useNavigate();
  const [form] = Form.useForm();
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);

  // 初始化表单值
  React.useEffect(() => {
    form.setFieldsValue({
      project_name: project.project_name,
      status: project.status,
      priority: project.priority,
      tickers: project.tickers || [],
      description: project.description,
      current_position: project.current_position
    });
  }, [project, form]);

  // 保存项目设置
  const handleSave = async (values: any) => {
    setSaving(true);
    try {
      const updateData: ProjectUpdateRequest = {
        project_name: values.project_name,
        status: values.status,
        priority: values.priority,
        tickers: values.tickers || [],
        description: values.description,
        current_position: values.current_position
      };

      await updateProject(project.id, updateData);
      message.success('Event settings saved successfully');
      onUpdate();
    } catch (error: any) {
      message.error(`Save failed: ${error.response?.data?.detail || error.message}`);
    } finally {
      setSaving(false);
    }
  };

  // 删除项目
  const handleDelete = () => {
    Modal.confirm({
      title: 'Confirm Delete Event',
      icon: <WarningOutlined />,
      content: (
        <div>
          <Paragraph>
            After deleting, the event will:
          </Paragraph>
          <ul>
            <li>Be permanently deleted from the system</li>
            <li>All associated data will be deleted (files, milestones, members)</li>
            <li>This action cannot be undone</li>
          </ul>
          <Paragraph strong>
            Are you sure you want to delete event "{project.project_name}"?
          </Paragraph>
        </div>
      ),
      okText: 'Confirm Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk: async () => {
        setDeleting(true);
        try {
          await deleteProject(project.id);
          message.success('Event deleted');
          setTimeout(() => {
            navigate('/projects');
          }, 1000);
        } catch (error: any) {
          message.error(`Delete failed: ${error.response?.data?.detail || error.message}`);
        } finally {
          setDeleting(false);
        }
      }
    });
  };

  const canEdit = permissions?.permissions?.can_edit || false;
  const canDelete = permissions?.permissions?.can_delete || false;

  return (
    <div>
      {/* 基本设置 */}
      <Card
        title={
          <Space>
            <FolderOpenOutlined />
            <span>Event Settings</span>
          </Space>
        }
        style={{ marginBottom: 16 }}
      >
        {!canEdit && (
          <Alert
            message="No Edit Permission"
            description="Only event administrators and owners can modify event settings"
            type="warning"
            showIcon
            style={{ marginBottom: 16 }}
          />
        )}

        <Form
          form={form}
          layout="vertical"
          onFinish={handleSave}
          disabled={!canEdit}
        >
          <Form.Item
            label="Event Name"
            name="project_name"
            rules={[{ required: true, message: 'Please enter event name' }]}
          >
            <Input placeholder="Event name" />
          </Form.Item>

          <Space size="large" style={{ width: '100%' }}>
            <Form.Item
              label="Event Status"
              name="status"
              rules={[{ required: true, message: 'Please select status' }]}
              style={{ width: 200 }}
            >
              <Select>
                {PROJECT_STATUS_OPTIONS.map(opt => (
                  <Option key={opt.value} value={opt.value}>
                    {opt.label}
                  </Option>
                ))}
              </Select>
            </Form.Item>

            <Form.Item
              label="Priority"
              name="priority"
              style={{ width: 200 }}
            >
              <Select>
                {PROJECT_PRIORITY_OPTIONS.map(opt => (
                  <Option key={opt.value} value={opt.value}>
                    {opt.label}
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Space>

          <Form.Item 
            label="Tickers" 
            name="tickers"
            tooltip="You can add multiple tickers, press Enter after each one"
          >
            <Select
              mode="tags"
              placeholder="e.g., TSLA.US, AAPL.US, 700.HK (Press Enter to add)"
              tokenSeparators={[',', ' ']}
              style={{ width: '100%' }}
            />
          </Form.Item>

          <Form.Item
            label="Description"
            name="description"
          >
            <TextArea rows={4} placeholder="Event description" />
          </Form.Item>

          <Form.Item
            label="Current Position"
            name="current_position"
          >
            <TextArea rows={3} placeholder="Current position description" />
          </Form.Item>

          {canEdit && (
            <Form.Item>
              <Button
                type="primary"
                htmlType="submit"
                icon={<SaveOutlined />}
                loading={saving}
              >
                Save Settings
              </Button>
            </Form.Item>
          )}
        </Form>
      </Card>

      {/* 危险操作区域 */}
      {canDelete && (
        <Card
          title={
            <Space>
              <WarningOutlined style={{ color: '#ff4d4f' }} />
              <span style={{ color: '#ff4d4f' }}>Dangerous Operations</span>
            </Space>
          }
        >
          <Alert
            message="Delete Event"
            description="After deleting, the event and all associated data (files, milestones, members) will be permanently removed. This action cannot be undone."
            type="error"
            showIcon
            style={{ marginBottom: 16 }}
          />

          <Button
            danger
            icon={<DeleteOutlined />}
            onClick={handleDelete}
            loading={deleting}
          >
            Delete This Event
          </Button>
        </Card>
      )}

      {!canDelete && (
        <Card>
          <Alert
            message="Insufficient Permissions"
            description="Only event owners can delete events"
            type="info"
            showIcon
          />
        </Card>
      )}
    </div>
  );
};

export default ProjectSettingsTab;


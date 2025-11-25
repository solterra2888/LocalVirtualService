/**
 * Project Settings Tab Component
 * 项目设置Tab - 项目编辑、归档、危险操作
 */

import React, { useState } from 'react';
import {
  Card,
  Form,
  Input,
  Select,
  DatePicker,
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
  const [archiving, setArchiving] = useState(false);

  // 初始化表单值
  React.useEffect(() => {
    form.setFieldsValue({
      project_name: project.project_name,
      status: project.status,
      priority: project.priority,
      description: project.description,
      investment_thesis: project.investment_thesis,
      target_price: project.target_price,
      current_position: project.current_position,
      target_end_date: project.target_end_date ? dayjs(project.target_end_date) : null,
      actual_end_date: project.actual_end_date ? dayjs(project.actual_end_date) : null
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
        description: values.description,
        investment_thesis: values.investment_thesis,
        target_price: values.target_price,
        current_position: values.current_position,
        target_end_date: values.target_end_date ? values.target_end_date.format('YYYY-MM-DD') : undefined,
        actual_end_date: values.actual_end_date ? values.actual_end_date.format('YYYY-MM-DD') : undefined
      };

      await updateProject(project.id, updateData);
      message.success('项目设置保存成功');
      onUpdate();
    } catch (error: any) {
      message.error(`保存失败: ${error.response?.data?.detail || error.message}`);
    } finally {
      setSaving(false);
    }
  };

  // 归档项目
  const handleArchive = () => {
    Modal.confirm({
      title: '确认归档项目',
      icon: <WarningOutlined />,
      content: (
        <div>
          <Paragraph>
            归档后，项目将：
          </Paragraph>
          <ul>
            <li>状态变更为"已归档"</li>
            <li>不再显示在活跃项目列表中</li>
            <li>数据仍然保留，可以查看历史记录</li>
          </ul>
          <Paragraph strong>
            确定要归档项目"{project.project_name}"吗？
          </Paragraph>
        </div>
      ),
      okText: '确认归档',
      okType: 'danger',
      cancelText: '取消',
      onOk: async () => {
        setArchiving(true);
        try {
          await deleteProject(project.id);
          message.success('项目已归档');
          setTimeout(() => {
            navigate('/projects');
          }, 1000);
        } catch (error: any) {
          message.error(`归档失败: ${error.response?.data?.detail || error.message}`);
        } finally {
          setArchiving(false);
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
            <span>项目设置</span>
          </Space>
        }
        style={{ marginBottom: 16 }}
      >
        {!canEdit && (
          <Alert
            message="您没有编辑权限"
            description="只有项目管理员和负责人可以修改项目设置"
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
            label="项目名称"
            name="project_name"
            rules={[{ required: true, message: '请输入项目名称' }]}
          >
            <Input placeholder="项目名称" />
          </Form.Item>

          <Space size="large" style={{ width: '100%' }}>
            <Form.Item
              label="项目状态"
              name="status"
              rules={[{ required: true, message: '请选择状态' }]}
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
              label="优先级"
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
            label="项目描述"
            name="description"
          >
            <TextArea rows={4} placeholder="项目描述" />
          </Form.Item>

          <Form.Item
            label="投资论点"
            name="investment_thesis"
          >
            <TextArea rows={6} placeholder="投资论点和理由" />
          </Form.Item>

          <Space size="large" style={{ width: '100%' }}>
            <Form.Item
              label="目标价"
              name="target_price"
              style={{ width: 200 }}
            >
              <Input type="number" placeholder="目标价" prefix="$" />
            </Form.Item>

            <Form.Item
              label="目标完成日期"
              name="target_end_date"
              style={{ width: 250 }}
            >
              <DatePicker style={{ width: '100%' }} />
            </Form.Item>

            <Form.Item
              label="实际完成日期"
              name="actual_end_date"
              style={{ width: 250 }}
            >
              <DatePicker style={{ width: '100%' }} />
            </Form.Item>
          </Space>

          <Form.Item
            label="持仓情况"
            name="current_position"
          >
            <TextArea rows={3} placeholder="持仓情况描述" />
          </Form.Item>

          {canEdit && (
            <Form.Item>
              <Button
                type="primary"
                htmlType="submit"
                icon={<SaveOutlined />}
                loading={saving}
              >
                保存设置
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
              <span style={{ color: '#ff4d4f' }}>危险操作</span>
            </Space>
          }
        >
          <Alert
            message="归档项目"
            description="归档后，项目将被移到归档列表，不再显示在活跃项目中。归档的项目数据仍会保留。"
            type="warning"
            showIcon
            style={{ marginBottom: 16 }}
          />

          <Button
            danger
            icon={<DeleteOutlined />}
            onClick={handleArchive}
            loading={archiving}
          >
            归档此项目
          </Button>
        </Card>
      )}

      {!canDelete && (
        <Card>
          <Alert
            message="权限不足"
            description="只有项目负责人可以归档项目"
            type="info"
            showIcon
          />
        </Card>
      )}
    </div>
  );
};

export default ProjectSettingsTab;


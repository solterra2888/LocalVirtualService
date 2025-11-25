/**
 * Project Overview Tab Component
 * 项目概览Tab - 展示项目基本信息和描述
 */

import React, { useState } from 'react';
import {
  Row,
  Col,
  Card,
  Descriptions,
  Typography,
  Tag,
  Button,
  Space,
  message,
  Modal,
  Form,
  Input,
  Select,
  DatePicker
} from 'antd';
import {
  EditOutlined,
  CalendarOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';

import {
  InvestmentProject,
  ProjectUpdateRequest,
  PROJECT_STATUS_OPTIONS,
  PROJECT_PRIORITY_OPTIONS
} from '../../types/project';
import { updateProject } from '../../services/projects';

const { Paragraph, Text } = Typography;
const { TextArea } = Input;
const { Option } = Select;

interface ProjectOverviewTabProps {
  project: InvestmentProject;
  permissions: any;
  onUpdate: () => void;
}

const ProjectOverviewTab: React.FC<ProjectOverviewTabProps> = ({
  project,
  permissions,
  onUpdate
}) => {
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [loading, setLoading] = useState(false);
  const [form] = Form.useForm();

  // 打开编辑模态框
  const handleEdit = () => {
    form.setFieldsValue({
      project_name: project.project_name,
      status: project.status,
      priority: project.priority,
      description: project.description,
      investment_thesis: project.investment_thesis,
      target_price: project.target_price,
      current_position: project.current_position,
      target_end_date: project.target_end_date ? dayjs(project.target_end_date) : null
    });
    setEditModalVisible(true);
  };

  // 保存编辑
  const handleSave = async (values: any) => {
    setLoading(true);
    try {
      const updateData: ProjectUpdateRequest = {
        project_name: values.project_name,
        status: values.status,
        priority: values.priority,
        description: values.description,
        investment_thesis: values.investment_thesis,
        target_price: values.target_price,
        current_position: values.current_position,
        target_end_date: values.target_end_date ? values.target_end_date.format('YYYY-MM-DD') : undefined
      };

      await updateProject(project.id, updateData);
      message.success('项目信息更新成功');
      setEditModalVisible(false);
      onUpdate();
    } catch (error: any) {
      message.error(`更新失败: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const canEdit = permissions?.permissions?.can_edit || false;

  return (
    <div>
      {/* 基本信息卡片 */}
      <Card
        title={
          <Space>
            <span>基本信息</span>
            {canEdit && (
              <Button
                type="primary"
                size="small"
                icon={<EditOutlined />}
                onClick={handleEdit}
              >
                编辑
              </Button>
            )}
          </Space>
        }
        style={{ marginBottom: 16 }}
      >
        <Descriptions column={2} bordered>
          <Descriptions.Item label="项目名称" span={2}>
            <Text strong style={{ fontSize: '16px' }}>{project.project_name}</Text>
          </Descriptions.Item>
          
          <Descriptions.Item label="项目编号">
            <Text code>{project.project_code}</Text>
          </Descriptions.Item>
          
          <Descriptions.Item label="项目类型">
            <Tag color="blue">
              {project.project_type === 'ticker' && '📊 标的研究'}
              {project.project_type === 'industry' && '🏭 行业研究'}
              {project.project_type === 'sector' && '📈 板块研究'}
            </Tag>
          </Descriptions.Item>

          {project.ticker_symbol && (
            <Descriptions.Item label="股票代码">
              <Tag color="geekblue">{project.ticker_symbol}</Tag>
            </Descriptions.Item>
          )}

          {project.industry && (
            <Descriptions.Item label="行业">
              <Tag color="cyan">{project.industry}</Tag>
            </Descriptions.Item>
          )}

          {project.sector && (
            <Descriptions.Item label="板块">
              <Tag color="purple">{project.sector}</Tag>
            </Descriptions.Item>
          )}

          {project.region && (
            <Descriptions.Item label="区域">
              <Tag>{project.region}</Tag>
            </Descriptions.Item>
          )}

          <Descriptions.Item label="状态">
            {PROJECT_STATUS_OPTIONS.find(opt => opt.value === project.status)?.label}
          </Descriptions.Item>

          <Descriptions.Item label="优先级">
            <Tag color={PROJECT_PRIORITY_OPTIONS.find(opt => opt.value === project.priority)?.color}>
              {PROJECT_PRIORITY_OPTIONS.find(opt => opt.value === project.priority)?.label}
            </Tag>
          </Descriptions.Item>

          {project.target_price && (
            <Descriptions.Item label="目标价">
              ${project.target_price.toFixed(2)}
            </Descriptions.Item>
          )}

          {project.current_position && (
            <Descriptions.Item label="持仓情况">
              {project.current_position}
            </Descriptions.Item>
          )}

          <Descriptions.Item label="开始日期">
            {project.start_date ? (
              <Space>
                <CalendarOutlined />
                {dayjs(project.start_date).format('YYYY-MM-DD')}
              </Space>
            ) : '-'}
          </Descriptions.Item>

          <Descriptions.Item label="目标完成日期">
            {project.target_end_date ? (
              <Space>
                <CalendarOutlined />
                {dayjs(project.target_end_date).format('YYYY-MM-DD')}
              </Space>
            ) : '-'}
          </Descriptions.Item>

          <Descriptions.Item label="创建时间">
            {dayjs(project.created_at).format('YYYY-MM-DD HH:mm')}
          </Descriptions.Item>

          <Descriptions.Item label="最后更新">
            {dayjs(project.updated_at).format('YYYY-MM-DD HH:mm')}
          </Descriptions.Item>
        </Descriptions>
      </Card>

      {/* 项目描述 */}
      <Row gutter={16}>
        <Col span={12}>
          <Card title="项目描述" style={{ height: '100%' }}>
            {project.description ? (
              <Paragraph style={{ whiteSpace: 'pre-wrap' }}>
                {project.description}
              </Paragraph>
            ) : (
              <Text type="secondary">暂无项目描述</Text>
            )}
          </Card>
        </Col>
        
        <Col span={12}>
          <Card title="投资论点" style={{ height: '100%' }}>
            {project.investment_thesis ? (
              <Paragraph style={{ whiteSpace: 'pre-wrap' }}>
                {project.investment_thesis}
              </Paragraph>
            ) : (
              <Text type="secondary">暂无投资论点</Text>
            )}
          </Card>
        </Col>
      </Row>

      {/* 编辑模态框 */}
      <Modal
        title="编辑项目信息"
        open={editModalVisible}
        onOk={() => form.submit()}
        onCancel={() => setEditModalVisible(false)}
        width={800}
        okText="保存"
        cancelText="取消"
        confirmLoading={loading}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleSave}
        >
          <Form.Item
            label="项目名称"
            name="project_name"
            rules={[{ required: true, message: '请输入项目名称' }]}
          >
            <Input placeholder="项目名称" />
          </Form.Item>

          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label="状态"
                name="status"
                rules={[{ required: true, message: '请选择状态' }]}
              >
                <Select>
                  {PROJECT_STATUS_OPTIONS.map(opt => (
                    <Option key={opt.value} value={opt.value}>
                      {opt.label}
                    </Option>
                  ))}
                </Select>
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="优先级" name="priority">
                <Select>
                  {PROJECT_PRIORITY_OPTIONS.map(opt => (
                    <Option key={opt.value} value={opt.value}>
                      {opt.label}
                    </Option>
                  ))}
                </Select>
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={12}>
              <Form.Item label="目标价" name="target_price">
                <Input type="number" placeholder="目标价" prefix="$" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="目标完成日期" name="target_end_date">
                <DatePicker style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item label="持仓情况" name="current_position">
            <TextArea rows={2} placeholder="持仓情况描述" />
          </Form.Item>

          <Form.Item label="项目描述" name="description">
            <TextArea rows={3} placeholder="项目描述" />
          </Form.Item>

          <Form.Item label="投资论点" name="investment_thesis">
            <TextArea rows={4} placeholder="投资论点和理由" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default ProjectOverviewTab;


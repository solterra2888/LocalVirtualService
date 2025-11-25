/**
 * Project Milestones Tab Component
 * 项目里程碑管理Tab - 会议纪要、决策记录等
 */

import React, { useState, useEffect } from 'react';
import {
  Timeline,
  Card,
  Button,
  Space,
  Tag,
  Modal,
  Form,
  Input,
  Select,
  DatePicker,
  message,
  Popconfirm,
  Empty,
  Row,
  Col,
  Typography
} from 'antd';
import {
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  FileTextOutlined,
  CheckCircleOutlined,
  ClockCircleOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';
import ReactMarkdown from 'react-markdown';
import { api } from '../../services/api';

import {
  ProjectMilestone,
  MilestoneCreateRequest,
  MilestoneUpdateRequest,
  MILESTONE_TYPE_OPTIONS,
  DECISION_TYPE_OPTIONS
} from '../../types/project';

const { TextArea } = Input;
const { Option } = Select;
const { Title, Text, Paragraph } = Typography;

interface ProjectMilestonesTabProps {
  projectId: number;
  permissions: any;
}

// API服务

const getProjectMilestones = async (projectId: number): Promise<ProjectMilestone[]> => {
  return await api.get(`/projects/${projectId}/milestones`);
};

const createMilestone = async (
  projectId: number,
  data: MilestoneCreateRequest
): Promise<ProjectMilestone> => {
  return await api.post(`/projects/${projectId}/milestones`, data);
};

const updateMilestone = async (
  projectId: number,
  milestoneId: number,
  data: MilestoneUpdateRequest
): Promise<ProjectMilestone> => {
  return await api.put(`/projects/${projectId}/milestones/${milestoneId}`, data);
};

const deleteMilestone = async (
  projectId: number,
  milestoneId: number
): Promise<any> => {
  return await api.delete(`/projects/${projectId}/milestones/${milestoneId}`);
};

const ProjectMilestonesTab: React.FC<ProjectMilestonesTabProps> = ({
  projectId,
  permissions
}) => {
  const [milestones, setMilestones] = useState<ProjectMilestone[]>([]);
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [viewModalVisible, setViewModalVisible] = useState(false);
  const [currentMilestone, setCurrentMilestone] = useState<ProjectMilestone | null>(null);
  const [createForm] = Form.useForm();
  const [editForm] = Form.useForm();

  // 加载里程碑列表
  const loadMilestones = async () => {
    try {
      const data = await getProjectMilestones(projectId);
      setMilestones(data);
    } catch (error: any) {
      message.error(`加载里程碑失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  useEffect(() => {
    loadMilestones();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectId]);

  // 创建里程碑
  const handleCreate = async (values: any) => {
    try {
      const milestoneData: MilestoneCreateRequest = {
        milestone_type: values.milestone_type,
        title: values.title,
        content: values.content,
        decision_type: values.decision_type,
        decision_rationale: values.decision_rationale,
        milestone_date: values.milestone_date ? values.milestone_date.format('YYYY-MM-DD') : undefined
      };

      await createMilestone(projectId, milestoneData);
      message.success('里程碑创建成功');
      setCreateModalVisible(false);
      createForm.resetFields();
      loadMilestones();
    } catch (error: any) {
      message.error(`创建失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 打开编辑模态框
  const handleEdit = (milestone: ProjectMilestone) => {
    setCurrentMilestone(milestone);
    editForm.setFieldsValue({
      milestone_type: milestone.milestone_type,
      title: milestone.title,
      content: milestone.content,
      decision_type: milestone.decision_type,
      decision_rationale: milestone.decision_rationale,
      milestone_date: milestone.milestone_date ? dayjs(milestone.milestone_date) : null
    });
    setEditModalVisible(true);
  };

  // 更新里程碑
  const handleUpdate = async (values: any) => {
    if (!currentMilestone) return;

    try {
      const updateData: MilestoneUpdateRequest = {
        milestone_type: values.milestone_type,
        title: values.title,
        content: values.content,
        decision_type: values.decision_type,
        decision_rationale: values.decision_rationale,
        milestone_date: values.milestone_date ? values.milestone_date.format('YYYY-MM-DD') : undefined
      };

      await updateMilestone(projectId, currentMilestone.id, updateData);
      message.success('里程碑更新成功');
      setEditModalVisible(false);
      loadMilestones();
    } catch (error: any) {
      message.error(`更新失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 删除里程碑
  const handleDelete = async (milestoneId: number, title: string) => {
    try {
      await deleteMilestone(projectId, milestoneId);
      message.success(`已删除里程碑: ${title}`);
      loadMilestones();
    } catch (error: any) {
      message.error(`删除失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 查看详情
  const handleView = (milestone: ProjectMilestone) => {
    setCurrentMilestone(milestone);
    setViewModalVisible(true);
  };

  // 获取里程碑类型信息
  const getMilestoneTypeOption = (type: string) => {
    return MILESTONE_TYPE_OPTIONS.find(opt => opt.value === type);
  };

  // 获取决策类型信息
  const getDecisionTypeOption = (type: string) => {
    return DECISION_TYPE_OPTIONS.find(opt => opt.value === type);
  };

  const canEdit = permissions?.permissions?.can_edit || false;

  return (
    <div>
      {/* 操作栏 */}
      <Space style={{ marginBottom: 16 }}>
        {canEdit && (
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setCreateModalVisible(true)}
          >
            创建里程碑
          </Button>
        )}
      </Space>

      {/* 里程碑时间线 - 左右交替显示 */}
      {milestones.length > 0 ? (
        <Timeline mode="alternate">
          {milestones.map((milestone, index) => {
            const typeOption = getMilestoneTypeOption(milestone.milestone_type);
            const decisionOption = milestone.decision_type
              ? getDecisionTypeOption(milestone.decision_type)
              : null;
            const isLeft = index % 2 === 0;

            return (
              <Timeline.Item
                key={milestone.id}
                label={
                  <div style={{ 
                    textAlign: isLeft ? 'right' : 'left',
                    paddingRight: isLeft ? 12 : 0,
                    paddingLeft: isLeft ? 0 : 12,
                    fontWeight: 500,
                    color: '#666'
                  }}>
                    {milestone.milestone_date
                      ? dayjs(milestone.milestone_date).format('YYYY-MM-DD')
                      : dayjs(milestone.created_at).format('YYYY-MM-DD')}
                  </div>
                }
                dot={
                  milestone.milestone_type === 'decision' ? (
                    <CheckCircleOutlined style={{ fontSize: 16, color: '#52c41a' }} />
                  ) : (
                    <ClockCircleOutlined style={{ fontSize: 16, color: '#1890ff' }} />
                  )
                }
              >
                <Card
                  size="small"
                  style={{ 
                    maxWidth: 360,
                    marginLeft: isLeft ? 0 : 'auto',
                    marginRight: isLeft ? 'auto' : 0,
                    boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
                    borderRadius: 8
                  }}
                  title={
                    <Space size={4} wrap>
                      <span>{typeOption?.icon}</span>
                      <span style={{ fontSize: 14 }}>{milestone.title}</span>
                      {decisionOption && (
                        <Tag color={decisionOption.color} style={{ marginLeft: 4 }}>
                          {decisionOption.label}
                        </Tag>
                      )}
                    </Space>
                  }
                  extra={
                    canEdit && (
                      <Space size={0}>
                        <Button
                          type="link"
                          size="small"
                          icon={<FileTextOutlined />}
                          onClick={() => handleView(milestone)}
                          title="查看"
                        />
                        <Button
                          type="link"
                          size="small"
                          icon={<EditOutlined />}
                          onClick={() => handleEdit(milestone)}
                          title="编辑"
                        />
                        <Popconfirm
                          title="确认删除里程碑？"
                          onConfirm={() => handleDelete(milestone.id, milestone.title)}
                          okText="确认"
                          cancelText="取消"
                        >
                          <Button
                            type="link"
                            danger
                            size="small"
                            icon={<DeleteOutlined />}
                            title="删除"
                          />
                        </Popconfirm>
                      </Space>
                    )
                  }
                >
                  <Paragraph
                    ellipsis={{ rows: 2, expandable: true }}
                    style={{ marginBottom: 8, fontSize: 13 }}
                  >
                    {milestone.content || '暂无内容'}
                  </Paragraph>
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    {milestone.created_by_username || `用户${milestone.created_by}`}
                    {' · '}
                    {dayjs(milestone.created_at).format('MM-DD HH:mm')}
                  </Text>
                </Card>
              </Timeline.Item>
            );
          })}
        </Timeline>
      ) : (
        <Empty
          description="暂无里程碑，点击「创建里程碑」开始记录项目进展"
          style={{ padding: '60px 0' }}
        />
      )}

      {/* 创建里程碑模态框 */}
      <Modal
        title="创建里程碑"
        open={createModalVisible}
        onOk={() => createForm.submit()}
        onCancel={() => {
          setCreateModalVisible(false);
          createForm.resetFields();
        }}
        width={800}
        okText="创建"
        cancelText="取消"
      >
        <Form
          form={createForm}
          layout="vertical"
          onFinish={handleCreate}
        >
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label="里程碑类型"
                name="milestone_type"
                rules={[{ required: true, message: '请选择里程碑类型' }]}
              >
                <Select placeholder="选择类型">
                  {MILESTONE_TYPE_OPTIONS.map(opt => (
                    <Option key={opt.value} value={opt.value}>
                      {opt.icon} {opt.label}
                    </Option>
                  ))}
                </Select>
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item
                label="日期"
                name="milestone_date"
              >
                <DatePicker style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item
            label="标题"
            name="title"
            rules={[{ required: true, message: '请输入标题' }]}
          >
            <Input placeholder="例如：Q1季度复盘会议" />
          </Form.Item>

          <Form.Item
            label="内容"
            name="content"
            tooltip="支持Markdown格式"
          >
            <TextArea
              rows={8}
              placeholder="详细记录会议纪要、研究发现、决策理由等内容...&#10;支持Markdown格式"
            />
          </Form.Item>

          <Form.Item
            noStyle
            shouldUpdate={(prevValues, currentValues) =>
              prevValues.milestone_type !== currentValues.milestone_type
            }
          >
            {({ getFieldValue }) =>
              getFieldValue('milestone_type') === 'decision' && (
                <>
                  <Form.Item
                    label="决策类型"
                    name="decision_type"
                  >
                    <Select placeholder="选择决策类型">
                      {DECISION_TYPE_OPTIONS.map(opt => (
                        <Option key={opt.value} value={opt.value}>
                          {opt.label}
                        </Option>
                      ))}
                    </Select>
                  </Form.Item>

                  <Form.Item
                    label="决策理由"
                    name="decision_rationale"
                  >
                    <TextArea rows={4} placeholder="说明做出该决策的原因..." />
                  </Form.Item>
                </>
              )
            }
          </Form.Item>
        </Form>
      </Modal>

      {/* 编辑里程碑模态框 */}
      <Modal
        title="编辑里程碑"
        open={editModalVisible}
        onOk={() => editForm.submit()}
        onCancel={() => {
          setEditModalVisible(false);
          setCurrentMilestone(null);
        }}
        width={800}
        okText="保存"
        cancelText="取消"
      >
        <Form
          form={editForm}
          layout="vertical"
          onFinish={handleUpdate}
        >
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label="里程碑类型"
                name="milestone_type"
                rules={[{ required: true, message: '请选择里程碑类型' }]}
              >
                <Select>
                  {MILESTONE_TYPE_OPTIONS.map(opt => (
                    <Option key={opt.value} value={opt.value}>
                      {opt.icon} {opt.label}
                    </Option>
                  ))}
                </Select>
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="日期" name="milestone_date">
                <DatePicker style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item
            label="标题"
            name="title"
            rules={[{ required: true, message: '请输入标题' }]}
          >
            <Input />
          </Form.Item>

          <Form.Item label="内容" name="content">
            <TextArea rows={8} />
          </Form.Item>

          <Form.Item
            noStyle
            shouldUpdate={(prevValues, currentValues) =>
              prevValues.milestone_type !== currentValues.milestone_type
            }
          >
            {({ getFieldValue }) =>
              getFieldValue('milestone_type') === 'decision' && (
                <>
                  <Form.Item label="决策类型" name="decision_type">
                    <Select>
                      {DECISION_TYPE_OPTIONS.map(opt => (
                        <Option key={opt.value} value={opt.value}>
                          {opt.label}
                        </Option>
                      ))}
                    </Select>
                  </Form.Item>

                  <Form.Item label="决策理由" name="decision_rationale">
                    <TextArea rows={4} />
                  </Form.Item>
                </>
              )
            }
          </Form.Item>
        </Form>
      </Modal>

      {/* 查看详情模态框 */}
      <Modal
        title={
          currentMilestone && (
            <Space>
              <span>{getMilestoneTypeOption(currentMilestone.milestone_type)?.icon}</span>
              <span>{currentMilestone.title}</span>
            </Space>
          )
        }
        open={viewModalVisible}
        onCancel={() => {
          setViewModalVisible(false);
          setCurrentMilestone(null);
        }}
        width={800}
        footer={[
          <Button key="close" onClick={() => setViewModalVisible(false)}>
            关闭
          </Button>
        ]}
      >
        {currentMilestone && (
          <div>
            <Space direction="vertical" size="large" style={{ width: '100%' }}>
              <div>
                <Text type="secondary">日期：</Text>
                <Text>
                  {currentMilestone.milestone_date
                    ? dayjs(currentMilestone.milestone_date).format('YYYY-MM-DD')
                    : dayjs(currentMilestone.created_at).format('YYYY-MM-DD')}
                </Text>
              </div>

              {currentMilestone.decision_type && (
                <div>
                  <Text type="secondary">决策类型：</Text>
                  <Tag color={getDecisionTypeOption(currentMilestone.decision_type)?.color}>
                    {getDecisionTypeOption(currentMilestone.decision_type)?.label}
                  </Tag>
                </div>
              )}

              {currentMilestone.content && (
                <div>
                  <Title level={5}>内容</Title>
                  <Card>
                    <ReactMarkdown>{currentMilestone.content}</ReactMarkdown>
                  </Card>
                </div>
              )}

              {currentMilestone.decision_rationale && (
                <div>
                  <Title level={5}>决策理由</Title>
                  <Card>
                    <Paragraph>{currentMilestone.decision_rationale}</Paragraph>
                  </Card>
                </div>
              )}

              <div>
                <Text type="secondary">
                  创建者: {currentMilestone.created_by_username || `用户${currentMilestone.created_by}`}
                  {' | '}
                  创建时间: {dayjs(currentMilestone.created_at).format('YYYY-MM-DD HH:mm')}
                </Text>
              </div>
            </Space>
          </div>
        )}
      </Modal>
    </div>
  );
};

export default ProjectMilestonesTab;


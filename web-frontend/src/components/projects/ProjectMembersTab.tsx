/**
 * Project Members Tab Component
 * 项目成员管理Tab - 成员列表、邀请、角色管理
 */

import React, { useState, useEffect } from 'react';
import {
  Table,
  Button,
  Space,
  Tag,
  Modal,
  Form,
  Select,
  message,
  Popconfirm,
  Row,
  Col,
  Tooltip,
  Switch,
  Input
} from 'antd';
import {
  UserAddOutlined,
  EditOutlined,
  DeleteOutlined,
  CrownOutlined,
  SafetyOutlined,
  TeamOutlined,
  EyeOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';

import {
  ProjectMember,
  MemberRole,
  MemberInviteRequest,
  MemberUpdateRequest,
  InvestmentProject,
  MEMBER_ROLE_OPTIONS
} from '../../types/project';
import {
  getProjectMembers,
  inviteMembers,
  updateMember,
  removeMember
} from '../../services/projects';

const { Option } = Select;

interface ProjectMembersTabProps {
  projectId: number;
  project: InvestmentProject;
  permissions: any;
  onUpdate: () => void;
}

const ProjectMembersTab: React.FC<ProjectMembersTabProps> = ({
  projectId,
  project,
  permissions,
  onUpdate
}) => {
  const [members, setMembers] = useState<ProjectMember[]>([]);
  const [loading, setLoading] = useState(false);
  const [inviteModalVisible, setInviteModalVisible] = useState(false);
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [currentMember, setCurrentMember] = useState<ProjectMember | null>(null);
  const [inviteForm] = Form.useForm();
  const [editForm] = Form.useForm();

  // 加载成员列表
  const loadMembers = async () => {
    setLoading(true);
    try {
      const data = await getProjectMembers(projectId);
      setMembers(data);
    } catch (error: any) {
      message.error(`加载成员列表失败: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadMembers();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectId]);

  // 邀请成员
  const handleInvite = async (values: any) => {
    try {
      const inviteData: MemberInviteRequest = {
        user_ids: values.user_ids,
        role: values.role,
        can_edit: values.can_edit !== false,
        can_invite: values.can_invite || false,
        message: values.message
      };

      await inviteMembers(projectId, inviteData);
      message.success('成员邀请成功');
      setInviteModalVisible(false);
      inviteForm.resetFields();
      loadMembers();
      onUpdate();
    } catch (error: any) {
      message.error(`邀请失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 打开编辑成员模态框
  const handleEditMember = (member: ProjectMember) => {
    setCurrentMember(member);
    editForm.setFieldsValue({
      role: member.role,
      can_edit: member.can_edit,
      can_invite: member.can_invite
    });
    setEditModalVisible(true);
  };

  // 更新成员信息
  const handleUpdateMember = async (values: any) => {
    if (!currentMember) return;

    try {
      const updateData: MemberUpdateRequest = {
        role: values.role,
        can_edit: values.can_edit,
        can_invite: values.can_invite
      };

      await updateMember(projectId, currentMember.user_id, updateData);
      message.success('成员信息更新成功');
      setEditModalVisible(false);
      loadMembers();
      onUpdate();
    } catch (error: any) {
      message.error(`更新失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 移除成员
  const handleRemoveMember = async (userId: number, username: string) => {
    try {
      await removeMember(projectId, userId);
      message.success(`已移除成员: ${username}`);
      loadMembers();
      onUpdate();
    } catch (error: any) {
      message.error(`移除失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 获取角色图标
  const getRoleIcon = (role: MemberRole) => {
    switch (role) {
      case MemberRole.OWNER:
        return <CrownOutlined style={{ color: '#faad14' }} />;
      case MemberRole.ADMIN:
        return <SafetyOutlined style={{ color: '#1890ff' }} />;
      case MemberRole.MEMBER:
        return <TeamOutlined style={{ color: '#52c41a' }} />;
      case MemberRole.VIEWER:
        return <EyeOutlined style={{ color: '#8c8c8c' }} />;
      default:
        return <TeamOutlined />;
    }
  };

  // 获取角色显示文本
  const getRoleText = (role: MemberRole) => {
    const option = MEMBER_ROLE_OPTIONS.find(opt => opt.value === role);
    return option?.label || role;
  };

  // 获取角色颜色
  const getRoleColor = (role: MemberRole) => {
    switch (role) {
      case MemberRole.OWNER:
        return 'gold';
      case MemberRole.ADMIN:
        return 'blue';
      case MemberRole.MEMBER:
        return 'green';
      case MemberRole.VIEWER:
        return 'default';
      default:
        return 'default';
    }
  };

  const canInvite = permissions?.permissions?.can_invite || false;
  const canManage = permissions?.permissions?.can_manage || false;

  // 表格列定义
  const columns: ColumnsType<ProjectMember> = [
    {
      title: '成员',
      key: 'member',
      width: 200,
      render: (_, record) => (
        <Space>
          {getRoleIcon(record.role)}
          <span>{record.username || `用户${record.user_id}`}</span>
        </Space>
      )
    },
    {
      title: '角色',
      dataIndex: 'role',
      key: 'role',
      width: 120,
      render: (role: MemberRole) => (
        <Tag color={getRoleColor(role)} icon={getRoleIcon(role)}>
          {getRoleText(role)}
        </Tag>
      )
    },
    {
      title: '权限',
      key: 'permissions',
      width: 200,
      render: (_, record) => (
        <Space>
          {record.can_edit && <Tag color="green">可编辑</Tag>}
          {record.can_invite && <Tag color="blue">可邀请</Tag>}
          {!record.can_edit && !record.can_invite && <Tag>仅查看</Tag>}
        </Space>
      )
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => {
        const statusMap: Record<string, { text: string; color: string }> = {
          active: { text: '活跃', color: 'success' },
          inactive: { text: '停用', color: 'default' },
          invited: { text: '邀请中', color: 'processing' }
        };
        const statusInfo = statusMap[status] || { text: status, color: 'default' };
        return <Tag color={statusInfo.color}>{statusInfo.text}</Tag>;
      }
    },
    {
      title: '加入时间',
      dataIndex: 'joined_at',
      key: 'joined_at',
      width: 150,
      render: (date: string) => dayjs(date).format('YYYY-MM-DD')
    },
    {
      title: '操作',
      key: 'action',
      width: 150,
      fixed: 'right',
      render: (_, record) => {
        // Owner不能被编辑或移除
        if (record.role === MemberRole.OWNER) {
          return <Tag color="gold">项目负责人</Tag>;
        }

        if (!canManage) {
          return <span>-</span>;
        }

        return (
          <Space>
            <Tooltip title="编辑成员">
              <Button
                type="link"
                size="small"
                icon={<EditOutlined />}
                onClick={() => handleEditMember(record)}
              />
            </Tooltip>
            <Popconfirm
              title="确认移除成员？"
              description={`确定要从项目中移除 ${record.username} 吗？`}
              onConfirm={() => handleRemoveMember(record.user_id, record.username || `用户${record.user_id}`)}
              okText="确认"
              cancelText="取消"
            >
              <Tooltip title="移除成员">
                <Button
                  type="link"
                  danger
                  size="small"
                  icon={<DeleteOutlined />}
                />
              </Tooltip>
            </Popconfirm>
          </Space>
        );
      }
    }
  ];

  return (
    <div>
      {/* 操作栏 */}
      <Row justify="space-between" align="middle" style={{ marginBottom: 16 }}>
        <Col>
          <Space>
            <TeamOutlined style={{ fontSize: 16 }} />
            <span>共 {members.length} 位成员</span>
          </Space>
        </Col>
        <Col>
          {canInvite && (
            <Button
              type="primary"
              icon={<UserAddOutlined />}
              onClick={() => setInviteModalVisible(true)}
            >
              邀请成员
            </Button>
          )}
        </Col>
      </Row>

      {/* 成员列表 */}
      <Table
        columns={columns}
        dataSource={members}
        rowKey="id"
        loading={loading}
        pagination={false}
        bordered
      />

      {/* 邀请成员模态框 */}
      <Modal
        title="邀请成员加入项目"
        open={inviteModalVisible}
        onOk={() => inviteForm.submit()}
        onCancel={() => {
          setInviteModalVisible(false);
          inviteForm.resetFields();
        }}
        width={600}
        okText="发送邀请"
        cancelText="取消"
      >
        <Form
          form={inviteForm}
          layout="vertical"
          onFinish={handleInvite}
        >
          <Form.Item
            label="用户ID"
            name="user_ids"
            rules={[{ required: true, message: '请输入用户ID' }]}
            tooltip="输入要邀请的用户ID，多个用逗号分隔"
          >
            <Select
              mode="tags"
              placeholder="输入用户ID，如: 2, 3, 5"
              tokenSeparators={[',']}
            />
          </Form.Item>

          <Form.Item
            label="角色"
            name="role"
            initialValue={MemberRole.MEMBER}
            rules={[{ required: true, message: '请选择角色' }]}
          >
            <Select>
              {MEMBER_ROLE_OPTIONS.filter(opt => opt.value !== MemberRole.OWNER).map(opt => (
                <Option key={opt.value} value={opt.value}>
                  <Space>
                    <span>{opt.label}</span>
                    <span style={{ color: '#8c8c8c', fontSize: '12px' }}>
                      {opt.description}
                    </span>
                  </Space>
                </Option>
              ))}
            </Select>
          </Form.Item>

          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label="可编辑内容"
                name="can_edit"
                valuePropName="checked"
                initialValue={true}
              >
                <Switch checkedChildren="是" unCheckedChildren="否" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item
                label="可邀请成员"
                name="can_invite"
                valuePropName="checked"
                initialValue={false}
              >
                <Switch checkedChildren="是" unCheckedChildren="否" />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item label="邀请消息（可选）" name="message">
            <Input.TextArea
              rows={3}
              placeholder="向成员说明项目情况和邀请原因..."
            />
          </Form.Item>
        </Form>
      </Modal>

      {/* 编辑成员模态框 */}
      <Modal
        title="编辑成员信息"
        open={editModalVisible}
        onOk={() => editForm.submit()}
        onCancel={() => {
          setEditModalVisible(false);
          setCurrentMember(null);
        }}
        width={500}
        okText="保存"
        cancelText="取消"
      >
        <Form
          form={editForm}
          layout="vertical"
          onFinish={handleUpdateMember}
        >
          <Form.Item
            label="角色"
            name="role"
            rules={[{ required: true, message: '请选择角色' }]}
          >
            <Select>
              {MEMBER_ROLE_OPTIONS.filter(opt => opt.value !== MemberRole.OWNER).map(opt => (
                <Option key={opt.value} value={opt.value}>
                  <Space>
                    <span>{opt.label}</span>
                    <span style={{ color: '#8c8c8c', fontSize: '12px' }}>
                      {opt.description}
                    </span>
                  </Space>
                </Option>
              ))}
            </Select>
          </Form.Item>

          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label="可编辑内容"
                name="can_edit"
                valuePropName="checked"
              >
                <Switch checkedChildren="是" unCheckedChildren="否" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item
                label="可邀请成员"
                name="can_invite"
                valuePropName="checked"
              >
                <Switch checkedChildren="是" unCheckedChildren="否" />
              </Form.Item>
            </Col>
          </Row>
        </Form>
      </Modal>
    </div>
  );
};

export default ProjectMembersTab;


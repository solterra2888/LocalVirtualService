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
import { useDashboardSettings } from '../../contexts/DashboardSettingsContext';
import { getModalStyles } from '../../utils/modalStyles';

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
  const { themeMode } = useDashboardSettings();
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
      message.error(`Failed to load member list: ${error.response?.data?.detail || error.message}`);
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
      message.success('Member invited successfully');
      setInviteModalVisible(false);
      inviteForm.resetFields();
      loadMembers();
      onUpdate();
    } catch (error: any) {
      message.error(`Invite failed: ${error.response?.data?.detail || error.message}`);
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
      message.success('Member information updated successfully');
      setEditModalVisible(false);
      loadMembers();
      onUpdate();
    } catch (error: any) {
      message.error(`Update failed: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 移除成员
  const handleRemoveMember = async (userId: number, username: string) => {
    try {
      await removeMember(projectId, userId);
      message.success(`Member removed: ${username}`);
      loadMembers();
      onUpdate();
    } catch (error: any) {
      message.error(`Remove failed: ${error.response?.data?.detail || error.message}`);
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
      title: 'Member',
      key: 'member',
      width: 200,
      render: (_, record) => (
        <Space>
          {getRoleIcon(record.role)}
          <span>{record.username || `User ${record.user_id}`}</span>
        </Space>
      )
    },
    {
      title: 'Role',
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
      title: 'Permissions',
      key: 'permissions',
      width: 200,
      render: (_, record) => (
        <Space>
          {record.can_edit && <Tag color="green">Can Edit</Tag>}
          {record.can_invite && <Tag color="blue">Can Invite</Tag>}
          {!record.can_edit && !record.can_invite && <Tag>View Only</Tag>}
        </Space>
      )
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: string) => {
        const statusMap: Record<string, { text: string; color: string }> = {
          active: { text: 'Active', color: 'success' },
          inactive: { text: 'Inactive', color: 'default' },
          invited: { text: 'Invited', color: 'processing' }
        };
        const statusInfo = statusMap[status] || { text: status, color: 'default' };
        return <Tag color={statusInfo.color}>{statusInfo.text}</Tag>;
      }
    },
    {
      title: 'Joined At',
      dataIndex: 'joined_at',
      key: 'joined_at',
      width: 150,
      render: (date: string) => dayjs(date).format('YYYY-MM-DD')
    },
    {
      title: 'Actions',
      key: 'action',
      width: 150,
      fixed: 'right',
      render: (_, record) => {
        // Owner不能被编辑或移除
        if (record.role === MemberRole.OWNER) {
          return <Tag color="gold">Event Owner</Tag>;
        }

        if (!canManage) {
          return <span>-</span>;
        }

        return (
          <Space>
            <Tooltip title="Edit Member">
              <Button
                type="link"
                size="small"
                icon={<EditOutlined />}
                onClick={() => handleEditMember(record)}
              />
            </Tooltip>
            <Popconfirm
              title="Confirm Remove Member?"
              description={`Are you sure you want to remove ${record.username} from the event?`}
              onConfirm={() => handleRemoveMember(record.user_id, record.username || `User ${record.user_id}`)}
              okText="Confirm"
              cancelText="Cancel"
            >
              <Tooltip title="Remove Member">
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
            <span>Total {members.length} members</span>
          </Space>
        </Col>
        <Col>
          {canInvite && (
            <Button
              type="primary"
              icon={<UserAddOutlined />}
              onClick={() => setInviteModalVisible(true)}
            >
              Invite Member
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
        title="Invite Member to Event"
        open={inviteModalVisible}
        onOk={() => inviteForm.submit()}
        onCancel={() => {
          setInviteModalVisible(false);
          inviteForm.resetFields();
        }}
        width={600}
        okText="Send Invite"
        cancelText="Cancel"
        styles={getModalStyles(themeMode)}
      >
        <Form
          form={inviteForm}
          layout="vertical"
          onFinish={handleInvite}
        >
          <Form.Item
            label="User ID"
            name="user_ids"
            rules={[{ required: true, message: 'Please enter user ID' }]}
            tooltip="Enter user IDs to invite, separated by commas"
          >
            <Select
              mode="tags"
              placeholder="Enter user IDs, e.g., 2, 3, 5"
              tokenSeparators={[',']}
            />
          </Form.Item>

          <Form.Item
            label="Role"
            name="role"
            initialValue={MemberRole.MEMBER}
            rules={[{ required: true, message: 'Please select role' }]}
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
                label="Can Edit Content"
                name="can_edit"
                valuePropName="checked"
                initialValue={true}
              >
                <Switch checkedChildren="Yes" unCheckedChildren="No" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item
                label="Can Invite Members"
                name="can_invite"
                valuePropName="checked"
                initialValue={false}
              >
                <Switch checkedChildren="Yes" unCheckedChildren="No" />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item label="Invitation Message (Optional)" name="message">
            <Input.TextArea
              rows={3}
              placeholder="Explain the event situation and invitation reason to members..."
            />
          </Form.Item>
        </Form>
      </Modal>

      {/* 编辑成员模态框 */}
      <Modal
        title="Edit Member Information"
        open={editModalVisible}
        onOk={() => editForm.submit()}
        onCancel={() => {
          setEditModalVisible(false);
          setCurrentMember(null);
        }}
        width={500}
        okText="Save"
        cancelText="Cancel"
        styles={getModalStyles(themeMode)}
      >
        <Form
          form={editForm}
          layout="vertical"
          onFinish={handleUpdateMember}
        >
          <Form.Item
            label="Role"
            name="role"
            rules={[{ required: true, message: 'Please select role' }]}
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
                label="Can Edit Content"
                name="can_edit"
                valuePropName="checked"
              >
                <Switch checkedChildren="Yes" unCheckedChildren="No" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item
                label="Can Invite Members"
                name="can_invite"
                valuePropName="checked"
              >
                <Switch checkedChildren="Yes" unCheckedChildren="No" />
              </Form.Item>
            </Col>
          </Row>
        </Form>
      </Modal>
    </div>
  );
};

export default ProjectMembersTab;


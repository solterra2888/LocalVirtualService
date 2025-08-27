import React, { useState, useEffect } from 'react';
import { Card, Button, Table, Tag, Space, Typography, Modal, message, Popconfirm } from 'antd';
import { PlusOutlined, EditOutlined, DeleteOutlined, ReloadOutlined } from '@ant-design/icons';
import { User, CreateUserRequest, UpdateUserRequest } from '../types';
import { usersApi } from '../services/users';
import UserModal from '../components/UserModal';
import { getErrorMessage } from '../utils/errorHandler';

const { Title } = Typography;
const { confirm } = Modal;

const UsersPage: React.FC = () => {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(false);
  const [modalVisible, setModalVisible] = useState(false);
  const [modalLoading, setModalLoading] = useState(false);
  const [editingUser, setEditingUser] = useState<User | null>(null);
  const [pagination, setPagination] = useState({
    current: 1,
    pageSize: 20,
    total: 0
  });

  // 加载用户列表
  const loadUsers = async (page: number = 1, pageSize: number = 20) => {
    setLoading(true);
    try {
      const userList = await usersApi.getUsers(page, pageSize);
      setUsers(userList);
      setPagination(prev => ({
        ...prev,
        current: page,
        pageSize,
        total: userList.length // 注意：这里假设API返回所有数据，实际可能需要总数
      }));
    } catch (error: any) {
      console.error('加载用户列表失败:', error);
      const errorMessage = getErrorMessage(error, '加载用户列表失败');
      message.error(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  // 页面初始加载
  useEffect(() => {
    loadUsers();
  }, []);

  // 处理新建用户
  const handleCreateUser = () => {
    setEditingUser(null);
    setModalVisible(true);
  };

  // 处理编辑用户
  const handleEditUser = (user: User) => {
    setEditingUser(user);
    setModalVisible(true);
  };

  // 处理删除用户
  const handleDeleteUser = (user: User) => {
    confirm({
      title: 'Confirm User Deletion',
      content: `Are you sure you want to delete user "${user.username}"? This action cannot be undone.`,
      okText: 'Confirm',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk: async () => {
        try {
          await usersApi.deleteUser(user.id);
          message.success('User deleted successfully');
          loadUsers(pagination.current, pagination.pageSize);
        } catch (error) {
          console.error('删除用户失败:', error);
          // 错误信息已在API层处理
        }
      }
    });
  };

  // 处理用户表单提交
  const handleUserSubmit = async (userData: CreateUserRequest | UpdateUserRequest) => {
    setModalLoading(true);
    try {
      if (editingUser) {
        // 编辑用户
        await usersApi.updateUser(editingUser.id, userData as UpdateUserRequest);
      } else {
        // 创建用户
        await usersApi.createUser(userData as CreateUserRequest);
      }
      setModalVisible(false);
      loadUsers(pagination.current, pagination.pageSize);
    } catch (error) {
      console.error('提交用户数据失败:', error);
      throw error; // 重新抛出错误让Modal组件处理
    } finally {
      setModalLoading(false);
    }
  };

  // 处理模态框关闭
  const handleModalCancel = () => {
    setModalVisible(false);
    setEditingUser(null);
  };

  // 处理模态框成功
  const handleModalSuccess = () => {
    setModalVisible(false);
    setEditingUser(null);
  };

  // 处理分页变化
  const handleTableChange = (pagination: any) => {
    loadUsers(pagination.current, pagination.pageSize);
  };

  // 表格列定义
  const columns = [
    { 
      title: 'ID', 
      dataIndex: 'id', 
      key: 'id',
      width: 80,
      sorter: (a: User, b: User) => a.id - b.id,
      defaultSortOrder: 'ascend' as const
    },
    { 
      title: 'Username', 
      dataIndex: 'username', 
      key: 'username',
      sorter: (a: User, b: User) => a.username.localeCompare(b.username)
    },
    {
      title: 'Role',
      dataIndex: 'is_admin',
      key: 'is_admin',
      sorter: (a: User, b: User) => {
        // Admin排在前面
        if (a.is_admin && !b.is_admin) return -1;
        if (!a.is_admin && b.is_admin) return 1;
        return 0;
      },
      render: (is_admin: boolean) => (
        <Tag color={is_admin ? 'red' : 'blue'}>
          {is_admin ? 'Admin' : 'User'}
        </Tag>
      )
    },
    { 
      title: 'Created Time', 
      dataIndex: 'created_at', 
      key: 'created_at',
      sorter: (a: User, b: User) => {
        if (!a.created_at && !b.created_at) return 0;
        if (!a.created_at) return 1;
        if (!b.created_at) return -1;
        return new Date(a.created_at).getTime() - new Date(b.created_at).getTime();
      },
      render: (text: string) => text ? new Date(text).toLocaleString() : '-'
    },
    { 
      title: 'Last Login', 
      dataIndex: 'last_login', 
      key: 'last_login',
      sorter: (a: User, b: User) => {
        if (!a.last_login && !b.last_login) return 0;
        if (!a.last_login) return 1;
        if (!b.last_login) return -1;
        return new Date(a.last_login).getTime() - new Date(b.last_login).getTime();
      },
      render: (text: string) => text ? new Date(text).toLocaleString() : 'Never logged in'
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 150,
      render: (_: any, record: User) => (
        <Space>
          <Button 
            icon={<EditOutlined />} 
            size="small"
            onClick={() => handleEditUser(record)}
          >
            Edit
          </Button>
          <Popconfirm
            title="Are you sure you want to delete this user?"
            description="This action cannot be undone"
            onConfirm={() => handleDeleteUser(record)}
            okText="Confirm"
            cancelText="Cancel"
          >
            <Button 
              icon={<DeleteOutlined />} 
              size="small" 
              danger
            >
              Delete
            </Button>
          </Popconfirm>
        </Space>
      )
    }
  ];

  return (
    <div>
      <div className="page-header">
        <div className="flex-between">
          <div>
            <Title level={3} className="page-title">User Management</Title>
            <div className="page-description">Manage system users and permissions</div>
          </div>
          <Space>
            <Button 
              icon={<ReloadOutlined />}
              onClick={() => loadUsers(pagination.current, pagination.pageSize)}
              loading={loading}
            >
              Refresh
            </Button>
            <Button 
              type="primary" 
              icon={<PlusOutlined />}
              onClick={handleCreateUser}
            >
              Create User
            </Button>
          </Space>
        </div>
      </div>

      <div className="page-content">
        <Card title="User List">
          <Table
            columns={columns}
            dataSource={users}
            rowKey="id"
            loading={loading}
            pagination={{
              ...pagination,
              showSizeChanger: true,
              showQuickJumper: true,
              showTotal: (total, range) => 
                `${range[0]}-${range[1]} of ${total} items`
            }}
            onChange={handleTableChange}
          />
        </Card>
      </div>

      <UserModal
        visible={modalVisible}
        onCancel={handleModalCancel}
        onSuccess={handleModalSuccess}
        user={editingUser}
        loading={modalLoading}
        onSubmit={handleUserSubmit}
      />
    </div>
  );
};

export default UsersPage; 
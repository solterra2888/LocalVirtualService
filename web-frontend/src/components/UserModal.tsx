import React, { useEffect } from 'react';
import { Modal, Form, Input, Switch, message, Space } from 'antd';
import { User, CreateUserRequest, UpdateUserRequest } from '../types';
import { getErrorMessage } from '../utils/errorHandler';

interface UserModalProps {
  visible: boolean;
  onCancel: () => void;
  onSuccess: () => void;
  user?: User | null; // 如果是编辑模式，传入用户数据
  loading?: boolean;
  onSubmit: (userData: CreateUserRequest | UpdateUserRequest) => Promise<void>;
}

const UserModal: React.FC<UserModalProps> = ({
  visible,
  onCancel,
  onSuccess,
  user,
  loading = false,
  onSubmit
}) => {
  const [form] = Form.useForm();
  const isEdit = !!user;

  useEffect(() => {
    if (visible) {
      if (isEdit && user) {
        // 编辑模式：填充现有用户数据
        form.setFieldsValue({
          username: user.username,
          is_admin: user.is_admin
        });
      } else {
        // 新建模式：重置表单
        form.resetFields();
      }
    }
  }, [visible, user, isEdit, form]);

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields();
      
      let userData: CreateUserRequest | UpdateUserRequest;
      
      if (isEdit) {
        // 编辑模式：只提交修改的字段
        userData = {
          ...(values.username && values.username !== user?.username && { username: values.username }),
          ...(values.password && { password: values.password }),
          is_admin: values.is_admin
        } as UpdateUserRequest;
      } else {
        // 新建模式：提交所有必需字段
        userData = {
          username: values.username,
          password: values.password,
          is_admin: values.is_admin || false
        } as CreateUserRequest;
      }

      await onSubmit(userData);
      message.success(isEdit ? 'User updated successfully' : 'User created successfully');
      form.resetFields();
      onSuccess();
    } catch (error) {
      console.error('提交失败:', error);
      // 错误信息已在API层处理，这里不需要重复显示
    }
  };

  const handleCancel = () => {
    form.resetFields();
    onCancel();
  };

  return (
    <Modal
      title={isEdit ? 'Edit User' : 'Create User'}
      open={visible}
      onCancel={handleCancel}
      onOk={handleSubmit}
      confirmLoading={loading}
      destroyOnClose
    >
      <Form
        form={form}
        layout="vertical"
        preserve={false}
        key={isEdit && user ? `edit-${user.id}` : 'create'}
        initialValues={isEdit && user ? {
          username: user.username,
          is_admin: user.is_admin
        } : {}}
      >
        <Form.Item
          label="Username"
          name="username"
          rules={[
            { required: !isEdit, message: 'Please enter username' },
            { min: 3, message: 'Username must be at least 3 characters' },
            { max: 50, message: 'Username cannot exceed 50 characters' },
            {
              pattern: /^[a-zA-Z0-9_]+$/,
              message: 'Username can only contain letters, numbers and underscores'
            }
          ]}
        >
          <Input 
            placeholder="Please enter username" 
            disabled={isEdit && user?.username === 'admin'} // 只有admin账户不可修改用户名
          />
        </Form.Item>

        <Form.Item
          label={isEdit ? 'New Password (leave blank to keep unchanged)' : 'Password'}
          name="password"
          rules={[
            { required: !isEdit, message: 'Please enter password' },
            { min: 6, message: 'Password must be at least 6 characters' },
            { max: 100, message: 'Password cannot exceed 100 characters' }
          ]}
        >
          <Input.Password 
            placeholder={isEdit ? 'Leave blank to keep current password' : 'Please enter password'} 
          />
        </Form.Item>

        <Form.Item
          label="Admin Privileges"
          name="is_admin"
          valuePropName="checked"
          extra={isEdit && user?.username === 'admin' 
            ? "Admin account must maintain administrator privileges" 
            : "Admins can manage users and perform all operations"
          }
        >
          <Switch 
            checkedChildren="Yes" 
            unCheckedChildren="No" 
            disabled={isEdit && user?.username === 'admin'} // admin账户的管理员权限不可修改
          />
        </Form.Item>
      </Form>
    </Modal>
  );
};

export default UserModal; 
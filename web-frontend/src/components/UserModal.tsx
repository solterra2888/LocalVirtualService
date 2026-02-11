import React, { useEffect } from 'react';
import { Modal, Form, Input, Switch, message, ConfigProvider, theme as antdTheme } from 'antd';
import { User, CreateUserRequest, UpdateUserRequest } from '../types';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

interface UserModalProps {
  visible: boolean;
  onCancel: () => void;
  onSuccess: () => void;
  user?: User | null; // 如果是编辑模式，传入用户数据
  loading?: boolean;
  onSubmit: (userData: CreateUserRequest | UpdateUserRequest) => Promise<void>;
}

// Aligned with FRONTEND_STYLE_GUIDE: semantic colors + Behance Modal
const THEME_COLORS = {
  light: {
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    inputBg: undefined as string | undefined,
  },
  dark: {
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
    inputBg: '#1E202B',
  },
} as const;

const UserModal: React.FC<UserModalProps> = ({
  visible,
  onCancel,
  onSuccess,
  user,
  loading = false,
  onSubmit
}) => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
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

  const modalStyles = isDark
    ? {
        content: {
          borderRadius: 12,
          boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
          border: '1px solid rgba(255, 255, 255, 0.06)',
          backgroundColor: colors.cardBg,
        },
        header: {
          color: colors.text,
          borderBottomColor: colors.cardBorder,
        },
        body: {
          color: colors.text,
        },
      }
    : undefined;

  const inputStyle = isDark && colors.inputBg
    ? { backgroundColor: colors.inputBg }
    : undefined;

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
            borderRadius: 8,
            borderRadiusLG: 12,
          },
        }),
      }}
    >
      <Modal
        title={isEdit ? 'Edit User' : 'Create User'}
        open={visible}
        onCancel={handleCancel}
        onOk={handleSubmit}
        confirmLoading={loading}
        destroyOnClose
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
        <Form
          form={form}
          layout="vertical"
          preserve={false}
          key={isEdit && user ? `edit-${user.id}` : 'create'}
          initialValues={isEdit && user ? { username: user.username, is_admin: user.is_admin } : {}}
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
                message: 'Username can only contain letters, numbers and underscores',
              },
            ]}
          >
            <Input
              placeholder="Please enter username"
              disabled={isEdit && user?.username === 'admin'}
              style={inputStyle}
            />
          </Form.Item>

          <Form.Item
            label={isEdit ? 'New Password (leave blank to keep unchanged)' : 'Password'}
            name="password"
            rules={[
              { required: !isEdit, message: 'Please enter password' },
              { min: 6, message: 'Password must be at least 6 characters' },
              { max: 100, message: 'Password cannot exceed 100 characters' },
            ]}
          >
            <Input.Password
              placeholder={isEdit ? 'Leave blank to keep current password' : 'Please enter password'}
              style={inputStyle}
            />
          </Form.Item>

          <Form.Item
            label="Admin Privileges"
            name="is_admin"
            valuePropName="checked"
            extra={
              isEdit && user?.username === 'admin'
                ? 'Admin account must maintain administrator privileges'
                : 'Admins can manage users and perform all operations'
            }
          >
            <Switch
              checkedChildren="Yes"
              unCheckedChildren="No"
              disabled={isEdit && user?.username === 'admin'}
            />
          </Form.Item>
        </Form>
      </Modal>
    </ConfigProvider>
  );
};

export default UserModal; 
import React from 'react';
import { Form, Input, Button, Card, Typography, message } from 'antd';
import { UserOutlined, LockOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { useAuthStore } from '../store/authStore';

const { Text } = Typography;

interface LoginFormData {
  username: string;
  password: string;
}

const LoginPage: React.FC = () => {
  const navigate = useNavigate();
  const { login, loading } = useAuthStore();

  const handleLogin = async (values: LoginFormData) => {
    try {
      await login(values);
      message.success('Login successful!');
      navigate('/feeds-dashboard');
    } catch (error) {
      message.error('Login failed, please check your username and password');
    }
  };

  return (
    <div className="login-container">
      <Card className="login-form-container">
        <div className="login-logo">
          <img
            src={`${(process.env.PUBLIC_URL || '').replace(/\/$/, '')}/brand/app-wordmark-guanhe-capital.svg`}
            alt="GuanHe Capital"
            style={{ display: 'block', margin: '0 auto 10px', width: 185 }}
          />
          <Text type="secondary" style={{ fontSize: 13, letterSpacing: '0.02em', display: 'block' }}>
            Do the Right things. Do things right.
          </Text>
        </div>

        <Form
          name="login"
          onFinish={handleLogin}
          autoComplete="off"
          size="large"
        >
          <Form.Item
            name="username"
            rules={[
              { required: true, message: 'Please enter your username!' },
              { min: 2, message: 'Username must be at least 2 characters!' }
            ]}
          >
            <Input
              prefix={<UserOutlined />}
              placeholder="Username"
              autoComplete="username"
            />
          </Form.Item>

          <Form.Item
            name="password"
            rules={[
              { required: true, message: 'Please enter your password!' },
              { min: 6, message: 'Password must be at least 6 characters!' }
            ]}
          >
            <Input.Password
              prefix={<LockOutlined />}
              placeholder="Password"
              autoComplete="current-password"
            />
          </Form.Item>

          <Form.Item>
            <Button
              type="primary"
              htmlType="submit"
              className="full-width"
              loading={loading}
            >
              Login
            </Button>
          </Form.Item>
        </Form>
      </Card>
    </div>
  );
};

export default LoginPage; 
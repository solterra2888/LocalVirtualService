import React from 'react';
import { Form, Input, Button, Card, Typography, message, Spin } from 'antd';
import { UserOutlined, LockOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { useAuthStore } from '../store/authStore';

const { Title, Text } = Typography;

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
      navigate('/daily');
    } catch (error) {
      message.error('Login failed, please check your username and password');
    }
  };

  return (
    <div className="login-container">
      <Card className="login-form-container">
        <div className="login-logo">
          <img src="/GuanHeCapital.ico" alt="GuanHe Capital Logo (觀合資本)" style={{ width: 72, height: 72, marginBottom: 16 }} />
          <Title
            level={2}
            className="login-title"
            style={{
              fontFamily: "'Aladin', cursive",
              letterSpacing: '0.02em',
            }}
          >
            {process.env.REACT_APP_APP_NAME || 'GuanHe Capital'}
          </Title>
          <Text
            className="login-subtitle"
            style={{
              fontFamily: "'Aladin', cursive",
              letterSpacing: '0.02em',
            }}
          >
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
              {loading ? <Spin size="small" /> : 'Login'}
            </Button>
          </Form.Item>
        </Form>

        <div style={{ textAlign: 'center', marginTop: 16 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>
            System Version: v{process.env.REACT_APP_APP_VERSION || '2.0.0'}
          </Text>
        </div>
      </Card>
    </div>
  );
};

export default LoginPage; 
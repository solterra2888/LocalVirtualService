import React from 'react';
import { Form, Input, Button, message } from 'antd';
import { useNavigate } from 'react-router-dom';
import { useAuthStore } from '../store/authStore';
import loginBgPoster from '../assets/login-bg.jpg';

interface LoginFormData {
  username: string;
  password: string;
}

/* 登录页背景：动画 WebP（与官网微信兜底完全同 URL——含 ?v=，命中同一
   immutable 缓存条目，经官网进入的用户零额外下载）。
   背景本就叠 blur(10px)+白化蒙层，细节不可辨，864px 动画与视频无差；
   <img> 不受媒体自动播放策略限制——原 <video> 实现在安卓微信（XWeb 硬拒绝）
   里同样需要点击才动，换成 <img> 后全端零点击自动循环，整段视频抢救
   机制（WeixinJSBridge/手势重试等）随之删除。
   login-bg.jpg 灰调静帧作容器底图：WebP 加载失败/慢网直入时立显，不露纯色底 */
const LoginPage: React.FC = () => {
  const navigate = useNavigate();
  const { login, loading } = useAuthStore();

  const handleLogin = async (values: LoginFormData) => {
    try {
      await login(values);
      message.success('Login successful!');
      navigate('/feeds');
    } catch (error) {
      message.error('Login failed, please check your username and password');
    }
  };

  return (
    <div
      className="login-container"
      style={{
        backgroundImage: `url(${loginBgPoster})`,
        backgroundSize: 'cover',
        backgroundPosition: 'center',
      }}
    >
      <img
        className="login-videobg"
        src={`${(process.env.PUBLIC_URL || '').replace(/\/$/, '')}/media/hero-guanhe-ambience.webp?v=20261116`}
        alt=""
        aria-hidden="true"
      />
      <div className="login-card">
        {/* 左：品牌蓝欢迎栏 */}
        <div className="login-brand-panel">
          <img
            className="login-brand-logo"
            src={`${(process.env.PUBLIC_URL || '').replace(/\/$/, '')}/brand/guanhe-logo-combo-white.svg`}
            alt="GuanHe Capital"
          />
          <h2 className="login-brand-welcome">Welcome to GuanHe Terminal.</h2>
          <span className="login-brand-bar" aria-hidden="true" />
          <p className="login-brand-tagline">
            Do the right things. Do things right.
          </p>
        </div>

        {/* 右：白色表单栏 */}
        <div className="login-form-panel">
          <div className="login-form-head">
            <h3>Login</h3>
            <span className="login-head-bar" aria-hidden="true" />
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
              <Input placeholder="Username" autoComplete="username" />
            </Form.Item>

            <Form.Item
              name="password"
              rules={[
                { required: true, message: 'Please enter your password!' },
                { min: 6, message: 'Password must be at least 6 characters!' }
              ]}
            >
              <Input.Password
                placeholder="Password"
                autoComplete="current-password"
              />
            </Form.Item>

            <Form.Item style={{ marginBottom: 0, marginTop: 34 }}>
              <Button
                type="primary"
                htmlType="submit"
                className="login-submit"
                loading={loading}
              >
                Login
              </Button>
            </Form.Item>
          </Form>
        </div>
      </div>
    </div>
  );
};

export default LoginPage;

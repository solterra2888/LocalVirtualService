import React, { useEffect, useRef } from 'react';
import { Form, Input, Button, message } from 'antd';
import { useNavigate } from 'react-router-dom';
import { useAuthStore } from '../store/authStore';
import loginBgPoster from '../assets/login-bg.jpg';

interface LoginFormData {
  username: string;
  password: string;
}

/* 登录页背景视频：复用官网 Hero 视频 1080p 版（public/media，零新增文件），
   模糊+白化渲染后细节不可辨，1080p 足够；poster 取自同款灰调静帧兜底弱网 */
const LoginPage: React.FC = () => {
  const navigate = useNavigate();
  const { login, loading } = useAuthStore();
  const videoRef = useRef<HTMLVideoElement>(null);

  // 背景视频：与官网 Hero 同款微信兜底机制（LandingPage.tsx 同源实现）
  // （1）iOS 微信 autoplay 需 WeixinJSBridge ready 后触发；（2）Android 老内核（X5）
  // 需 x5-video-player-type 同层播放；（3）返回导航/熄屏恢复时 autoplay 常被拒绝——
  // 数据就绪、页面可见、任意触摸/点击（捕获阶段）都会重试播放，self-healing。
  useEffect(() => {
    const v = videoRef.current;
    if (!v) return;

    const tryPlay = () => {
      v.play().catch(() => {});
    };
    const onWxReady = () => tryPlay();
    const wx = (window as unknown as { WeixinJSBridge?: { invoke: (a: string, b: object, cb: () => void) => void } }).WeixinJSBridge;
    if (typeof wx === 'undefined') {
      document.addEventListener('WeixinJSBridgeReady', onWxReady, false);
    } else {
      wx.invoke('getNetworkType', {}, () => tryPlay());
    }

    const onMediaReady = () => tryPlay();
    const onVisible = () => {
      if (document.visibilityState === 'visible') tryPlay();
    };
    const onGesture = () => {
      if (v.paused) tryPlay();
    };
    v.addEventListener('canplay', onMediaReady);
    v.addEventListener('loadeddata', onMediaReady);
    document.addEventListener('visibilitychange', onVisible);
    // passive：不阻塞滚动；capture：任何区域（含表单/按钮）的触摸都能救活视频
    document.addEventListener('touchend', onGesture, { capture: true, passive: true });
    document.addEventListener('click', onGesture, true);

    const onError = () => {
      const e = v.error;
      console.error('[login] video error', e && { code: e.code, message: e.message });
    };
    v.addEventListener('error', onError);

    return () => {
      document.removeEventListener('WeixinJSBridgeReady', onWxReady);
      v.removeEventListener('canplay', onMediaReady);
      v.removeEventListener('loadeddata', onMediaReady);
      document.removeEventListener('visibilitychange', onVisible);
      document.removeEventListener('touchend', onGesture, true);
      document.removeEventListener('click', onGesture, true);
      v.removeEventListener('error', onError);
    };
  }, []);

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
      <video
        className="login-videobg"
        ref={videoRef}
        src={`${(process.env.PUBLIC_URL || '').replace(/\/$/, '')}/media/hero-guanhe-mobile.mp4`}
        poster={loginBgPoster}
        autoPlay
        muted
        loop
        playsInline
        preload="auto"
        // X5（微信安卓老内核）H5 同层播放，避免被系统播放器接管/黑屏；
        // 非标准属性，绕过 TS 的已知属性检查
        {...({
          'x5-video-player-type': 'h5',
          'x5-video-player-fullscreen': 'true',
        } as React.VideoHTMLAttributes<HTMLVideoElement>)}
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

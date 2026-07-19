import React, { useEffect, useState } from 'react';

interface AvatarImageProps {
  src?: string | null;
  /** 源缺失或加载失败 (onError) 时渲染的兜底节点（通常是图标方块） */
  fallback: React.ReactNode;
  size?: number;
  radius?: number;
  isDark?: boolean;
  style?: React.CSSProperties;
}

/**
 * 头像 <img> + 优雅兜底。
 *
 * 外部头像 URL（Substack / Weibo / ZSXQ CDN 等）随时可能失效（旧桶下线、签名过期、
 * 热链封禁返回 4xx），裸 <img> 失败时会显示裂图。此组件在 src 缺失或 onError 时
 * 退回到传入的 fallback 节点，保证 UI 始终有占位。
 *
 * src 变化（切换到不同头像）时自动重置错误态，避免上一张失败把新 src 也判死。
 */
const AvatarImage: React.FC<AvatarImageProps> = ({
  src,
  fallback,
  size = 32,
  radius = 6,
  isDark = false,
  style,
}) => {
  const [errored, setErrored] = useState(false);
  useEffect(() => {
    setErrored(false);
  }, [src]);

  if (!src || errored) {
    return <>{fallback}</>;
  }

  return (
    <img
      src={src}
      alt=""
      decoding="async"
      onError={() => setErrored(true)}
      style={{
        width: size,
        height: size,
        borderRadius: radius,
        objectFit: 'cover',
        flexShrink: 0,
        background: isDark ? 'rgba(255,255,255,0.08)' : '#F3F4F6',
        ...style,
      }}
    />
  );
};

export default AvatarImage;

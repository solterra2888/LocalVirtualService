import React, { lazy } from 'react';

/**
 * 带自愈的 React.lazy 包装。
 *
 * 背景：路由级代码分割后，长驻标签页在发版（rsync --delete 换掉旧 chunk）后
 * 点导航会 ChunkLoadError（2026-11-14 Daily 下线时就出现过旧 bundle 404 的同类问题，
 * 当时靠 index.html no-cache 修了一半，异步 chunk 这层由这里兜底）。
 * 策略：chunk 加载失败时整页刷新一次（sessionStorage 打标防循环），
 * 刷新后 index.html no-cache 必然拿到新哈希清单，自愈完成。
 */
/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
export function lazyWithRetry<T extends React.ComponentType<any>>(
  factory: () => Promise<{ default: T }>
): React.LazyExoticComponent<T> {
  const FLAG = 'guanhe_chunk_reload';
  return lazy(() =>
    factory().then(
      (module) => {
        // 任一 chunk 成功加载即清除标记，恢复"允许一次自愈刷新"的能力
        sessionStorage.removeItem(FLAG);
        return module;
      },
      (error: unknown) => {
        if (sessionStorage.getItem(FLAG) !== '1') {
          sessionStorage.setItem(FLAG, '1');
          window.location.reload();
        }
        throw error;
      }
    )
  );
}

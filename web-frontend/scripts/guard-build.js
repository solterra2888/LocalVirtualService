#!/usr/bin/env node
/**
 * 裸 `npm run build` 防护（VS Code Ctrl+Shift+B 默认构建任务执行的正是它）。
 *
 * 服务器仅 7.4GB 内存，裸构建曾两次触发 OOM 整机死机（2026-10-15 / 2026-10-17），
 * 且裸构建会原地重建 build/（Nginx 线上挂载目录），构建期间站点缺文件 404。
 *
 * - 默认：拒绝执行，提示使用 npm run deploy
 * - GUANHE_ALLOW_BUILD=1 npm run build：走与 deploy 相同的安全构建
 *   （锁 + 内存预检 + 限内存，产物仅到 build-staging/，不碰线上目录）
 */
const { spawnSync } = require('child_process');
const path = require('path');

if (process.env.GUANHE_ALLOW_BUILD !== '1') {
  console.error('');
  console.error('❌ 已禁止直接运行 npm run build。');
  console.error('   裸构建在低内存服务器上会触发 OOM 崩机，且会原地清空线上 build/ 目录。');
  console.error('');
  console.error('   发布线上：npm run deploy');
  console.error('   仅验证编译（不碰线上目录）：GUANHE_ALLOW_BUILD=1 npm run build');
  console.error('');
  process.exit(1);
}

const result = spawnSync(
  'bash',
  [path.join(__dirname, 'build-safe.sh')],
  { stdio: 'inherit' }
);
process.exit(result.status === null ? 1 : result.status);

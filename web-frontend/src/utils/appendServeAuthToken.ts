/**
 * 仅对后端 `/api/files/serve/{id}` 追加 token；OSS 等签名 URL 上追加 query 会破坏签名。
 * 供 `<audio>` / `<img>` / iframe 等无法带 Authorization 头的请求使用。
 */
export function appendServeAuthToken(
  fileUrl: string,
  token: string | null | undefined
): string {
  if (!token || !fileUrl) return fileUrl;
  const pathOnly = fileUrl.split('?')[0];
  if (!pathOnly.includes('/api/files/serve/')) return fileUrl;
  const separator = fileUrl.includes('?') ? '&' : '?';
  return `${fileUrl}${separator}token=${encodeURIComponent(token)}`;
}

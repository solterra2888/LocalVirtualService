/**
 * 无头像图片时展示的首字母，与 Files 分享区 Avatar 一致（username 首字符大写）。
 */
export function userAvatarLetter(username: string | undefined | null): string {
  const t = (username ?? '').trim();
  if (!t) return '?';
  return t.charAt(0).toUpperCase();
}

/** 与 upload / share 列表中「被分享用户」字母头像相同的背景色 */
export const USER_AVATAR_LETTER_BG = '#1890ff';

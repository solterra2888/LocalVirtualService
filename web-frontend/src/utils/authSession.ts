import { Modal } from 'antd';

/** Must match backend `SESSION_REPLACED_CODE` in `backend/auth/jwt_utils.py` */
export const SESSION_REPLACED_CODE = 'SESSION_REPLACED';

export function isSessionReplaced401Detail(detail: unknown): boolean {
  return (
    typeof detail === 'object' &&
    detail !== null &&
    (detail as { code?: string }).code === SESSION_REPLACED_CODE
  );
}

export function get401UserMessage(detail: unknown): string {
  if (
    typeof detail === 'object' &&
    detail !== null &&
    typeof (detail as { message?: unknown }).message === 'string'
  ) {
    return (detail as { message: string }).message;
  }
  if (typeof detail === 'string') {
    return detail;
  }
  return 'Your session has expired. Please sign in again.';
}

/**
 * 账号在其它设备登录导致当前 JWT 失效时的醒目提示（英文）
 */
export function showSessionReplacedModal(message: string, onOk: () => void): void {
  Modal.warning({
    title: 'Signed in on another device',
    content: message,
    okText: 'OK',
    centered: true,
    onOk,
  });
}

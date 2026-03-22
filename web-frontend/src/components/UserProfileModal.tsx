import React, { useEffect, useRef, useState } from 'react';
import { ConfigProvider, Modal, Input, Avatar, message, theme as antdTheme } from 'antd';
import { CameraOutlined, SyncOutlined } from '@ant-design/icons';
import { getModalStyles, MODAL_DARK_BG } from '../utils/modalStyles';
import { useAuthStore } from '../store/authStore';
import { usersApi } from '../services/users';
import { userAvatarLetter } from '../utils/userAvatar';

export interface UserProfileModalProps {
  open: boolean;
  onClose: () => void;
  themeMode: 'light' | 'dark';
}

const DARK_INPUT_TOKEN = {
  colorBgContainer: '#2a3444',
  colorText: 'rgba(255, 255, 255, 0.88)',
  colorTextPlaceholder: 'rgba(255, 255, 255, 0.45)',
  colorBorder: 'rgba(255, 255, 255, 0.12)',
  colorBorderSecondary: 'rgba(255, 255, 255, 0.12)',
};

/**
 * 编辑头像、用户名、密码（与 UploadFilesModal 等一致：dark 下用 ConfigProvider 包一层）
 */
const UserProfileModal: React.FC<UserProfileModalProps> = ({ open, onClose, themeMode }) => {
  const { user, updateUser } = useAuthStore();
  const [profileUsername, setProfileUsername] = useState('');
  const [profileAvatarUrl, setProfileAvatarUrl] = useState<string | undefined>();
  const [profileSaving, setProfileSaving] = useState(false);
  const [avatarUploading, setAvatarUploading] = useState(false);
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const avatarInputRef = useRef<HTMLInputElement>(null);
  const isDark = themeMode === 'dark';

  useEffect(() => {
    if (open) {
      setProfileUsername(user?.username || '');
      setProfileAvatarUrl(user?.avatar_url);
      setNewPassword('');
      setConfirmPassword('');
    }
  }, [open, user?.username, user?.avatar_url]);

  const handleAvatarUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const allowed = ['image/jpeg', 'image/png', 'image/gif', 'image/webp'];
    if (!allowed.includes(file.type)) {
      message.error('Only JPG, PNG, GIF, WEBP allowed');
      return;
    }
    if (file.size > 5 * 1024 * 1024) {
      message.error('Avatar must be under 5MB');
      return;
    }
    setAvatarUploading(true);
    try {
      const result = await usersApi.uploadAvatar(file);
      setProfileAvatarUrl(result.avatar_url);
      updateUser({ avatar_url: result.avatar_url });
      message.success('Avatar uploaded');
    } catch {
      message.error('Avatar upload failed');
    } finally {
      setAvatarUploading(false);
      if (avatarInputRef.current) avatarInputRef.current.value = '';
    }
  };

  const handleProfileSave = async () => {
    if (!profileUsername.trim()) {
      message.error('Username cannot be empty');
      return;
    }
    if (newPassword || confirmPassword) {
      if (newPassword.length < 6) {
        message.error('Password must be at least 6 characters');
        return;
      }
      if (newPassword !== confirmPassword) {
        message.error('Passwords do not match');
        return;
      }
    }
    setProfileSaving(true);
    try {
      const updated = await usersApi.updateMyProfile({
        username: profileUsername.trim(),
        avatar_url: profileAvatarUrl,
      });
      updateUser({ username: updated.username, avatar_url: updated.avatar_url });

      if (newPassword && user?.id) {
        await usersApi.changePassword(user.id, newPassword);
      }

      message.success(newPassword ? 'Profile and password updated' : 'Profile updated');
      onClose();
    } catch {
      message.error('Failed to update profile');
    } finally {
      setProfileSaving(false);
    }
  };

  const modalStyles = getModalStyles(themeMode);

  const content = (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 20, padding: '16px 0' }}>
      <div
        style={{ position: 'relative', cursor: 'pointer' }}
        onClick={() => avatarInputRef.current?.click()}
      >
        {profileAvatarUrl ? (
          <Avatar size={80} src={profileAvatarUrl} />
        ) : (
          <Avatar size={80} style={{ backgroundColor: '#1890ff', fontSize: 36 }}>
            {userAvatarLetter(profileUsername || user?.username)}
          </Avatar>
        )}
        <div
          style={{
            position: 'absolute',
            bottom: 0,
            right: 0,
            background: isDark ? '#364152' : '#fff',
            borderRadius: '50%',
            width: 28,
            height: 28,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            boxShadow: '0 2px 6px rgba(0,0,0,0.2)',
            border: `2px solid ${isDark ? '#1a2234' : '#fff'}`,
          }}
        >
          {avatarUploading ? (
            <SyncOutlined spin style={{ fontSize: 14, color: '#1890ff' }} />
          ) : (
            <CameraOutlined style={{ fontSize: 14, color: isDark ? '#e0e6f1' : '#666' }} />
          )}
        </div>
        <input
          ref={avatarInputRef}
          type="file"
          accept="image/jpeg,image/png,image/gif,image/webp"
          style={{ display: 'none' }}
          onChange={handleAvatarUpload}
        />
      </div>
      <div style={{ width: '100%' }}>
        <div style={{ marginBottom: 8, fontWeight: 500, color: isDark ? '#e0e6f1' : undefined }}>Username</div>
        <Input
          value={profileUsername}
          onChange={e => setProfileUsername(e.target.value)}
          maxLength={50}
          autoComplete="username"
          name="profile_username"
        />
      </div>
      <div style={{ width: '100%', borderTop: `1px solid ${isDark ? 'rgba(255,255,255,0.08)' : '#f0f0f0'}`, paddingTop: 16 }}>
        <div style={{ marginBottom: 12, fontWeight: 500, fontSize: 13, color: isDark ? '#94a3b8' : '#999' }}>
          Change Password (optional)
        </div>
        <div style={{ marginBottom: 8, fontWeight: 500, color: isDark ? '#e0e6f1' : undefined }}>New Password</div>
        <Input.Password
          value={newPassword}
          onChange={e => setNewPassword(e.target.value)}
          placeholder="Leave empty to keep current"
          autoComplete="new-password"
          name="profile_new_password"
          id="profile_new_password"
        />
        {newPassword && (
          <div style={{ marginTop: 12 }}>
            <div style={{ marginBottom: 8, fontWeight: 500, color: isDark ? '#e0e6f1' : undefined }}>Confirm Password</div>
            <Input.Password
              value={confirmPassword}
              onChange={e => setConfirmPassword(e.target.value)}
              placeholder="Re-enter new password"
              status={confirmPassword && confirmPassword !== newPassword ? 'error' : undefined}
              autoComplete="new-password"
              name="profile_confirm_password"
              id="profile_confirm_password"
            />
          </div>
        )}
      </div>
    </div>
  );

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgElevated: MODAL_DARK_BG,
            colorBgContainer: DARK_INPUT_TOKEN.colorBgContainer,
            colorText: DARK_INPUT_TOKEN.colorText,
            colorTextHeading: 'rgba(255, 255, 255, 0.88)',
            colorTextPlaceholder: DARK_INPUT_TOKEN.colorTextPlaceholder,
            colorBorder: DARK_INPUT_TOKEN.colorBorder,
            colorBorderSecondary: DARK_INPUT_TOKEN.colorBorderSecondary,
            borderRadius: 8,
          },
        }),
      }}
    >
      <Modal
        title={
          <span style={{ color: isDark ? 'rgba(255, 255, 255, 0.88)' : undefined }}>
            Edit Profile
          </span>
        }
        open={open}
        onCancel={onClose}
        onOk={handleProfileSave}
        okText="Save"
        cancelText="Cancel"
        confirmLoading={profileSaving}
        width={400}
        centered
        styles={modalStyles}
        destroyOnClose
      >
        {content}
      </Modal>
    </ConfigProvider>
  );
};

export default UserProfileModal;

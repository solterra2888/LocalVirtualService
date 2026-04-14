import React, { useState } from 'react';
import { Modal, Input, Button, message, Space, ConfigProvider, theme as antdTheme } from 'antd';
import { EditOutlined, SaveOutlined } from '@ant-design/icons';
import { useLocation, useNavigate } from 'react-router-dom';
import { filesAPI } from '../services/files';
import { getErrorMessage } from '../utils/errorHandler';
import { getModalStyles } from '../utils/modalStyles';

/** FilesPage 订阅此事件以刷新左侧 Sources 列表 */
export const TASK_EXECUTIONS_REFRESH_EVENT = 'guanhe:task-executions-refresh';

const { TextArea } = Input;

interface CreateTextNoteModalProps {
  visible: boolean;
  onCancel: () => void;
  themeMode: 'light' | 'dark';
}

const CreateTextNoteModal: React.FC<CreateTextNoteModalProps> = ({ visible, onCancel, themeMode }) => {
  const isDark = themeMode === 'dark';
  const navigate = useNavigate();
  const location = useLocation();
  const [title, setTitle] = useState('');
  const [content, setContent] = useState('');
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    const trimmed = content.trim();
    if (!trimmed) {
      message.warning('Please enter some content');
      return;
    }

    setSaving(true);
    try {
      await filesAPI.createTextNote(title.trim(), trimmed);
      message.success('Text note created! Auto-analysis will begin shortly.');
      handleClose();
      const onFilesPage = /\/files\/?$/.test(location.pathname) || location.pathname.endsWith('/files');
      if (onFilesPage) {
        window.dispatchEvent(
          new CustomEvent(TASK_EXECUTIONS_REFRESH_EVENT, { detail: { resetTypeFilter: true } })
        );
      } else {
        navigate('/files');
      }
    } catch (error: any) {
      message.error(getErrorMessage(error, 'Failed to create text note'));
    } finally {
      setSaving(false);
    }
  };

  const handleClose = () => {
    setTitle('');
    setContent('');
    setSaving(false);
    onCancel();
  };

  const modalStyles = getModalStyles(themeMode);

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: '#212936',
            colorBorder: '#364152',
            colorBorderSecondary: '#364152',
            colorText: '#e0e6f1',
            colorTextSecondary: '#94a3b8',
            colorPrimary: '#1890ff',
          },
        }),
      }}
    >
      <Modal
        title={
          <Space>
            <EditOutlined style={{ fontSize: '18px' }} />
            <span>My Notes / Thoughts</span>
          </Space>
        }
        open={visible}
        onCancel={handleClose}
        width="min(720px, calc(100vw - 24px))"
        centered
        footer={null}
        maskClosable={!saving}
        closable={!saving}
        destroyOnClose
        maskStyle={{
          background: isDark ? 'rgba(0, 0, 0, 0.6)' : undefined,
          backdropFilter: 'blur(6px)',
          WebkitBackdropFilter: 'blur(6px)',
        }}
        styles={modalStyles}
      >
        <div style={{ padding: '8px 0' }}>
          <div style={{ marginBottom: '16px' }}>
            <label
              style={{
                display: 'block',
                marginBottom: '6px',
                fontSize: '14px',
                fontWeight: 500,
                color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
              }}
            >
              Title <span style={{ color: isDark ? '#64748b' : 'rgba(0,0,0,0.45)', fontWeight: 400 }}>(optional)</span>
            </label>
            <Input
              placeholder="Untitled Note"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              size="large"
              disabled={saving}
              maxLength={200}
              style={{
                background: isDark ? '#212936' : '#fff',
                borderColor: isDark ? '#364152' : '#d9d9d9',
                color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
              }}
            />
          </div>

          <div style={{ marginBottom: '16px' }}>
            <label
              style={{
                display: 'block',
                marginBottom: '6px',
                fontSize: '14px',
                fontWeight: 500,
                color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
              }}
            >
              Content <span style={{ color: isDark ? '#64748b' : 'rgba(0,0,0,0.45)', fontWeight: 400 }}>(Markdown supported)</span>
            </label>
            <TextArea
              placeholder="Write your text here... Markdown formatting is supported."
              value={content}
              onChange={(e) => setContent(e.target.value)}
              disabled={saving}
              autoSize={{ minRows: 12, maxRows: 24 }}
              style={{
                background: isDark ? '#212936' : '#fff',
                borderColor: isDark ? '#364152' : '#d9d9d9',
                color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
                fontFamily: 'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace',
                fontSize: '13px',
                lineHeight: '1.6',
              }}
            />
          </div>

          <div
            style={{
              fontSize: '12px',
              color: isDark ? '#64748b' : 'rgba(0, 0, 0, 0.45)',
              marginBottom: '20px',
            }}
          >
            After saving, the system will automatically run content analysis (tagging, naming, classification).
          </div>

          <div
            style={{
              display: 'flex',
              justifyContent: 'flex-end',
              alignItems: 'center',
              gap: '8px',
            }}
          >
            <Button onClick={handleClose} disabled={saving}>
              Cancel
            </Button>
            <Button
              type="primary"
              onClick={handleSave}
              icon={<SaveOutlined />}
              disabled={!content.trim() || saving}
              loading={saving}
            >
              Save
            </Button>
          </div>
        </div>
      </Modal>
    </ConfigProvider>
  );
};

export default CreateTextNoteModal;

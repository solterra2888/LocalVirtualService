/**
 * 知识星球附件文件列表（统一只做下载链接）。
 *
 * 由 API 读取层合并后的 metadata.files 驱动：每个文件 {file_id, name, size, ext, ready}。
 *  - ready=true：点击调用 getZsxqFileDownloadUrl 即时签名 → 新标签预览(pdf/image) 或下载(其余)
 *  - ready=false：显示文件名 + "处理中"灰字
 *
 * 复用于 ZsxqOriginalView（Daily 抽屉）与 FeedArticleBody（Feed 详情，含移动端）。
 */
import React, { useState } from 'react';
import { Tooltip, message } from 'antd';
import { DownloadOutlined, LoadingOutlined } from '@ant-design/icons';
import { ZsxqFile, getZsxqFileDownloadUrl } from '../../services/subscriptions';
import { getFileIconByExt, formatFileSize } from './fileTypeIcon';

export interface ZsxqFileListProps {
  files: ZsxqFile[];
  /** 标签颜色（可选，默认 #999） */
  mutedColor?: string;
  /** 主色（下载链接，可选，默认 #1677ff） */
  accentColor?: string;
}

const ZsxqFileList: React.FC<ZsxqFileListProps> = ({
  files,
  mutedColor = '#999',
  accentColor = '#1677ff',
}) => {
  const [loadingId, setLoadingId] = useState<string | null>(null);

  if (!files || files.length === 0) return null;

  const handleDownload = async (file: ZsxqFile) => {
    if (!file.ready || !file.file_id) return;
    setLoadingId(file.file_id);
    try {
      const { url, inline } = await getZsxqFileDownloadUrl(file.file_id);
      if (inline) {
        window.open(url, '_blank', 'noopener,noreferrer');
      } else {
        // 触发浏览器下载（不跳转）
        const a = document.createElement('a');
        a.href = url;
        a.download = file.name || '';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
      }
    } catch (e: any) {
      message.error(e?.response?.data?.detail || '获取下载链接失败');
    } finally {
      setLoadingId(null);
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      {files.map((file, idx) => {
        const iconInfo = getFileIconByExt(file.ext);
        const sizeText = formatFileSize(file.size);
        const isLoading = loadingId === file.file_id;
        return (
          <div
            key={file.file_id || `${file.name}-${idx}`}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 8,
              padding: '6px 8px',
              borderRadius: 6,
              background: 'rgba(0,0,0,0.03)',
              fontSize: 13,
            }}
          >
            <Tooltip title={iconInfo.tooltip}>
              <span style={{ color: iconInfo.color, fontSize: 16, display: 'inline-flex' }}>
                {iconInfo.icon}
              </span>
            </Tooltip>
            <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {file.name || '未命名文件'}
              {sizeText ? <span style={{ color: mutedColor, marginLeft: 6, fontSize: 11 }}>{sizeText}</span> : null}
            </span>
            {file.ready ? (
              <a
                onClick={(e) => {
                  e.preventDefault();
                  handleDownload(file);
                }}
                style={{ color: accentColor, cursor: 'pointer', whiteSpace: 'nowrap', fontSize: 12 }}
              >
                {isLoading ? <LoadingOutlined /> : <DownloadOutlined />} 下载
              </a>
            ) : (
              <span style={{ color: mutedColor, fontSize: 11, whiteSpace: 'nowrap' }}>处理中</span>
            )}
          </div>
        );
      })}
    </div>
  );
};

export default ZsxqFileList;

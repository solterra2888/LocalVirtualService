import React from 'react';
import { Card, Space, Tag, Button, Dropdown, Tooltip } from 'antd';
import {
  CheckOutlined,
  StarOutlined,
  StarFilled,
  MoreOutlined,
  EditOutlined,
  CopyOutlined,
  DeleteOutlined
} from '@ant-design/icons';
import type { MenuProps } from 'antd';
import { NewsFilterPreset } from '../types/newsPresets';

interface PresetCardProps {
  preset: NewsFilterPreset;
  onApply: (preset: NewsFilterPreset) => void;
  onEdit: (preset: NewsFilterPreset) => void;
  onDuplicate: (preset: NewsFilterPreset) => void;
  onToggleFavorite: (preset: NewsFilterPreset) => void;
  onDelete: (preset: NewsFilterPreset) => void;
  isActive?: boolean;
  compact?: boolean;
}

const PresetCard: React.FC<PresetCardProps> = ({
  preset,
  onApply,
  onEdit,
  onDuplicate,
  onToggleFavorite,
  onDelete,
  isActive = false,
  compact = false
}) => {
  const isSystemPreset = preset.preset_type === 'system';

  // 生成筛选条件预览标签
  const getFilterTags = () => {
    const tags: string[] = [];
    const config = preset.filter_config;

    if (config.categories && config.categories.length > 0) {
      tags.push(...config.categories.slice(0, 2).map(c => c));
      if (config.categories.length > 2) tags.push(`+${config.categories.length - 2}`);
    }
    if (config.event_types && config.event_types.length > 0) {
      tags.push(...config.event_types.slice(0, 2));
      if (config.event_types.length > 2) tags.push(`+${config.event_types.length - 2}`);
    }
    if (config.related_sectors && config.related_sectors.length > 0) {
      tags.push(...config.related_sectors.slice(0, 1));
      if (config.related_sectors.length > 1) tags.push(`+${config.related_sectors.length - 1}`);
    }
    if (config.related_countries && config.related_countries.length > 0) {
      tags.push(...config.related_countries.slice(0, 2));
      if (config.related_countries.length > 2) tags.push(`+${config.related_countries.length - 2}`);
    }
    if (config.urgency && config.urgency.length > 0) {
      tags.push(...config.urgency.slice(0, 1));
    }
    if (config.only_primary) {
      tags.push('Primary Only');
    }
    if (config.min_confidence && config.min_confidence > 0.5) {
      tags.push(`Conf ≥${(config.min_confidence * 100).toFixed(0)}%`);
    }

    return tags.slice(0, 5); // 最多显示5个标签
  };

  const filterTags = getFilterTags();

  // 下拉菜单项
  const menuItems: MenuProps['items'] = [
    !isSystemPreset && {
      key: 'edit',
      label: 'Edit Preset',
      icon: <EditOutlined />,
      onClick: () => onEdit(preset)
    },
    {
      key: 'duplicate',
      label: 'Duplicate',
      icon: <CopyOutlined />,
      onClick: () => onDuplicate(preset)
    },
    {
      key: 'favorite',
      label: preset.is_favorite ? 'Remove from Favorites' : 'Add to Favorites',
      icon: preset.is_favorite ? <StarFilled /> : <StarOutlined />,
      onClick: () => onToggleFavorite(preset)
    },
    !isSystemPreset && {
      type: 'divider'
    },
    !isSystemPreset && {
      key: 'delete',
      label: 'Delete',
      icon: <DeleteOutlined />,
      danger: true,
      onClick: () => onDelete(preset)
    }
  ].filter(Boolean) as MenuProps['items'];

  if (compact) {
    // 紧凑模式：单行显示
    return (
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          padding: '8px 12px',
          borderRadius: '6px',
          backgroundColor: isActive ? '#e6f7ff' : '#fafafa',
          border: isActive ? '1px solid #1890ff' : '1px solid #d9d9d9',
          marginBottom: '8px',
          cursor: 'pointer',
          transition: 'all 0.2s'
        }}
        onClick={() => onApply(preset)}
      >
        <Space style={{ flex: 1, fontSize: '12px' }}>
          <span style={{ fontSize: '14px' }}>{preset.icon || '📋'}</span>
          <span style={{ fontWeight: isActive ? 600 : 400 }}>{preset.preset_name}</span>
          {preset.is_favorite && <StarFilled style={{ color: '#faad14', fontSize: '12px' }} />}
          {isActive && <CheckOutlined style={{ color: '#52c41a' }} />}
        </Space>
        <Dropdown menu={{ items: menuItems }} trigger={['click']} placement="bottomRight">
          <Button
            type="text"
            size="small"
            icon={<MoreOutlined />}
            onClick={(e) => e.stopPropagation()}
            style={{ padding: '0 4px' }}
          />
        </Dropdown>
      </div>
    );
  }

  // 标准模式：卡片显示
  return (
    <Card
      size="small"
      style={{
        marginBottom: '12px',
        borderRadius: '8px',
        border: isActive ? '2px solid #1890ff' : '1px solid #d9d9d9',
        boxShadow: isActive ? '0 2px 8px rgba(24, 144, 255, 0.2)' : 'none',
        transition: 'all 0.3s'
      }}
      bodyStyle={{ padding: '12px' }}
      hoverable
    >
      {/* 标题行 */}
      <div style={{ display: 'flex', alignItems: 'center', marginBottom: '8px' }}>
        <Space style={{ flex: 1 }}>
          <span style={{ fontSize: '16px' }}>{preset.icon || '📋'}</span>
          <span style={{ fontSize: '13px', fontWeight: 600 }}>{preset.preset_name}</span>
          {preset.is_favorite && (
            <StarFilled style={{ color: '#faad14', fontSize: '12px' }} />
          )}
          {preset.is_default && (
            <Tag color="blue" style={{ fontSize: '9px', padding: '0 4px', lineHeight: '16px' }}>
              Default
            </Tag>
          )}
          {isActive && (
            <Tag color="green" icon={<CheckOutlined />} style={{ fontSize: '9px', padding: '0 4px', lineHeight: '16px' }}>
              Active
            </Tag>
          )}
        </Space>
        <Dropdown menu={{ items: menuItems }} trigger={['click']} placement="bottomRight">
          <Button
            type="text"
            size="small"
            icon={<MoreOutlined />}
            style={{ padding: '0 4px' }}
          />
        </Dropdown>
      </div>

      {/* 描述 */}
      {preset.description && (
        <div style={{ fontSize: '11px', color: '#666', marginBottom: '8px', lineHeight: '1.4' }}>
          {preset.description}
        </div>
      )}

      {/* 筛选条件预览 */}
      {filterTags.length > 0 && (
        <div style={{ marginBottom: '8px' }}>
          <Space size={[4, 4]} wrap>
            {filterTags.map((tag, index) => (
              <Tag
                key={index}
                color="default"
                style={{
                  fontSize: '9px',
                  padding: '0 6px',
                  lineHeight: '18px',
                  margin: 0,
                  border: '1px solid #e0e0e0'
                }}
              >
                {tag}
              </Tag>
            ))}
          </Space>
        </div>
      )}

      {/* 统计信息和操作按钮 */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div style={{ fontSize: '9px', color: '#999' }}>
          {preset.usage_count > 0 && (
            <Tooltip title="使用次数">
              <span>Used {preset.usage_count} times</span>
            </Tooltip>
          )}
        </div>
        <Button
          type={isActive ? 'default' : 'primary'}
          size="small"
          onClick={() => onApply(preset)}
          style={{ fontSize: '11px', height: '24px' }}
        >
          {isActive ? 'Applied' : 'Apply'}
        </Button>
      </div>
    </Card>
  );
};

export default PresetCard;


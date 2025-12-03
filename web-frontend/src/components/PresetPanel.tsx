import React, { useState, useEffect } from 'react';
import {
  Card,
  Space,
  Button,
  Modal,
  message,
  Empty,
  Spin,
  Divider,
  Tooltip
} from 'antd';
import {
  PlusOutlined,
  BookOutlined,
  ExclamationCircleOutlined,
  ReloadOutlined,
  SettingOutlined
} from '@ant-design/icons';
import PresetCard from './PresetCard';
import CreatePresetModal from './CreatePresetModal';
import PresetsManager from './PresetsManager';
import { newsPresetsAPI } from '../services/newsPresets';
import { NewsFilterPreset, CreatePresetRequest, UpdatePresetRequest, FilterConfig } from '../types/newsPresets';
import { ThemeFilterState } from '../types/themes';
import { useAuthStore } from '../store/authStore';

const { confirm } = Modal;

interface PresetPanelProps {
  onApplyPreset: (preset: NewsFilterPreset) => void;
  currentFilters?: ThemeFilterState;
  activePresetId?: number | null;
  compact?: boolean;
  onOpenAdvancedFilter?: () => void;  // 🔥 新增：打开高级筛选
}

const PresetPanel: React.FC<PresetPanelProps> = ({
  onApplyPreset,
  currentFilters,
  activePresetId,
  compact = false,
  onOpenAdvancedFilter
}) => {
  const { user } = useAuthStore();
  const [presets, setPresets] = useState<NewsFilterPreset[]>([]);
  const [loading, setLoading] = useState(false);
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [editingPreset, setEditingPreset] = useState<NewsFilterPreset | null>(null);
  const [adminModalVisible, setAdminModalVisible] = useState(false);

  // 检查是否为 admin
  const isAdmin = user?.username === 'admin' || user?.username === 'administrator';

  // 加载预设列表
  const loadPresets = async () => {
    setLoading(true);
    try {
      const response = await newsPresetsAPI.getPresets({ include_system: true });
      setPresets(response.presets);
    } catch (error: any) {
      console.error('Failed to load presets:', error);
      message.error('Failed to load presets');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadPresets();
  }, []);

  // 应用预设
  const handleApplyPreset = async (preset: NewsFilterPreset) => {
    try {
      // 先应用筛选
      onApplyPreset(preset);
      
      // 如果不是系统预设，异步更新使用统计（不阻塞用户体验）
      if (preset.id > 0 && preset.preset_type !== 'system') {
        newsPresetsAPI.usePreset(preset.id).catch(err => {
          console.warn('Failed to update preset usage statistics:', err);
        });
        
        // 延迟刷新预设列表以显示更新的统计
        setTimeout(() => {
          loadPresets();
        }, 500);
      }
      
      message.success(`Applied preset: ${preset.preset_name}`);
    } catch (error: any) {
      console.error('Failed to apply preset:', error);
      message.error('Failed to apply preset');
    }
  };

  // 创建预设
  const handleCreatePreset = async (data: {
    preset_name: string;
    description?: string;
    icon?: string;
    filter_config: FilterConfig;
    scope_type?: 'global' | 'symbol_specific';
    symbol_code?: string;
    is_default?: boolean;
    is_favorite?: boolean;
  }) => {
    try {
      // 转换为 CreatePresetRequest 类型
      const request: CreatePresetRequest = {
        ...data,
        scope_type: data.scope_type || 'global', // 确保 scope_type 必填
      };
      
      await newsPresetsAPI.createPreset(request);
      await loadPresets();
      setCreateModalVisible(false);
    } catch (error: any) {
      console.error('Failed to create preset:', error);
      throw error; // 让 Modal 处理错误
    }
  };

  // 编辑预设
  const handleEditPreset = (preset: NewsFilterPreset) => {
    setEditingPreset(preset);
    setCreateModalVisible(true);
  };

  // 更新预设
  const handleUpdatePreset = async (data: {
    preset_name?: string;
    description?: string;
    icon?: string;
    filter_config?: FilterConfig;
    is_default?: boolean;
    is_favorite?: boolean;
  }) => {
    if (!editingPreset) return;
    
    try {
      const request: UpdatePresetRequest = {
        preset_name: data.preset_name,
        description: data.description,
        icon: data.icon,
        filter_config: data.filter_config,
        is_default: data.is_default,
        is_favorite: data.is_favorite
      };
      
      await newsPresetsAPI.updatePreset(editingPreset.id, request);
      await loadPresets();
      setCreateModalVisible(false);
      setEditingPreset(null);
    } catch (error: any) {
      console.error('Failed to update preset:', error);
      throw error;
    }
  };

  // 复制预设
  const handleDuplicatePreset = async (preset: NewsFilterPreset) => {
    const newName = `${preset.preset_name} (Copy)`;
    try {
      await newsPresetsAPI.duplicatePreset(preset.id, newName);
      await loadPresets();
      message.success(`Duplicated preset: ${newName}`);
    } catch (error: any) {
      console.error('Failed to duplicate preset:', error);
      message.error('Failed to duplicate preset');
    }
  };

  // 切换收藏状态
  const handleToggleFavorite = async (preset: NewsFilterPreset) => {
    try {
      await newsPresetsAPI.toggleFavorite(preset);
      await loadPresets();
      message.success(preset.is_favorite ? 'Removed from favorites' : 'Added to favorites');
    } catch (error: any) {
      console.error('Failed to toggle favorite:', error);
      message.error('Failed to update favorite status');
    }
  };

  // 删除预设
  const handleDeletePreset = (preset: NewsFilterPreset) => {
    confirm({
      title: 'Delete Preset',
      icon: <ExclamationCircleOutlined />,
      content: `Are you sure you want to delete "${preset.preset_name}"?`,
      okText: 'Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk: async () => {
        try {
          await newsPresetsAPI.deletePreset(preset.id);
          await loadPresets();
          message.success('Preset deleted successfully');
        } catch (error: any) {
          console.error('Failed to delete preset:', error);
          message.error('Failed to delete preset');
        }
      }
    });
  };

  // 分组预设：系统预设和用户预设
  const systemPresets = presets.filter(p => p.preset_type === 'system');
  const userPresets = presets.filter(p => p.preset_type === 'custom');
  const favoritePresets = userPresets.filter(p => p.is_favorite);
  const regularPresets = userPresets.filter(p => !p.is_favorite);

  return (
    <>
      <Card
        title={
          <Space>
            <BookOutlined />
            <span>My Presets</span>
          </Space>
        }
        extra={
          <Space>
            <Tooltip title="Manage presets">
              <Button
                type="text"
                size="small"
                icon={<SettingOutlined />}
                onClick={() => setAdminModalVisible(true)}
              />
            </Tooltip>
            <Tooltip title="Refresh presets">
              <Button
                type="text"
                size="small"
                icon={<ReloadOutlined />}
                onClick={loadPresets}
                loading={loading}
              />
            </Tooltip>
            <Button
              type="primary"
              size="small"
              icon={<PlusOutlined />}
              onClick={() => {
                setEditingPreset(null);
                setCreateModalVisible(true);
              }}
              disabled={!currentFilters}
            >
              New
            </Button>
          </Space>
        }
        style={{ marginBottom: '16px' }}
        bodyStyle={{ padding: '12px', maxHeight: '600px', overflowY: 'auto' }}
      >
        {/* 🔥 新增：高级筛选按钮 */}
        {onOpenAdvancedFilter && (
          <Button
            icon={<SettingOutlined />}
            onClick={onOpenAdvancedFilter}
            style={{ width: '100%', marginBottom: '12px' }}
            size="small"
            type="dashed"
          >
            🎛️ Advanced Filter
          </Button>
        )}
        
        {loading ? (
          <div style={{ textAlign: 'center', padding: '20px' }}>
            <Spin tip="Loading presets..." />
          </div>
        ) : presets.length === 0 ? (
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description="No presets yet"
            style={{ padding: '20px 0' }}
          >
            <Button
              type="primary"
              size="small"
              icon={<PlusOutlined />}
              onClick={() => setCreateModalVisible(true)}
              disabled={!currentFilters}
            >
              Create Your First Preset
            </Button>
          </Empty>
        ) : (
          <>
            {/* 收藏的预设 */}
            {favoritePresets.length > 0 && (
              <>
                <div style={{ fontSize: '11px', fontWeight: 600, color: '#666', marginBottom: '8px' }}>
                  ⭐ Favorites
                </div>
                {favoritePresets.map(preset => (
                  <PresetCard
                    key={preset.id}
                    preset={preset}
                    onApply={handleApplyPreset}
                    onEdit={handleEditPreset}
                    onDuplicate={handleDuplicatePreset}
                    onToggleFavorite={handleToggleFavorite}
                    onDelete={handleDeletePreset}
                    isActive={preset.id === activePresetId}
                    compact={compact}
                  />
                ))}
                {(regularPresets.length > 0 || systemPresets.length > 0) && (
                  <Divider style={{ margin: '12px 0' }} />
                )}
              </>
            )}

            {/* 用户自定义预设 */}
            {regularPresets.length > 0 && (
              <>
                <div style={{ fontSize: '11px', fontWeight: 600, color: '#666', marginBottom: '8px' }}>
                  📋 My Presets
                </div>
                {regularPresets.map(preset => (
                  <PresetCard
                    key={preset.id}
                    preset={preset}
                    onApply={handleApplyPreset}
                    onEdit={handleEditPreset}
                    onDuplicate={handleDuplicatePreset}
                    onToggleFavorite={handleToggleFavorite}
                    onDelete={handleDeletePreset}
                    isActive={preset.id === activePresetId}
                    compact={compact}
                  />
                ))}
                {systemPresets.length > 0 && <Divider style={{ margin: '12px 0' }} />}
              </>
            )}

            {/* 系统预设 */}
            {systemPresets.length > 0 && (
              <>
                <div style={{ fontSize: '11px', fontWeight: 600, color: '#666', marginBottom: '8px' }}>
                  💡 System Presets
                </div>
                {systemPresets.map(preset => (
                  <PresetCard
                    key={preset.id}
                    preset={preset}
                    onApply={handleApplyPreset}
                    onEdit={handleEditPreset}
                    onDuplicate={handleDuplicatePreset}
                    onToggleFavorite={handleToggleFavorite}
                    onDelete={handleDeletePreset}
                    isActive={preset.id === activePresetId}
                    compact={compact}
                  />
                ))}
              </>
            )}
          </>
        )}
      </Card>

      {/* 创建/编辑预设对话框 */}
      <CreatePresetModal
        visible={createModalVisible}
        onClose={() => {
          setCreateModalVisible(false);
          setEditingPreset(null);
          // 关闭后刷新列表（如果有修改）
          loadPresets();
        }}
        onSave={editingPreset ? handleUpdatePreset : handleCreatePreset}
        initialFilters={currentFilters}
        editingPreset={editingPreset}
      />

      {/* 预设管理对话框 */}
      <PresetsManager
        visible={adminModalVisible}
        onClose={() => {
          setAdminModalVisible(false);
          loadPresets(); // 刷新预设列表
        }}
      />
    </>
  );
};

export default PresetPanel;


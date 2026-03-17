/**
 * 新闻筛选预设组件 - Ant Design 版本
 * 支持用户创建、管理和应用个性化的新闻筛选预设
 */

import React, { useState, useEffect } from 'react';
import {
  Space,
  Tag,
  Button,
  Dropdown,
  Menu,
  Modal,
  Input,
  Checkbox,
  Typography,
  Tooltip,
  Spin,
  Alert,
  message,
  Form
} from 'antd';
import {
  PlusOutlined,
  MoreOutlined,
  StarFilled,
  StarOutlined,
  DeleteOutlined,
  EditOutlined,
  CopyOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined
} from '@ant-design/icons';

import { newsPresetsAPI } from '../services/newsPresets';
import {
  NewsFilterPreset,
  FilterConfig,
  CreatePresetRequest
} from '../types/newsPresets';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { getModalStyles } from '../utils/modalStyles';

const { TextArea } = Input;
const { Text } = Typography;

interface NewsFilterPresetsProps {
  symbolCode?: string;  // 如果提供，则显示该股票的预设
  onApplyFilter: (config: FilterConfig, preset?: NewsFilterPreset) => void;
  currentFilter?: FilterConfig;  // 当前应用的筛选条件
}

const NewsFilterPresets: React.FC<NewsFilterPresetsProps> = ({
  symbolCode,
  onApplyFilter,
  currentFilter
}) => {
  const { themeMode } = useDashboardSettings();
  // State
  const [presets, setPresets] = useState<NewsFilterPreset[]>([]);
  const [loading, setLoading] = useState(false);
  const [activePresetId, setActivePresetId] = useState<number | null>(null);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  // 创建预设表单状态
  const [form] = Form.useForm();

  // 加载预设
  useEffect(() => {
    loadPresets();
  }, [symbolCode]);

  const loadPresets = async () => {
    setLoading(true);
    setError(null);
    
    try {
      // 1. 获取系统预设
      const systemResponse = await newsPresetsAPI.getSystemPresets();
      const systemPresets = systemResponse.presets || [];
      
      // 2. 获取用户全局预设
      const globalResponse = await newsPresetsAPI.getPresets({ scope_type: 'global' });
      const globalPresets = globalResponse.presets || [];
      
      // 3. 如果是特定股票，获取股票级别预设
      let symbolPresets: NewsFilterPreset[] = [];
      if (symbolCode) {
        const symbolResponse = await newsPresetsAPI.getPresets({
          scope_type: 'symbol_specific',
          symbol_code: symbolCode
        });
        symbolPresets = symbolResponse.presets || [];
      }
      
      // 合并并排序：系统预设 -> 股票收藏预设 -> 全局收藏预设 -> 其他
      const allPresets = [
        ...systemPresets,
        ...symbolPresets.filter(p => p.is_favorite),
        ...globalPresets.filter(p => p.is_favorite),
        ...symbolPresets.filter(p => !p.is_favorite),
        ...globalPresets.filter(p => !p.is_favorite)
      ];
      
      setPresets(allPresets);
      
      // 自动应用默认预设
      const defaultPreset = allPresets.find(p => p.is_default && 
        (p.scope_type === 'global' || (p.scope_type === 'symbol_specific' && p.symbol_code === symbolCode))
      );
      if (defaultPreset && !currentFilter) {
        applyPreset(defaultPreset);
      }
      
    } catch (err: any) {
      console.error('Failed to load presets:', err);
      setError(err.message || '加载预设失败');
      message.error('加载预设失败');
    } finally {
      setLoading(false);
    }
  };

  const applyPreset = async (preset: NewsFilterPreset) => {
    setActivePresetId(preset.id);
    onApplyFilter(preset.filter_config, preset);
    
    // 更新使用统计（仅自定义预设）
    if (preset.id > 0 && preset.preset_type === 'custom') {
      try {
        await newsPresetsAPI.usePreset(preset.id);
      } catch (err) {
        console.error('Failed to update usage stats:', err);
      }
    }
  };

  const handleCreatePreset = async (values: any) => {
    if (!currentFilter) {
      message.error('当前没有筛选条件可保存');
      return;
    }
    
    try {
      const request: CreatePresetRequest = {
        preset_name: values.preset_name,
        description: values.description || undefined,
        icon: values.icon || undefined,
        filter_config: currentFilter,
        scope_type: values.save_as_global ? 'global' : 'symbol_specific',
        symbol_code: values.save_as_global ? undefined : symbolCode,
        is_default: false,
        is_favorite: false
      };
      
      await newsPresetsAPI.createPreset(request);
      
      message.success('预设创建成功');
      form.resetFields();
      setShowCreateModal(false);
      
      // 重新加载预设列表
      await loadPresets();
      
    } catch (err: any) {
      console.error('Failed to create preset:', err);
      message.error(err.message || '创建预设失败');
    }
  };

  const handleToggleFavorite = async (preset: NewsFilterPreset) => {
    try {
      await newsPresetsAPI.toggleFavorite(preset);
      message.success(preset.is_favorite ? '已取消收藏' : '已收藏');
      await loadPresets();
    } catch (err: any) {
      console.error('Failed to toggle favorite:', err);
      message.error('操作失败');
    }
  };

  const handleDeletePreset = async (presetId: number) => {
    Modal.confirm({
      title: '确定要删除此预设吗？',
      content: '删除后无法恢复',
      okText: '删除',
      okType: 'danger',
      cancelText: '取消',
      onOk: async () => {
        try {
          await newsPresetsAPI.deletePreset(presetId);
          message.success('预设已删除');
          await loadPresets();
          
          if (activePresetId === presetId) {
            setActivePresetId(null);
          }
        } catch (err: any) {
          console.error('Failed to delete preset:', err);
          message.error('删除失败');
        }
      }
    });
  };

  const handleDuplicatePreset = async (preset: NewsFilterPreset) => {
    Modal.confirm({
      title: '复制预设',
      content: (
        <Input
          placeholder={`${preset.preset_name} (副本)`}
          id="duplicate-preset-name"
        />
      ),
      okText: '复制',
      cancelText: '取消',
      onOk: async () => {
        const inputElement = document.getElementById('duplicate-preset-name') as HTMLInputElement;
        const newName = inputElement?.value || `${preset.preset_name} (副本)`;
        
        try {
          await newsPresetsAPI.duplicatePreset(preset.id, newName);
          message.success('预设已复制');
          await loadPresets();
        } catch (err: any) {
          console.error('Failed to duplicate preset:', err);
          message.error('复制失败');
        }
      }
    });
  };

  const handleSetAsDefault = async (preset: NewsFilterPreset) => {
    try {
      await newsPresetsAPI.setAsDefault(preset.id);
      message.success('已设为默认预设');
      await loadPresets();
    } catch (err: any) {
      console.error('Failed to set as default:', err);
      message.error('设置失败');
    }
  };

  // 构建预设操作菜单
  const getPresetMenu = (preset: NewsFilterPreset) => (
    <Menu>
      {preset.preset_type === 'custom' && (
        <>
          <Menu.Item
            key="favorite"
            icon={preset.is_favorite ? <StarFilled /> : <StarOutlined />}
            onClick={() => handleToggleFavorite(preset)}
          >
            {preset.is_favorite ? '取消收藏' : '收藏'}
          </Menu.Item>
          {!preset.is_default && (
            <Menu.Item
              key="default"
              icon={<CheckCircleOutlined />}
              onClick={() => handleSetAsDefault(preset)}
            >
              设为默认
            </Menu.Item>
          )}
        </>
      )}
      <Menu.Item
        key="duplicate"
        icon={<CopyOutlined />}
        onClick={() => handleDuplicatePreset(preset)}
      >
        复制
      </Menu.Item>
      {preset.preset_type === 'custom' && (
        <Menu.Item
          key="delete"
          danger
          icon={<DeleteOutlined />}
          onClick={() => handleDeletePreset(preset.id)}
        >
          删除
        </Menu.Item>
      )}
    </Menu>
  );

  return (
    <div style={{ marginBottom: 16 }}>
      {/* 错误提示 */}
      {error && (
        <Alert
          message={error}
          type="error"
          closable
          onClose={() => setError(null)}
          style={{ marginBottom: 12 }}
        />
      )}
      
      {/* 预设快捷按钮 */}
      <Space wrap style={{ width: '100%' }}>
        {loading ? (
          <Spin size="small" />
        ) : (
          <>
            {presets.slice(0, 6).map((preset) => (
              <Dropdown
                key={preset.id}
                overlay={getPresetMenu(preset)}
                trigger={['contextMenu']}
              >
                <Tag
                  color={activePresetId === preset.id ? 'blue' : 'default'}
                  style={{
                    cursor: 'pointer',
                    fontSize: '13px',
                    padding: '4px 12px',
                    borderRadius: '4px',
                    maxWidth: 150,
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap'
                  }}
                  onClick={() => applyPreset(preset)}
                  icon={preset.is_favorite ? <StarFilled style={{ fontSize: 12 }} /> : undefined}
                  closable={preset.preset_type === 'custom'}
                  onClose={(e) => {
                    e.preventDefault();
                    handleDeletePreset(preset.id);
                  }}
                >
                  {preset.preset_name}
                </Tag>
              </Dropdown>
            ))}
            
            {/* 保存当前筛选按钮 */}
            <Tooltip title="保存当前筛选为预设">
              <Button
                type="dashed"
                size="small"
                icon={<PlusOutlined />}
                onClick={() => setShowCreateModal(true)}
                disabled={!currentFilter}
              >
                保存筛选
              </Button>
            </Tooltip>
            
            {/* 更多预设菜单 */}
            {presets.length > 6 && (
              <Dropdown
                overlay={
                  <Menu>
                    {presets.slice(6).map((preset) => (
                      <Menu.Item
                        key={preset.id}
                        onClick={() => applyPreset(preset)}
                      >
                        {preset.icon} {preset.preset_name}
                      </Menu.Item>
                    ))}
                  </Menu>
                }
                trigger={['click']}
              >
                <Button size="small" icon={<MoreOutlined />} />
              </Dropdown>
            )}
          </>
        )}
      </Space>
      
      {/* 创建预设模态框 */}
      <Modal
        title="保存筛选预设"
        open={showCreateModal}
        onCancel={() => {
          setShowCreateModal(false);
          form.resetFields();
        }}
        onOk={() => form.submit()}
        okText="保存"
        cancelText="取消"
        width={500}
        styles={getModalStyles(themeMode)}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleCreatePreset}
          initialValues={{
            save_as_global: !symbolCode
          }}
        >
          <Form.Item
            label="预设名称"
            name="preset_name"
            rules={[{ required: true, message: '请输入预设名称' }]}
          >
            <Input placeholder="例如：我的财报追踪" />
          </Form.Item>
          
          <Form.Item
            label="描述（可选）"
            name="description"
          >
            <TextArea
              rows={2}
              placeholder="简要描述此预设的用途"
            />
          </Form.Item>
          
          <Form.Item
            label="图标（可选）"
            name="icon"
          >
            <Input
              placeholder="例如：📊 或 ⭐"
              maxLength={10}
            />
          </Form.Item>
          
          {symbolCode && (
            <Form.Item
              name="save_as_global"
              valuePropName="checked"
            >
              <Checkbox>
                应用到所有股票（取消勾选则仅应用于 {symbolCode}）
              </Checkbox>
            </Form.Item>
          )}
          
          <Alert
            message="当前筛选条件将被保存为预设，方便日后快速应用"
            type="info"
            showIcon
            style={{ marginTop: 8 }}
          />
        </Form>
      </Modal>
    </div>
  );
};

export default NewsFilterPresets;

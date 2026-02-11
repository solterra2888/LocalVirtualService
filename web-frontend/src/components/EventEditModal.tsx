import React, { useEffect, useState } from 'react';
import {
  Modal,
  Form,
  Input,
  Select,
  DatePicker,
  Space,
  Typography,
  Button,
  Popconfirm,
  message
} from 'antd';
import { PlusOutlined, DeleteOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { 
  ChartEvent, 
  EventType, 
  EventPosition, 
  EVENT_TYPE_CONFIGS, 
  EventFormData 
} from '../types/events';

const { TextArea } = Input;
const { Option } = Select;
const { Text } = Typography;

const THEME_COLORS = {
  light: {
    cardBg: '#fff',
    cardBorder: 'rgba(0, 0, 0, 0.06)',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    codeBlockBg: '#f6f6f6',
  },
  dark: {
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
    codeBlockBg: '#1E202B',
  },
} as const;

const CARD_STYLE_BEHANCE_DARK = {
  borderRadius: 12,
  boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
};

interface EventEditModalProps {
  visible: boolean;
  onCancel: () => void;
  onSave: (eventData: EventFormData) => Promise<void>;
  onDelete?: (eventId: string) => Promise<void>; // 删除事件的回调
  event?: ChartEvent | null; // 如果为空则为新增模式
  defaultDate?: string; // 默认日期（从图表点击获取）
  loading?: boolean;
  availableTradingDates?: string[]; // 图表中可用的交易日列表（YYYY-MM-DD格式）
}

const EventEditModal: React.FC<EventEditModalProps> = ({
  visible,
  onCancel,
  onSave,
  onDelete,
  event,
  defaultDate,
  loading = false,
  availableTradingDates = []
}) => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const [form] = Form.useForm();
  const [deleteLoading, setDeleteLoading] = useState(false);
  const isEdit = !!event;

  // 判断日期是否为交易日
  const isTradingDay = (date: dayjs.Dayjs | null): boolean => {
    if (!date || availableTradingDates.length === 0) {
      // 如果没有交易日列表，允许所有日期（向后兼容）
      return true;
    }
    const dateStr = date.format('YYYY-MM-DD');
    return availableTradingDates.includes(dateStr);
  };

  // DatePicker 的 disabledDate 函数：禁用非交易日
  const disabledDate = (current: dayjs.Dayjs | null): boolean => {
    if (!current) return false;
    // 编辑模式下，如果当前日期就是已有事件的日期，允许选择（即使不在交易日列表中）
    if (isEdit && event) {
      const eventDate = dayjs(event.date);
      if (current.format('YYYY-MM-DD') === eventDate.format('YYYY-MM-DD')) {
        return false;
      }
    }
    // 禁用非交易日
    return !isTradingDay(current);
  };

  useEffect(() => {
    if (visible) {
      if (event) {
        // 编辑模式：填充现有数据
        form.setFieldsValue({
          date: dayjs(event.date),
          type: event.type,
          title: event.title,
          description: event.description,
          relatedUrl: event.relatedUrl,
          relatedFileId: event.relatedFileId,
        });
      } else {
        // 新增模式：设置默认值
        form.setFieldsValue({
          date: defaultDate ? dayjs(defaultDate) : dayjs(),
          type: EventType.CUSTOM,
        });
      }
    }
  }, [visible, event, defaultDate, form]);

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      const selectedDate = values.date.format('YYYY-MM-DD');
      
      // 最终验证：确保选择的日期是交易日（除非是编辑已有事件且日期未改变）
      if (!isEdit || (isEdit && event && event.date !== selectedDate)) {
        if (!isTradingDay(values.date)) {
          message.error('Please select a trading day that exists in the chart');
          return;
        }
      }
      
      const eventData: EventFormData = {
        date: selectedDate,
        type: values.type,
        title: values.title,
        description: values.description,
        position: EventPosition.MIDDLE, // 默认使用 middle
        relatedUrl: values.relatedUrl,
        relatedFileId: values.relatedFileId,
      };
      
      await onSave(eventData);
      form.resetFields();
    } catch (error) {
      console.error('Form validation failed:', error);
    }
  };

  const handleCancel = () => {
    form.resetFields();
    onCancel();
  };

  const handleDelete = async () => {
    if (!event || !onDelete) return;
    
    setDeleteLoading(true);
    try {
      await onDelete(event.id);
      form.resetFields();
    } catch (error) {
      console.error('Delete event failed:', error);
    } finally {
      setDeleteLoading(false);
    }
  };

  const modalStyles = isDark
    ? {
        content: {
          borderRadius: 12,
          boxShadow: CARD_STYLE_BEHANCE_DARK.boxShadow,
          border: CARD_STYLE_BEHANCE_DARK.border,
          backgroundColor: colors.cardBg,
        },
        header: {
          color: colors.text,
          borderBottomColor: colors.cardBorder,
        },
        body: { color: colors.text },
      }
    : undefined;

  return (
    <Modal
      title={
        <Space style={{ color: 'inherit' }}>
          <PlusOutlined />
          {isEdit ? 'Edit Event' : 'Add Event Marker'}
        </Space>
      }
      open={visible}
      centered
      onCancel={handleCancel}
      onOk={handleSave}
      confirmLoading={loading}
      width={500}
      destroyOnClose
      styles={modalStyles}
      footer={[
        isEdit && onDelete && (
          <Popconfirm
            key="delete"
            title="Delete Event"
            description="Are you sure you want to delete this event? This action cannot be undone."
            onConfirm={handleDelete}
            okText="Yes, Delete"
            cancelText="Cancel"
            okButtonProps={{ danger: true, loading: deleteLoading }}
          >
            <Button
              danger
              icon={<DeleteOutlined />}
              style={{ float: 'left' }}
              loading={deleteLoading}
            >
              Delete
            </Button>
          </Popconfirm>
        ),
        <Button
          key="cancel"
          onClick={handleCancel}
          style={isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined}
        >
          Cancel
        </Button>,
        <Button
          key="save"
          type="primary"
          onClick={handleSave}
          loading={loading}
          style={isDark ? { background: '#2196F3', borderColor: '#2196F3' } : undefined}
        >
          {isEdit ? 'Update' : 'Create'}
        </Button>
      ]}
    >
      <Form
        form={form}
        layout="vertical"
        size="middle"
      >
        <Form.Item
          label="Date"
          name="date"
          rules={[
            { required: true, message: 'Please select a date' },
            {
              validator: (_, value) => {
                if (!value) {
                  return Promise.resolve();
                }
                if (!isTradingDay(value)) {
                  return Promise.reject(
                    new Error('Please select a trading day that exists in the chart')
                  );
                }
                return Promise.resolve();
              }
            }
          ]}
        >
          <DatePicker 
            style={{ width: '100%' }}
            format="YYYY-MM-DD"
            placeholder="Select event date"
            disabledDate={disabledDate}
          />
        </Form.Item>

        <Form.Item
          label="Event Type"
          name="type"
          rules={[{ required: true, message: 'Please select an event type' }]}
        >
          <Select
            placeholder="Select event type"
          >
            {Object.entries(EVENT_TYPE_CONFIGS).map(([type, config]) => (
              <Option key={type} value={type}>
                <Space>
                  <span>{config.icon}</span>
                  <span>{config.label}</span>
                </Space>
              </Option>
            ))}
          </Select>
        </Form.Item>

        <Form.Item
          label="Title"
          name="title"
          rules={[
            { required: true, message: 'Please enter event title' },
            { max: 100, message: 'Title cannot exceed 100 characters' }
          ]}
        >
          <Input placeholder="Enter event title" />
        </Form.Item>

        <Form.Item
          label="Description"
          name="description"
          rules={[{ max: 500, message: 'Description cannot exceed 500 characters' }]}
        >
          <TextArea 
            rows={3}
            placeholder="Enter event description (optional)"
            showCount
            maxLength={500}
          />
        </Form.Item>

        <Form.Item
          label="Related Link"
          name="relatedUrl"
          rules={[
            { type: 'url', message: 'Please enter a valid URL' }
          ]}
        >
          <Input 
            placeholder="Enter related news or webpage link (optional)"
            prefix="🔗"
          />
        </Form.Item>

        <Form.Item
          label="Related File ID"
          name="relatedFileId"
        >
          <Input 
            placeholder="Enter related file ID (optional)"
            prefix="📁"
          />
        </Form.Item>
      </Form>
      
      <div style={{ marginTop: '16px', padding: '12px', backgroundColor: colors.codeBlockBg, borderRadius: '6px' }}>
        <Text type="secondary" style={{ fontSize: '12px' }}>
          💡 Tip: Event markers will be displayed on the candlestick chart for the corresponding date. Different event types have different icons and colors for easy identification.
          {availableTradingDates.length > 0 && (
            <>
              <br />
              📅 Only trading days shown in the chart are selectable ({availableTradingDates.length} days available).
            </>
          )}
        </Text>
      </div>
    </Modal>
  );
};

export default EventEditModal;

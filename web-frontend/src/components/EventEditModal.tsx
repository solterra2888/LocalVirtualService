import React, { useEffect } from 'react';
import {
  Modal,
  Form,
  Input,
  Select,
  DatePicker,
  Button,
  Space,
  Typography,
  message
} from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import { 
  ChartEvent, 
  EventType, 
  EventPosition, 
  EVENT_TYPE_CONFIGS, 
  EventFormData 
} from './eventTypes';

const { TextArea } = Input;
const { Option } = Select;
const { Text } = Typography;

interface EventEditModalProps {
  visible: boolean;
  onCancel: () => void;
  onSave: (eventData: EventFormData) => Promise<void>;
  event?: ChartEvent | null; // 如果为空则为新增模式
  defaultDate?: string; // 默认日期（从图表点击获取）
  loading?: boolean;
}

const EventEditModal: React.FC<EventEditModalProps> = ({
  visible,
  onCancel,
  onSave,
  event,
  defaultDate,
  loading = false
}) => {
  const [form] = Form.useForm();
  const isEdit = !!event;

  useEffect(() => {
    if (visible) {
      if (event) {
        // 编辑模式：填充现有数据
        form.setFieldsValue({
          date: dayjs(event.date),
          type: event.type,
          title: event.title,
          description: event.description,
          position: event.position || EVENT_TYPE_CONFIGS[event.type].defaultPosition,
          relatedUrl: event.relatedUrl,
          relatedFileId: event.relatedFileId,
        });
      } else {
        // 新增模式：设置默认值
        form.setFieldsValue({
          date: defaultDate ? dayjs(defaultDate) : dayjs(),
          type: EventType.CUSTOM,
          position: EventPosition.MIDDLE,
        });
      }
    }
  }, [visible, event, defaultDate, form]);

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      const eventData: EventFormData = {
        date: values.date.format('YYYY-MM-DD'),
        type: values.type,
        title: values.title,
        description: values.description,
        position: values.position,
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

  const handleTypeChange = (type: EventType) => {
    // 当事件类型改变时，自动设置默认位置
    const config = EVENT_TYPE_CONFIGS[type];
    form.setFieldsValue({
      position: config.defaultPosition
    });
  };

  return (
    <Modal
      title={
        <Space>
          <PlusOutlined />
          {isEdit ? 'Edit Event' : 'Add Event Marker'}
        </Space>
      }
      open={visible}
      onCancel={handleCancel}
      onOk={handleSave}
      confirmLoading={loading}
      width={500}
      destroyOnClose
    >
      <Form
        form={form}
        layout="vertical"
        size="middle"
      >
        <Form.Item
          label="Date"
          name="date"
          rules={[{ required: true, message: 'Please select a date' }]}
        >
          <DatePicker 
            style={{ width: '100%' }}
            format="YYYY-MM-DD"
            placeholder="Select event date"
          />
        </Form.Item>

        <Form.Item
          label="Event Type"
          name="type"
          rules={[{ required: true, message: 'Please select an event type' }]}
        >
          <Select
            placeholder="Select event type"
            onChange={handleTypeChange}
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
          label="Display Position"
          name="position"
          rules={[{ required: true, message: 'Please select display position' }]}
        >
          <Select placeholder="Select position on chart">
            <Option value={EventPosition.TOP}>
              <Space>
                <span>🔝</span>
                <span>Top</span>
              </Space>
            </Option>
            <Option value={EventPosition.MIDDLE}>
              <Space>
                <span>🎯</span>
                <span>Middle</span>
              </Space>
            </Option>
            <Option value={EventPosition.BOTTOM}>
              <Space>
                <span>🔻</span>
                <span>Bottom</span>
              </Space>
            </Option>
          </Select>
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
      
      <div style={{ marginTop: '16px', padding: '12px', backgroundColor: '#f6f6f6', borderRadius: '6px' }}>
        <Text type="secondary" style={{ fontSize: '12px' }}>
          💡 Tip: Event markers will be displayed on the candlestick chart for the corresponding date. Different event types have different icons and colors for easy identification.
        </Text>
      </div>
    </Modal>
  );
};

export default EventEditModal;

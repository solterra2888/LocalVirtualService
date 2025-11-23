import React, { useState, useEffect } from 'react';
import { Modal, List, Button, Space, Popconfirm, message, Empty, Tag, Input, Form } from 'antd';
import { FolderOutlined, DeleteOutlined, EditOutlined, PlusOutlined } from '@ant-design/icons';
import { dashboardAPI, CustomGroup } from '../services/dashboard';

interface ManageCustomGroupsModalProps {
  visible: boolean;
  onClose: () => void;
  onUpdate: () => void;
}

const ManageCustomGroupsModal: React.FC<ManageCustomGroupsModalProps> = ({
  visible,
  onClose,
  onUpdate,
}) => {
  const [customGroups, setCustomGroups] = useState<CustomGroup[]>([]);
  const [loading, setLoading] = useState(false);
  const [editingGroupId, setEditingGroupId] = useState<number | null>(null);
  const [form] = Form.useForm();

  useEffect(() => {
    if (visible) {
      loadCustomGroups();
    }
  }, [visible]);

  const loadCustomGroups = async () => {
    try {
      setLoading(true);
      const groups = await dashboardAPI.getCustomGroups();
      setCustomGroups(groups);
    } catch (error) {
      console.error('Failed to load custom groups:', error);
      message.error('Failed to load custom groups');
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async (groupId: number, groupName: string) => {
    try {
      await dashboardAPI.deleteCustomGroup(groupId);
      message.success(`Group "${groupName}" deleted successfully`);
      await loadCustomGroups();
      onUpdate(); // 通知父组件更新
    } catch (error: any) {
      console.error('Failed to delete custom group:', error);
      if (error.response?.data?.detail) {
        message.error(error.response.data.detail);
      } else {
        message.error('Failed to delete custom group');
      }
    }
  };

  const handleEdit = (group: CustomGroup) => {
    setEditingGroupId(group.id);
    form.setFieldsValue({
      group_name: group.group_name,
      description: group.description || '',
    });
  };

  const handleCancelEdit = () => {
    setEditingGroupId(null);
    form.resetFields();
  };

  const handleSaveEdit = async (groupId: number) => {
    try {
      const values = await form.validateFields();
      await dashboardAPI.updateCustomGroup(groupId, values);
      message.success('Group updated successfully');
      setEditingGroupId(null);
      form.resetFields();
      await loadCustomGroups();
      onUpdate(); // 通知父组件更新
    } catch (error: any) {
      console.error('Failed to update custom group:', error);
      if (error.response?.data?.detail) {
        message.error(error.response.data.detail);
      } else {
        message.error('Failed to update custom group');
      }
    }
  };

  return (
    <Modal
      title={
        <Space>
          <FolderOutlined />
          <span>Manage Custom Groups</span>
        </Space>
      }
      open={visible}
      onCancel={onClose}
      footer={[
        <Button key="close" onClick={onClose}>
          Close
        </Button>,
      ]}
      width={600}
    >
      {customGroups.length === 0 ? (
        <Empty
          description="No custom groups yet"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        >
          <Button type="primary" icon={<PlusOutlined />} onClick={onClose}>
            Create Your First Group
          </Button>
        </Empty>
      ) : (
        <List
          loading={loading}
          dataSource={customGroups}
          renderItem={(group) => (
            <List.Item
              key={group.id}
              style={{
                padding: '16px',
                border: '1px solid #f0f0f0',
                borderRadius: '8px',
                marginBottom: '12px',
                backgroundColor: editingGroupId === group.id ? '#f5f5f5' : '#fff',
              }}
            >
              {editingGroupId === group.id ? (
                <div style={{ width: '100%' }}>
                  <Form form={form} layout="vertical">
                    <Form.Item
                      name="group_name"
                      label="Group Name"
                      rules={[
                        { required: true, message: 'Please enter group name' },
                        { max: 100, message: 'Name too long' },
                      ]}
                      style={{ marginBottom: '12px' }}
                    >
                      <Input placeholder="Group name" />
                    </Form.Item>
                    <Form.Item
                      name="description"
                      label="Description"
                      style={{ marginBottom: '12px' }}
                    >
                      <Input.TextArea
                        placeholder="Description (optional)"
                        rows={2}
                        maxLength={500}
                      />
                    </Form.Item>
                  </Form>
                  <Space>
                    <Button
                      type="primary"
                      size="small"
                      onClick={() => handleSaveEdit(group.id)}
                    >
                      Save
                    </Button>
                    <Button size="small" onClick={handleCancelEdit}>
                      Cancel
                    </Button>
                  </Space>
                </div>
              ) : (
                <div
                  style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    width: '100%',
                  }}
                >
                  <Space direction="vertical" size={4} style={{ flex: 1 }}>
                    <Space>
                      <div
                        style={{
                          width: 16,
                          height: 16,
                          borderRadius: '50%',
                          backgroundColor: group.color || '#1890ff',
                        }}
                      />
                      <span style={{ fontWeight: 500, fontSize: '15px' }}>
                        {group.group_name}
                      </span>
                      <Tag color="blue">{group.member_count || 0} tickers</Tag>
                    </Space>
                    {group.description && (
                      <div style={{ color: '#8c8c8c', fontSize: '13px' }}>
                        {group.description}
                      </div>
                    )}
                  </Space>
                  <Space>
                    <Button
                      type="text"
                      icon={<EditOutlined />}
                      size="small"
                      onClick={() => handleEdit(group)}
                    >
                      Edit
                    </Button>
                    <Popconfirm
                      title="Delete this group?"
                      description={`Are you sure you want to delete "${group.group_name}"? This will remove all tickers from this group.`}
                      onConfirm={() => handleDelete(group.id, group.group_name)}
                      okText="Yes"
                      cancelText="No"
                      okButtonProps={{ danger: true }}
                    >
                      <Button
                        type="text"
                        danger
                        icon={<DeleteOutlined />}
                        size="small"
                      >
                        Delete
                      </Button>
                    </Popconfirm>
                  </Space>
                </div>
              )}
            </List.Item>
          )}
        />
      )}
    </Modal>
  );
};

export default ManageCustomGroupsModal;


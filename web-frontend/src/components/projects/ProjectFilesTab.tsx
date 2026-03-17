/**
 * Project Files Tab Component
 * 项目文件管理Tab - 文件列表、上传、下载
 */

import React, { useState, useEffect } from 'react';
import {
  Table,
  Button,
  Space,
  Tag,
  Modal,
  Form,
  Input,
  Select,
  message,
  Popconfirm,
  Empty,
  Typography
} from 'antd';
import {
  UploadOutlined,
  FileOutlined,
  DownloadOutlined,
  DeleteOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';
import { api } from '../../services/api';

import { ProjectFile, ProjectFileCreateRequest } from '../../types/project';
import { useDashboardSettings } from '../../contexts/DashboardSettingsContext';
import { getModalStyles } from '../../utils/modalStyles';

const { TextArea } = Input;
const { Option } = Select;
const { Text } = Typography;

interface ProjectFilesTabProps {
  projectId: number;
  permissions: any;
}

// API服务

const getProjectFiles = async (projectId: number): Promise<ProjectFile[]> => {
  return await api.get(`/projects/${projectId}/files`);
};

const addProjectFile = async (
  projectId: number,
  data: ProjectFileCreateRequest
): Promise<ProjectFile> => {
  return await api.post(`/projects/${projectId}/files`, data);
};

const deleteProjectFile = async (
  projectId: number,
  fileId: number
): Promise<any> => {
  return await api.delete(`/projects/${projectId}/files/${fileId}`);
};

const ProjectFilesTab: React.FC<ProjectFilesTabProps> = ({
  projectId,
  permissions
}) => {
  const { themeMode } = useDashboardSettings();
  const [files, setFiles] = useState<ProjectFile[]>([]);
  const [loading, setLoading] = useState(false);
  const [addModalVisible, setAddModalVisible] = useState(false);
  const [form] = Form.useForm();

  // 加载文件列表
  const loadFiles = async () => {
    setLoading(true);
    try {
      const data = await getProjectFiles(projectId);
      setFiles(data);
    } catch (error: any) {
      message.error(`Failed to load file list: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadFiles();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectId]);

  // 添加文件记录
  const handleAddFile = async (values: any) => {
    try {
      const fileData: ProjectFileCreateRequest = {
        file_name: values.file_name,
        file_path: values.file_path || `/uploads/projects/${projectId}/${values.file_name}`,
        file_type: values.file_type,
        file_size: values.file_size,
        category: values.category,
        description: values.description
      };

      await addProjectFile(projectId, fileData);
      message.success('File record added successfully');
      setAddModalVisible(false);
      form.resetFields();
      loadFiles();
    } catch (error: any) {
      message.error(`Add failed: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 删除文件
  const handleDeleteFile = async (fileId: number, fileName: string) => {
    try {
      await deleteProjectFile(projectId, fileId);
      message.success(`File deleted: ${fileName}`);
      loadFiles();
    } catch (error: any) {
      message.error(`Delete failed: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 格式化文件大小
  const formatFileSize = (bytes?: number) => {
    if (!bytes) return '-';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(2) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
  };

  // 获取文件类型标签颜色
  const getFileTypeColor = (type?: string) => {
    if (!type) return 'default';
    if (type.includes('pdf')) return 'red';
    if (type.includes('word') || type.includes('doc')) return 'blue';
    if (type.includes('excel') || type.includes('sheet')) return 'green';
    if (type.includes('image')) return 'orange';
    return 'default';
  };

  // 获取分类标签
  const getCategoryTag = (category?: string) => {
    const categoryMap: Record<string, { text: string; color: string }> = {
      meeting_notes: { text: 'Meeting Notes', color: 'blue' },
      report: { text: 'Research Report', color: 'green' },
      contract: { text: 'Contract', color: 'red' },
      presentation: { text: 'Presentation', color: 'orange' },
      other: { text: 'Other', color: 'default' }
    };
    const info = category ? categoryMap[category] : null;
    return info ? (
      <Tag color={info.color}>{info.text}</Tag>
    ) : (
      <Tag>Uncategorized</Tag>
    );
  };

  const canEdit = permissions?.permissions?.can_edit || false;

  // 表格列定义
  const columns: ColumnsType<ProjectFile> = [
    {
      title: 'File Name',
      dataIndex: 'file_name',
      key: 'file_name',
      width: 300,
      render: (name, record) => (
        <Space>
          <FileOutlined style={{ color: '#1890ff' }} />
          <Text>{name}</Text>
        </Space>
      )
    },
    {
      title: 'Type',
      dataIndex: 'file_type',
      key: 'file_type',
      width: 150,
      render: (type: string) => (
        <Tag color={getFileTypeColor(type)}>
          {type || '未知'}
        </Tag>
      )
    },
    {
      title: 'Category',
      dataIndex: 'category',
      key: 'category',
      width: 120,
      render: (category: string) => getCategoryTag(category)
    },
    {
      title: 'Size',
      dataIndex: 'file_size',
      key: 'file_size',
      width: 100,
      render: (size: number) => formatFileSize(size)
    },
    {
      title: 'Description',
      dataIndex: 'description',
      key: 'description',
      width: 200,
      ellipsis: true,
      render: (desc: string) => desc || '-'
    },
    {
      title: 'Uploaded By',
      dataIndex: 'uploaded_by_username',
      key: 'uploaded_by',
      width: 120,
      render: (username: string, record) => username || `User ${record.uploaded_by}`
    },
    {
      title: 'Uploaded At',
      dataIndex: 'uploaded_at',
      key: 'uploaded_at',
      width: 150,
      render: (date: string) => dayjs(date).format('YYYY-MM-DD HH:mm')
    },
    {
      title: 'Actions',
      key: 'action',
      width: 120,
      fixed: 'right',
      render: (_, record) => {
        return (
          <Space>
            <Button
              type="link"
              size="small"
              icon={<DownloadOutlined />}
              disabled
            >
              Download
            </Button>
            {canEdit && (
              <Popconfirm
                title="Confirm Delete File?"
                description="This will delete the file record"
                onConfirm={() => handleDeleteFile(record.id, record.file_name)}
                okText="Confirm"
                cancelText="Cancel"
              >
                <Button
                  type="link"
                  danger
                  size="small"
                  icon={<DeleteOutlined />}
                />
              </Popconfirm>
            )}
          </Space>
        );
      }
    }
  ];

  return (
    <div>
      {/* 操作栏 */}
      <Space style={{ marginBottom: 16 }}>
        {canEdit && (
          <Button
            type="primary"
            icon={<UploadOutlined />}
            onClick={() => setAddModalVisible(true)}
          >
            Add File Record
          </Button>
        )}
        <Text type="secondary">
          Note: Full file upload functionality is under development, currently only supports adding file records
        </Text>
      </Space>

      {/* 文件列表 */}
      {files.length > 0 ? (
        <Table
          columns={columns}
          dataSource={files}
          rowKey="id"
          loading={loading}
          pagination={{ pageSize: 20 }}
          bordered
        />
      ) : (
        <Empty
          description="No files yet, click 'Add File Record' to start"
          style={{ padding: '60px 0' }}
        />
      )}

      {/* 添加文件记录模态框 */}
      <Modal
        title="Add File Record"
        open={addModalVisible}
        onOk={() => form.submit()}
        onCancel={() => {
          setAddModalVisible(false);
          form.resetFields();
        }}
        width={600}
        okText="Add"
        cancelText="Cancel"
        styles={getModalStyles(themeMode)}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleAddFile}
        >
          <Form.Item
            label="File Name"
            name="file_name"
            rules={[{ required: true, message: 'Please enter file name' }]}
          >
            <Input placeholder="e.g., Q1 Earnings Analysis.pdf" />
          </Form.Item>

          <Form.Item
            label="File Path"
            name="file_path"
            tooltip="Optional, file storage path or URL"
          >
            <Input placeholder="File path or URL" />
          </Form.Item>

          <Form.Item
            label="File Type"
            name="file_type"
          >
            <Input placeholder="e.g., application/pdf" />
          </Form.Item>

          <Form.Item
            label="File Size (bytes)"
            name="file_size"
          >
            <Input type="number" placeholder="File size" />
          </Form.Item>

          <Form.Item
            label="Category"
            name="category"
          >
            <Select placeholder="Select file category">
              <Option value="meeting_notes">Meeting Notes</Option>
              <Option value="report">Research Report</Option>
              <Option value="contract">Contract</Option>
              <Option value="presentation">Presentation</Option>
              <Option value="other">Other</Option>
            </Select>
          </Form.Item>

          <Form.Item
            label="Description"
            name="description"
          >
            <TextArea rows={3} placeholder="File description or notes..." />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default ProjectFilesTab;


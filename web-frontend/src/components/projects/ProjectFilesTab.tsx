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
      message.error(`加载文件列表失败: ${error.response?.data?.detail || error.message}`);
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
      message.success('文件记录添加成功');
      setAddModalVisible(false);
      form.resetFields();
      loadFiles();
    } catch (error: any) {
      message.error(`添加失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 删除文件
  const handleDeleteFile = async (fileId: number, fileName: string) => {
    try {
      await deleteProjectFile(projectId, fileId);
      message.success(`已删除文件: ${fileName}`);
      loadFiles();
    } catch (error: any) {
      message.error(`删除失败: ${error.response?.data?.detail || error.message}`);
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
      meeting_notes: { text: '会议纪要', color: 'blue' },
      report: { text: '研究报告', color: 'green' },
      contract: { text: '合同文档', color: 'red' },
      presentation: { text: '演示文稿', color: 'orange' },
      other: { text: '其他', color: 'default' }
    };
    const info = category ? categoryMap[category] : null;
    return info ? (
      <Tag color={info.color}>{info.text}</Tag>
    ) : (
      <Tag>未分类</Tag>
    );
  };

  const canEdit = permissions?.permissions?.can_edit || false;

  // 表格列定义
  const columns: ColumnsType<ProjectFile> = [
    {
      title: '文件名',
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
      title: '类型',
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
      title: '分类',
      dataIndex: 'category',
      key: 'category',
      width: 120,
      render: (category: string) => getCategoryTag(category)
    },
    {
      title: '大小',
      dataIndex: 'file_size',
      key: 'file_size',
      width: 100,
      render: (size: number) => formatFileSize(size)
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      width: 200,
      ellipsis: true,
      render: (desc: string) => desc || '-'
    },
    {
      title: '上传者',
      dataIndex: 'uploaded_by_username',
      key: 'uploaded_by',
      width: 120,
      render: (username: string, record) => username || `用户${record.uploaded_by}`
    },
    {
      title: '上传时间',
      dataIndex: 'uploaded_at',
      key: 'uploaded_at',
      width: 150,
      render: (date: string) => dayjs(date).format('YYYY-MM-DD HH:mm')
    },
    {
      title: '操作',
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
              下载
            </Button>
            {canEdit && (
              <Popconfirm
                title="确认删除文件？"
                description="此操作将删除文件记录"
                onConfirm={() => handleDeleteFile(record.id, record.file_name)}
                okText="确认"
                cancelText="取消"
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
            添加文件记录
          </Button>
        )}
        <Text type="secondary">
          注：完整的文件上传功能正在开发中，当前仅支持添加文件记录
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
          description="暂无文件，点击「添加文件记录」开始"
          style={{ padding: '60px 0' }}
        />
      )}

      {/* 添加文件记录模态框 */}
      <Modal
        title="添加文件记录"
        open={addModalVisible}
        onOk={() => form.submit()}
        onCancel={() => {
          setAddModalVisible(false);
          form.resetFields();
        }}
        width={600}
        okText="添加"
        cancelText="取消"
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleAddFile}
        >
          <Form.Item
            label="文件名"
            name="file_name"
            rules={[{ required: true, message: '请输入文件名' }]}
          >
            <Input placeholder="例如：Q1财报分析.pdf" />
          </Form.Item>

          <Form.Item
            label="文件路径"
            name="file_path"
            tooltip="可选，文件的存储路径或URL"
          >
            <Input placeholder="文件路径或URL" />
          </Form.Item>

          <Form.Item
            label="文件类型"
            name="file_type"
          >
            <Input placeholder="例如：application/pdf" />
          </Form.Item>

          <Form.Item
            label="文件大小（字节）"
            name="file_size"
          >
            <Input type="number" placeholder="文件大小" />
          </Form.Item>

          <Form.Item
            label="分类"
            name="category"
          >
            <Select placeholder="选择文件分类">
              <Option value="meeting_notes">会议纪要</Option>
              <Option value="report">研究报告</Option>
              <Option value="contract">合同文档</Option>
              <Option value="presentation">演示文稿</Option>
              <Option value="other">其他</Option>
            </Select>
          </Form.Item>

          <Form.Item
            label="描述"
            name="description"
          >
            <TextArea rows={3} placeholder="文件描述或备注..." />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default ProjectFilesTab;


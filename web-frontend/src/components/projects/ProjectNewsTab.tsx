/**
 * Project News Tab Component
 * 项目新闻管理Tab - 新闻列表、添加、标注
 */

import React, { useState, useEffect } from 'react';
import {
  Table,
  Button,
  Space,
  Modal,
  Form,
  Input,
  message,
  Popconfirm,
  Rate,
  Switch,
  Tooltip,
  Empty,
  Typography,
  Alert
} from 'antd';
import {
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  StarFilled,
  FileTextOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';
import { ProjectNewsTracking, NewsTrackingCreateRequest, NewsTrackingUpdateRequest } from '../../types/project';
import { api } from '../../services/api';
import { useDashboardSettings } from '../../contexts/DashboardSettingsContext';
import { getModalStyles } from '../../utils/modalStyles';

const { TextArea } = Input;
const { Text } = Typography;

interface ProjectNewsTabProps {
  projectId: number;
  permissions: any;
}

// API服务（临时实现，后续需要添加到services/projects.ts）

const getProjectNews = async (projectId: number): Promise<ProjectNewsTracking[]> => {
  return await api.get(`/projects/${projectId}/news`);
};

const addNewsToProject = async (
  projectId: number,
  data: NewsTrackingCreateRequest
): Promise<any> => {
  return await api.post(`/projects/${projectId}/news`, data);
};

const updateProjectNews = async (
  projectId: number,
  newsId: number,
  data: NewsTrackingUpdateRequest
): Promise<any> => {
  return await api.put(`/projects/${projectId}/news/${newsId}`, data);
};

const removeProjectNews = async (
  projectId: number,
  newsId: number
): Promise<any> => {
  return await api.delete(`/projects/${projectId}/news/${newsId}`);
};

const ProjectNewsTab: React.FC<ProjectNewsTabProps> = ({ projectId, permissions }) => {
  const { themeMode } = useDashboardSettings();
  const [newsList, setNewsList] = useState<ProjectNewsTracking[]>([]);
  const [loading, setLoading] = useState(false);
  const [addModalVisible, setAddModalVisible] = useState(false);
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [currentNews, setCurrentNews] = useState<ProjectNewsTracking | null>(null);
  const [addForm] = Form.useForm();
  const [editForm] = Form.useForm();

  // 加载新闻列表
  const loadNews = async () => {
    setLoading(true);
    try {
      const data = await getProjectNews(projectId);
      setNewsList(data);
    } catch (error: any) {
      message.error(`Failed to load news list: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadNews();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectId]);

  // 添加新闻
  const handleAddNews = async (values: any) => {
    try {
      const newsData: NewsTrackingCreateRequest = {
        news_id: parseInt(values.news_id),
        relevance_score: values.relevance_score,
        is_important: values.is_important || false,
        user_notes: values.user_notes
      };

      await addNewsToProject(projectId, newsData);
      message.success('News added successfully');
      setAddModalVisible(false);
      addForm.resetFields();
      loadNews();
    } catch (error: any) {
      message.error(`Failed to add news: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 打开编辑模态框
  const handleEditNews = (news: ProjectNewsTracking) => {
    setCurrentNews(news);
    editForm.setFieldsValue({
      relevance_score: news.relevance_score,
      is_important: news.is_important,
      user_notes: news.user_notes
    });
    setEditModalVisible(true);
  };

  // 更新新闻标注
  const handleUpdateNews = async (values: any) => {
    if (!currentNews) return;

    try {
      const updateData: NewsTrackingUpdateRequest = {
        relevance_score: values.relevance_score,
        is_important: values.is_important,
        user_notes: values.user_notes
      };

      await updateProjectNews(projectId, currentNews.news_id, updateData);
      message.success('News annotation updated successfully');
      setEditModalVisible(false);
      loadNews();
    } catch (error: any) {
      message.error(`Update failed: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 移除新闻
  const handleRemoveNews = async (newsId: number, newsTitle: string) => {
    try {
      await removeProjectNews(projectId, newsId);
      message.success(`News removed: ${newsTitle}`);
      loadNews();
    } catch (error: any) {
      message.error(`Remove failed: ${error.response?.data?.detail || error.message}`);
    }
  };

  const canEdit = permissions?.permissions?.can_edit || false;

  // 表格列定义
  const columns: ColumnsType<ProjectNewsTracking> = [
    {
      title: 'News Title',
      dataIndex: 'news_title',
      key: 'news_title',
      width: 400,
      render: (text, record) => (
        <Space direction="vertical" size={0}>
          <Space>
            {record.is_important && (
              <StarFilled style={{ color: '#faad14' }} />
            )}
            <span style={{ fontWeight: record.is_important ? 'bold' : 'normal' }}>
              {text || `新闻 ${record.news_id}`}
            </span>
          </Space>
          {record.news_source && (
            <Text type="secondary" style={{ fontSize: '12px' }}>
              Source: {record.news_source}
            </Text>
          )}
        </Space>
      )
    },
    {
      title: 'Relevance',
      dataIndex: 'relevance_score',
      key: 'relevance_score',
      width: 150,
      render: (score: number) => (
        <Rate disabled value={score || 0} count={5} />
      )
    },
    {
      title: 'Notes',
      dataIndex: 'user_notes',
      key: 'user_notes',
      width: 250,
      ellipsis: true,
      render: (notes: string) => (
        <Tooltip title={notes}>
          <span style={{ color: notes ? '#000' : '#8c8c8c' }}>
            {notes || 'No notes'}
          </span>
        </Tooltip>
      )
    },
    {
      title: 'Date',
      dataIndex: 'news_date',
      key: 'news_date',
      width: 120,
      render: (date: string) => date ? dayjs(date).format('YYYY-MM-DD') : '-'
    },
    {
      title: 'Added At',
      dataIndex: 'added_at',
      key: 'added_at',
      width: 150,
      render: (date: string) => dayjs(date).format('YYYY-MM-DD HH:mm')
    },
    {
      title: 'Actions',
      key: 'action',
      width: 120,
      fixed: 'right',
      render: (_, record) => {
        if (!canEdit) {
          return <span>-</span>;
        }

        return (
          <Space>
            <Tooltip title="Edit Notes">
              <Button
                type="link"
                size="small"
                icon={<EditOutlined />}
                onClick={() => handleEditNews(record)}
              />
            </Tooltip>
            <Popconfirm
              title="Confirm Remove News?"
              description="This will remove the news from the event"
              onConfirm={() => handleRemoveNews(record.news_id, record.news_title || `News ${record.news_id}`)}
              okText="Confirm"
              cancelText="Cancel"
            >
              <Tooltip title="Remove News">
                <Button
                  type="link"
                  danger
                  size="small"
                  icon={<DeleteOutlined />}
                />
              </Tooltip>
            </Popconfirm>
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
            icon={<PlusOutlined />}
            onClick={() => setAddModalVisible(true)}
          >
            Add News
          </Button>
        )}
        <Button icon={<FileTextOutlined />} disabled>
          Auto Fetch (In Development)
        </Button>
      </Space>

      {/* 新闻列表 */}
      {newsList.length > 0 ? (
        <Table
          columns={columns}
          dataSource={newsList}
          rowKey="id"
          loading={loading}
          pagination={{ pageSize: 20 }}
          bordered
        />
      ) : (
        <Empty
          description="No news yet, click 'Add News' to start"
          style={{ padding: '60px 0' }}
        />
      )}

      {/* 添加新闻模态框 */}
      <Modal
        title="Add News to Event"
        open={addModalVisible}
        onOk={() => addForm.submit()}
        onCancel={() => {
          setAddModalVisible(false);
          addForm.resetFields();
        }}
        width={700}
        okText="Add"
        cancelText="Cancel"
        styles={getModalStyles(themeMode)}
      >
        <Form
          form={addForm}
          layout="vertical"
          onFinish={handleAddNews}
        >
          <Alert
            message="How to Get News ID"
            description={
              <div>
                <p>1. Go to the "News" page to view the latest news</p>
                <p>2. Find the news you want to associate in the news list</p>
                <p>3. Copy the news ID (usually shown in URL or details)</p>
                <p>4. Paste the news ID in the input box below</p>
              </div>
            }
            type="info"
            showIcon
            style={{ marginBottom: 16 }}
          />
          
          <Form.Item
            label="News ID"
            name="news_id"
            rules={[
              { required: true, message: 'Please enter news ID' },
              { 
                validator: async (_, value) => {
                  if (value && value <= 0) {
                    return Promise.reject(new Error('News ID must be greater than 0'));
                  }
                  return Promise.resolve();
                }
              }
            ]}
            extra="Note: Entering a non-existent news ID will cause the addition to fail"
          >
            <Input 
              type="number" 
              placeholder="e.g., 1234"
              addonBefore={<FileTextOutlined />}
            />
          </Form.Item>

          <Form.Item
            label="Relevance Score"
            name="relevance_score"
            tooltip="1-5 stars, indicating the relevance of the news to the event"
          >
            <Rate />
          </Form.Item>

          <Form.Item
            label="Important News"
            name="is_important"
            valuePropName="checked"
            initialValue={false}
          >
            <Switch checkedChildren="Yes" unCheckedChildren="No" />
          </Form.Item>

          <Form.Item
            label="Notes"
            name="user_notes"
          >
            <TextArea
              rows={4}
              placeholder="Record your thoughts, key points, or follow-up actions about this news..."
            />
          </Form.Item>
        </Form>
      </Modal>

      {/* 编辑标注模态框 */}
      <Modal
        title="Edit News Annotation"
        open={editModalVisible}
        onOk={() => editForm.submit()}
        onCancel={() => {
          setEditModalVisible(false);
          setCurrentNews(null);
        }}
        width={600}
        okText="Save"
        cancelText="Cancel"
        styles={getModalStyles(themeMode)}
      >
        <Form
          form={editForm}
          layout="vertical"
          onFinish={handleUpdateNews}
        >
          <Form.Item
            label="Relevance Score"
            name="relevance_score"
          >
            <Rate />
          </Form.Item>

          <Form.Item
            label="Important News"
            name="is_important"
            valuePropName="checked"
          >
            <Switch checkedChildren="Yes" unCheckedChildren="No" />
          </Form.Item>

          <Form.Item
            label="Notes"
            name="user_notes"
          >
            <TextArea
              rows={4}
              placeholder="Record your thoughts, key points, or follow-up actions about this news..."
            />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default ProjectNewsTab;


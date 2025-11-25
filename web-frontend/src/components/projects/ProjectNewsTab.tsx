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
      message.error(`加载新闻列表失败: ${error.response?.data?.detail || error.message}`);
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
      message.success('新闻添加成功');
      setAddModalVisible(false);
      addForm.resetFields();
      loadNews();
    } catch (error: any) {
      message.error(`添加新闻失败: ${error.response?.data?.detail || error.message}`);
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
      message.success('新闻标注更新成功');
      setEditModalVisible(false);
      loadNews();
    } catch (error: any) {
      message.error(`更新失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 移除新闻
  const handleRemoveNews = async (newsId: number, newsTitle: string) => {
    try {
      await removeProjectNews(projectId, newsId);
      message.success(`已移除新闻: ${newsTitle}`);
      loadNews();
    } catch (error: any) {
      message.error(`移除失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  const canEdit = permissions?.permissions?.can_edit || false;

  // 表格列定义
  const columns: ColumnsType<ProjectNewsTracking> = [
    {
      title: '新闻标题',
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
              来源: {record.news_source}
            </Text>
          )}
        </Space>
      )
    },
    {
      title: '相关性',
      dataIndex: 'relevance_score',
      key: 'relevance_score',
      width: 150,
      render: (score: number) => (
        <Rate disabled value={score || 0} count={5} />
      )
    },
    {
      title: '标注',
      dataIndex: 'user_notes',
      key: 'user_notes',
      width: 250,
      ellipsis: true,
      render: (notes: string) => (
        <Tooltip title={notes}>
          <span style={{ color: notes ? '#000' : '#8c8c8c' }}>
            {notes || '暂无标注'}
          </span>
        </Tooltip>
      )
    },
    {
      title: '日期',
      dataIndex: 'news_date',
      key: 'news_date',
      width: 120,
      render: (date: string) => date ? dayjs(date).format('YYYY-MM-DD') : '-'
    },
    {
      title: '添加时间',
      dataIndex: 'added_at',
      key: 'added_at',
      width: 150,
      render: (date: string) => dayjs(date).format('YYYY-MM-DD HH:mm')
    },
    {
      title: '操作',
      key: 'action',
      width: 120,
      fixed: 'right',
      render: (_, record) => {
        if (!canEdit) {
          return <span>-</span>;
        }

        return (
          <Space>
            <Tooltip title="编辑标注">
              <Button
                type="link"
                size="small"
                icon={<EditOutlined />}
                onClick={() => handleEditNews(record)}
              />
            </Tooltip>
            <Popconfirm
              title="确认移除新闻？"
              description="此操作将从项目中移除该新闻"
              onConfirm={() => handleRemoveNews(record.news_id, record.news_title || `新闻${record.news_id}`)}
              okText="确认"
              cancelText="取消"
            >
              <Tooltip title="移除新闻">
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
            添加新闻
          </Button>
        )}
        <Button icon={<FileTextOutlined />} disabled>
          自动抓取（开发中）
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
          description="暂无新闻，点击「添加新闻」开始添加"
          style={{ padding: '60px 0' }}
        />
      )}

      {/* 添加新闻模态框 */}
      <Modal
        title="添加新闻到项目"
        open={addModalVisible}
        onOk={() => addForm.submit()}
        onCancel={() => {
          setAddModalVisible(false);
          addForm.resetFields();
        }}
        width={700}
        okText="添加"
        cancelText="取消"
      >
        <Form
          form={addForm}
          layout="vertical"
          onFinish={handleAddNews}
        >
          <Alert
            message="如何获取新闻ID"
            description={
              <div>
                <p>1. 前往「新闻」页面查看最新新闻</p>
                <p>2. 在新闻列表中找到要关联的新闻</p>
                <p>3. 复制新闻的ID（通常在URL或详情中显示）</p>
                <p>4. 在下方输入框中粘贴新闻ID</p>
              </div>
            }
            type="info"
            showIcon
            style={{ marginBottom: 16 }}
          />
          
          <Form.Item
            label="新闻ID"
            name="news_id"
            rules={[
              { required: true, message: '请输入新闻ID' },
              { 
                validator: async (_, value) => {
                  if (value && value <= 0) {
                    return Promise.reject(new Error('新闻ID必须大于0'));
                  }
                  return Promise.resolve();
                }
              }
            ]}
            extra="提示：输入不存在的新闻ID会导致添加失败"
          >
            <Input 
              type="number" 
              placeholder="例如：1234"
              addonBefore={<FileTextOutlined />}
            />
          </Form.Item>

          <Form.Item
            label="相关性评分"
            name="relevance_score"
            tooltip="1-5星，表示新闻与项目的相关程度"
          >
            <Rate />
          </Form.Item>

          <Form.Item
            label="重要新闻"
            name="is_important"
            valuePropName="checked"
            initialValue={false}
          >
            <Switch checkedChildren="是" unCheckedChildren="否" />
          </Form.Item>

          <Form.Item
            label="批注"
            name="user_notes"
          >
            <TextArea
              rows={4}
              placeholder="记录关于这条新闻的想法、要点或后续行动..."
            />
          </Form.Item>
        </Form>
      </Modal>

      {/* 编辑标注模态框 */}
      <Modal
        title="编辑新闻标注"
        open={editModalVisible}
        onOk={() => editForm.submit()}
        onCancel={() => {
          setEditModalVisible(false);
          setCurrentNews(null);
        }}
        width={600}
        okText="保存"
        cancelText="取消"
      >
        <Form
          form={editForm}
          layout="vertical"
          onFinish={handleUpdateNews}
        >
          <Form.Item
            label="相关性评分"
            name="relevance_score"
          >
            <Rate />
          </Form.Item>

          <Form.Item
            label="重要新闻"
            name="is_important"
            valuePropName="checked"
          >
            <Switch checkedChildren="是" unCheckedChildren="否" />
          </Form.Item>

          <Form.Item
            label="批注"
            name="user_notes"
          >
            <TextArea
              rows={4}
              placeholder="记录关于这条新闻的想法、要点或后续行动..."
            />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default ProjectNewsTab;


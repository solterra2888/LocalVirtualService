/**
 * Investment Projects Management Page
 * 投资项目管理页面
 */

import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Card,
  Table,
  Button,
  Space,
  Tag,
  Typography,
  Row,
  Col,
  message,
  Modal,
  Input,
  Select,
  DatePicker,
  Form,
  Tooltip,
  Badge,
  Dropdown,
  Menu
} from 'antd';
import {
  PlusOutlined,
  SearchOutlined,
  ReloadOutlined,
  FolderOutlined,
  TeamOutlined,
  FileTextOutlined,
  MessageOutlined,
  MoreOutlined,
  EditOutlined,
  DeleteOutlined,
  EyeOutlined,
  FundProjectionScreenOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';

import {
  InvestmentProject,
  ProjectType,
  ProjectStatus,
  ProjectPriority,
  ProjectCreateRequest,
  ProjectListQuery,
  PROJECT_STATUS_OPTIONS,
  PROJECT_TYPE_OPTIONS,
  PROJECT_PRIORITY_OPTIONS
} from '../types/project';
import {
  listProjects,
  createProject,
  deleteProject,
  getProjectIdentifier
} from '../services/projects';

const { Title, Text } = Typography;
const { Option } = Select;
const { TextArea } = Input;

const ProjectsPage: React.FC = () => {
  const navigate = useNavigate();
  const [projects, setProjects] = useState<InvestmentProject[]>([]);
  const [loading, setLoading] = useState(false);
  const [total, setTotal] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  
  // 筛选条件
  const [filters, setFilters] = useState<ProjectListQuery>({});
  const [searchText, setSearchText] = useState('');
  
  // 模态框状态
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [form] = Form.useForm();

  // 加载项目列表
  const loadProjects = async () => {
    setLoading(true);
    try {
      const query: ProjectListQuery = {
        ...filters,
        page: currentPage,
        page_size: pageSize,
        search: searchText || undefined
      };
      
      const response = await listProjects(query);
      setProjects(response.items);
      setTotal(response.total);
    } catch (error: any) {
      message.error(`加载项目列表失败: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
    }
  };

  // 初始加载
  useEffect(() => {
    loadProjects();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentPage, pageSize, filters]);

  // 创建项目
  const handleCreateProject = async (values: any) => {
    try {
      const projectData: ProjectCreateRequest = {
        project_name: values.project_name,
        project_type: values.project_type,
        ticker_symbol: values.ticker_symbol,
        industry: values.industry,
        sector: values.sector,
        region: values.region,
        priority: values.priority || ProjectPriority.MEDIUM,
        description: values.description,
        investment_thesis: values.investment_thesis,
        start_date: values.start_date ? values.start_date.format('YYYY-MM-DD') : undefined
      };
      
      await createProject(projectData);
      message.success('项目创建成功');
      setCreateModalVisible(false);
      form.resetFields();
      loadProjects();
    } catch (error: any) {
      message.error(`创建项目失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 删除项目
  const handleDeleteProject = async (projectId: number, projectName: string) => {
    Modal.confirm({
      title: '确认归档项目',
      content: `确定要归档项目"${projectName}"吗？归档后项目将不再显示在列表中。`,
      okText: '确认',
      okType: 'danger',
      cancelText: '取消',
      onOk: async () => {
        try {
          await deleteProject(projectId);
          message.success('项目已归档');
          loadProjects();
        } catch (error: any) {
          message.error(`归档项目失败: ${error.response?.data?.detail || error.message}`);
        }
      }
    });
  };

  // 表格列定义
  const columns: ColumnsType<InvestmentProject> = [
    {
      title: '项目名称',
      dataIndex: 'project_name',
      key: 'project_name',
      width: 250,
      fixed: 'left',
      render: (text, record) => (
        <Space direction="vertical" size={0}>
          <Text strong style={{ fontSize: '14px' }}>
            <FolderOutlined style={{ marginRight: 8 }} />
            {text}
          </Text>
          <Text type="secondary" style={{ fontSize: '12px' }}>
            {record.project_code}
          </Text>
        </Space>
      )
    },
    {
      title: '类型',
      dataIndex: 'project_type',
      key: 'project_type',
      width: 100,
      render: (type: ProjectType) => {
        const option = PROJECT_TYPE_OPTIONS.find(opt => opt.value === type);
        return (
          <Tag color="blue">
            {option?.icon} {option?.label || type}
          </Tag>
        );
      }
    },
    {
      title: '标的/行业',
      key: 'identifier',
      width: 150,
      render: (_, record) => {
        const identifier = getProjectIdentifier(record);
        return (
          <Tag color="geekblue" icon={<FundProjectionScreenOutlined />}>
            {identifier}
          </Tag>
        );
      }
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: ProjectStatus) => {
        const option = PROJECT_STATUS_OPTIONS.find(opt => opt.value === status);
        const colorMap: Record<string, string> = {
          active: 'success',
          paused: 'warning',
          completed: 'default',
          archived: 'error'
        };
        return <Tag color={colorMap[status]}>{option?.label || status}</Tag>;
      }
    },
    {
      title: '优先级',
      dataIndex: 'priority',
      key: 'priority',
      width: 80,
      render: (priority: ProjectPriority) => {
        const option = PROJECT_PRIORITY_OPTIONS.find(opt => opt.value === priority);
        return <Tag color={option?.color}>{option?.label || priority}</Tag>;
      }
    },
    {
      title: '统计',
      key: 'stats',
      width: 200,
      render: (_, record) => (
        <Space size="middle">
          <Tooltip title="成员数量">
            <Badge count={record.member_count || 0} showZero>
              <TeamOutlined style={{ fontSize: 16 }} />
            </Badge>
          </Tooltip>
          <Tooltip title="新闻数量">
            <Badge count={record.news_count || 0} showZero>
              <FileTextOutlined style={{ fontSize: 16 }} />
            </Badge>
          </Tooltip>
          <Tooltip title="里程碑数量">
            <Badge count={record.milestone_count || 0} showZero>
              <MessageOutlined style={{ fontSize: 16 }} />
            </Badge>
          </Tooltip>
        </Space>
      )
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 150,
      render: (date: string) => dayjs(date).format('YYYY-MM-DD HH:mm')
    },
    {
      title: '操作',
      key: 'action',
      width: 120,
      fixed: 'right',
      render: (_, record) => (
        <Space>
          <Tooltip title="查看详情">
            <Button
              type="link"
              icon={<EyeOutlined />}
              onClick={() => handleViewProject(record.id)}
            />
          </Tooltip>
          <Tooltip title="编辑">
            <Button
              type="link"
              icon={<EditOutlined />}
              onClick={() => handleEditProject(record.id)}
            />
          </Tooltip>
          <Dropdown
            overlay={
              <Menu>
                <Menu.Item
                  key="delete"
                  danger
                  icon={<DeleteOutlined />}
                  onClick={() => handleDeleteProject(record.id, record.project_name)}
                >
                  归档项目
                </Menu.Item>
              </Menu>
            }
          >
            <Button type="link" icon={<MoreOutlined />} />
          </Dropdown>
        </Space>
      )
    }
  ];

  // 查看项目详情
  const handleViewProject = (projectId: number) => {
    navigate(`/projects/${projectId}`);
  };

  // 编辑项目
  const handleEditProject = (projectId: number) => {
    navigate(`/projects/${projectId}?tab=settings`);
  };

  return (
    <div style={{ padding: '24px' }}>
      {/* 页面标题 */}
      <Row justify="space-between" align="middle" style={{ marginBottom: 24 }}>
        <Col>
          <Title level={2}>
            <FolderOutlined style={{ marginRight: 12 }} />
            投资项目管理
          </Title>
        </Col>
        <Col>
          <Space>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setCreateModalVisible(true)}
            >
              创建项目
            </Button>
            <Button icon={<ReloadOutlined />} onClick={loadProjects}>
              刷新
            </Button>
          </Space>
        </Col>
      </Row>

      {/* 筛选条件 */}
      <Card style={{ marginBottom: 16 }}>
        <Row gutter={[16, 16]}>
          <Col span={6}>
            <Input
              placeholder="搜索项目名称或代码"
              prefix={<SearchOutlined />}
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              onPressEnter={loadProjects}
            />
          </Col>
          <Col span={4}>
            <Select
              style={{ width: '100%' }}
              placeholder="项目状态"
              allowClear
              onChange={(value) => setFilters({ ...filters, status: value })}
            >
              {PROJECT_STATUS_OPTIONS.map(opt => (
                <Option key={opt.value} value={opt.value}>
                  {opt.label}
                </Option>
              ))}
            </Select>
          </Col>
          <Col span={4}>
            <Select
              style={{ width: '100%' }}
              placeholder="项目类型"
              allowClear
              onChange={(value) => setFilters({ ...filters, project_type: value })}
            >
              {PROJECT_TYPE_OPTIONS.map(opt => (
                <Option key={opt.value} value={opt.value}>
                  {opt.icon} {opt.label}
                </Option>
              ))}
            </Select>
          </Col>
          <Col span={4}>
            <Select
              style={{ width: '100%' }}
              placeholder="优先级"
              allowClear
              onChange={(value) => setFilters({ ...filters, priority: value })}
            >
              {PROJECT_PRIORITY_OPTIONS.map(opt => (
                <Option key={opt.value} value={opt.value}>
                  {opt.label}
                </Option>
              ))}
            </Select>
          </Col>
          <Col span={6}>
            <Space>
              <Button type="primary" icon={<SearchOutlined />} onClick={loadProjects}>
                搜索
              </Button>
              <Button
                onClick={() => {
                  setSearchText('');
                  setFilters({});
                  setCurrentPage(1);
                }}
              >
                重置
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* 项目列表表格 */}
      <Card>
        <Table
          columns={columns}
          dataSource={projects}
          rowKey="id"
          loading={loading}
          scroll={{ x: 1400 }}
          pagination={{
            current: currentPage,
            pageSize: pageSize,
            total: total,
            showSizeChanger: true,
            showTotal: (total) => `共 ${total} 个项目`,
            onChange: (page, size) => {
              setCurrentPage(page);
              setPageSize(size);
            }
          }}
        />
      </Card>

      {/* 创建项目模态框 */}
      <Modal
        title="创建投资项目"
        open={createModalVisible}
        onOk={() => form.submit()}
        onCancel={() => {
          setCreateModalVisible(false);
          form.resetFields();
        }}
        width={700}
        okText="创建"
        cancelText="取消"
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleCreateProject}
        >
          <Form.Item
            label="项目名称"
            name="project_name"
            rules={[{ required: true, message: '请输入项目名称' }]}
          >
            <Input placeholder="例如：特斯拉深度研究" />
          </Form.Item>

          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label="项目类型"
                name="project_type"
                rules={[{ required: true, message: '请选择项目类型' }]}
              >
                <Select placeholder="选择项目类型">
                  {PROJECT_TYPE_OPTIONS.map(opt => (
                    <Option key={opt.value} value={opt.value}>
                      {opt.icon} {opt.label}
                    </Option>
                  ))}
                </Select>
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="优先级" name="priority">
                <Select placeholder="选择优先级">
                  {PROJECT_PRIORITY_OPTIONS.map(opt => (
                    <Option key={opt.value} value={opt.value}>
                      {opt.label}
                    </Option>
                  ))}
                </Select>
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={16}>
            <Col span={8}>
              <Form.Item label="股票代码" name="ticker_symbol">
                <Input placeholder="例如：TSLA.US" />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="行业" name="industry">
                <Input placeholder="例如：电动汽车" />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="区域" name="region">
                <Select placeholder="选择区域">
                  <Option value="US">美股</Option>
                  <Option value="HK">港股</Option>
                  <Option value="CN">A股</Option>
                </Select>
              </Form.Item>
            </Col>
          </Row>

          <Form.Item label="项目描述" name="description">
            <TextArea rows={3} placeholder="简要描述项目目标和范围..." />
          </Form.Item>

          <Form.Item label="投资论点" name="investment_thesis">
            <TextArea rows={4} placeholder="详细说明投资逻辑和理由..." />
          </Form.Item>

          <Form.Item label="开始日期" name="start_date">
            <DatePicker style={{ width: '100%' }} />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default ProjectsPage;


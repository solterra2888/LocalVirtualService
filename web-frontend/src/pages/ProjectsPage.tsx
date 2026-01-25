/**
 * Investment Projects Management Page
 * 投资项目管理页面
 */

import React, { useState, useEffect } from 'react';
import {
  Button,
  Space,
  Typography,
  Row,
  Col,
  message,
  Modal,
  Input,
  Select,
  DatePicker,
  Form,
  Layout,
  Spin,
  Empty,
  Tabs
} from 'antd';
import {
  PlusOutlined,
  SearchOutlined,
  ReloadOutlined,
  FolderOutlined,
  FlagOutlined,
  PaperClipOutlined,
  TeamOutlined,
  SettingOutlined
} from '@ant-design/icons';

import {
  InvestmentProject,
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
  getProject,
  getMyPermissions
} from '../services/projects';
import ProjectCard from '../components/projects/ProjectCard';
import ProjectMilestonesTab from '../components/projects/ProjectMilestonesTab';
import ProjectFilesTab from '../components/projects/ProjectFilesTab';
import ProjectMembersTab from '../components/projects/ProjectMembersTab';
import ProjectSettingsTab from '../components/projects/ProjectSettingsTab';

const { Title, Text } = Typography;
const { Option } = Select;
const { TextArea } = Input;
const { Sider, Content } = Layout;
const { TabPane } = Tabs;

const ProjectsPage: React.FC = () => {
  const [projects, setProjects] = useState<InvestmentProject[]>([]);
  const [loading, setLoading] = useState(false);
  const [total, setTotal] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const pageSize = 100; // 固定每页显示100个项目
  
  // 筛选条件
  const [filters, setFilters] = useState<ProjectListQuery>({});
  const [searchText, setSearchText] = useState('');
  
  // 选中的项目
  const [selectedProject, setSelectedProject] = useState<InvestmentProject | null>(null);
  const [selectedProjectDetail, setSelectedProjectDetail] = useState<InvestmentProject | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [permissions, setPermissions] = useState<any>(null);
  const [activeTab, setActiveTab] = useState('milestones');
  
  // 模态框状态
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [form] = Form.useForm();

  // 加载项目列表
  const loadProjects = async (autoSelectFirst: boolean = false) => {
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
      
      // 自动选中第一个项目
      if (autoSelectFirst && response.items.length > 0) {
        const firstProject = response.items[0];
        setSelectedProject(firstProject);
        loadProjectDetail(firstProject.id);
      }
    } catch (error: any) {
      message.error(`Failed to load event list: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
    }
  };

  // 初始加载
  useEffect(() => {
    // 首次加载时自动选中第一个项目
    const isInitialLoad = currentPage === 1 && Object.keys(filters).length === 0 && !searchText;
    loadProjects(isInitialLoad);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentPage, pageSize, filters]);

  // 加载项目详情
  const loadProjectDetail = async (projectId: number) => {
    setDetailLoading(true);
    try {
      const data = await getProject(projectId, true);
      setSelectedProjectDetail(data);
      
      // 加载权限
      const perms = await getMyPermissions(projectId);
      setPermissions(perms);
    } catch (error: any) {
      message.error(`Failed to load event details: ${error.response?.data?.detail || error.message}`);
    } finally {
      setDetailLoading(false);
    }
  };

  // 选择项目
  const handleSelectProject = (project: InvestmentProject) => {
    setSelectedProject(project);
    loadProjectDetail(project.id);
  };

  // 创建项目
  const handleCreateProject = async (values: any) => {
    try {
      const projectData: ProjectCreateRequest = {
        project_name: values.project_name,
        project_type: values.project_type,
        tickers: values.tickers || [], // 支持多个ticker
        industry: values.industry,
        sector: values.sector,
        region: values.region,
        priority: values.priority || ProjectPriority.MEDIUM,
        description: values.description,
        start_date: values.start_date ? values.start_date.format('YYYY-MM-DD') : undefined
      };
      
      const newProject = await createProject(projectData);
      message.success('Event created successfully');
      setCreateModalVisible(false);
      form.resetFields();
      await loadProjects();
      // 自动选中新创建的项目
      if (newProject && newProject.id) {
        const foundProject = projects.find(p => p.id === newProject.id);
        if (foundProject) {
          handleSelectProject(foundProject);
        }
      }
    } catch (error: any) {
      message.error(`Failed to create event: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 刷新项目详情
  const handleRefreshDetail = () => {
    if (selectedProject) {
      loadProjectDetail(selectedProject.id);
      loadProjects(false); // 同时刷新列表以更新统计数据
    }
  };

  // 渲染右侧项目详情面板
  const renderProjectDetail = () => {
    if (detailLoading) {
      return (
        <div style={{ 
          display: 'flex', 
          justifyContent: 'center', 
          alignItems: 'center', 
          height: '50%' 
        }}>
          <Spin size="large" tip="Loading event details..." />
        </div>
      );
    }

    if (!selectedProjectDetail) {
      return (
        <div style={{ 
          display: 'flex', 
          justifyContent: 'center', 
          alignItems: 'center', 
          height: '50%',
          flexDirection: 'column'
        }}>
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description={
              <span>
                Please select an event from the left<br />
                to view details
              </span>
            }
          />
        </div>
      );
    }

    const project = selectedProjectDetail;

    return (
      <div style={{ height: '100%' }}>
        {/* Tab内容区域 */}
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          size="large"
        >
          {/* 记录 Tab - 第一位 */}
          <TabPane
            tab={
              <span>
                <FlagOutlined />
                记录 ({project.milestone_count || 0})
              </span>
            }
            key="milestones"
          >
            <ProjectMilestonesTab 
              projectId={project.id}
              permissions={permissions}
            />
          </TabPane>

          {/* Files Tab - 第二位 */}
          <TabPane
            tab={
              <span>
                <PaperClipOutlined />
                Files ({project.file_count || 0})
              </span>
            }
            key="files"
          >
            <ProjectFilesTab 
              projectId={project.id}
              permissions={permissions}
            />
          </TabPane>

          {/* Members Tab - 第三位 */}
          <TabPane
            tab={
              <span>
                <TeamOutlined />
                Members ({project.member_count || 0})
              </span>
            }
            key="members"
          >
            <ProjectMembersTab 
              projectId={project.id}
              project={project}
              permissions={permissions}
              onUpdate={handleRefreshDetail}
            />
          </TabPane>

          {/* Settings Tab - 最后 */}
          <TabPane
            tab={
              <span>
                <SettingOutlined />
                Settings
              </span>
            }
            key="settings"
          >
            <ProjectSettingsTab 
              project={project}
              permissions={permissions}
              onUpdate={handleRefreshDetail}
            />
          </TabPane>
        </Tabs>
      </div>
    );
  };

  return (
    <div style={{ padding: '24px', height: 'calc(100vh - 64px)' }}>
      {/* 页面标题 */}
      <Row justify="space-between" align="middle" style={{ marginBottom: 16 }}>
        <Col>
          <Title level={2} style={{ margin: 0 }}>
            <FolderOutlined style={{ marginRight: 12 }} />
            Investment Tracking
          </Title>
          <Text type="secondary" style={{ display: 'block', marginTop: 8 }}>
            Currently under design and development. Only partial features are available for preview. 目前正在设计和开发中......
          </Text>
        </Col>
        <Col>
          <Space>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setCreateModalVisible(true)}
            >
              Create Event
            </Button>
            <Button 
              icon={<ReloadOutlined />} 
              onClick={() => {
                loadProjects(false);
                if (selectedProject) {
                  handleRefreshDetail();
                }
              }}
            >
              Refresh
            </Button>
          </Space>
        </Col>
      </Row>

      {/* 左右布局 */}
      <Layout style={{ background: '#fff', height: 'calc(100% - 60px)' }}>
        {/* 左侧面板 - 事件卡片列表 */}
        <Sider
          width={350}
          style={{
            background: '#fafafa',
            borderRight: '1px solid #f0f0f0',
            height: '100%',
            overflow: 'hidden',
            display: 'flex',
            flexDirection: 'column'
          }}
        >
          <div style={{ 
            padding: '16px',
            borderBottom: '1px solid #f0f0f0',
            background: '#fff'
          }}>
            <Space direction="vertical" size={12} style={{ width: '100%' }}>
              {/* 搜索框 */}
              <Input
                placeholder="Search event name or code"
                prefix={<SearchOutlined />}
                value={searchText}
                onChange={(e) => setSearchText(e.target.value)}
                onPressEnter={() => loadProjects(false)}
                allowClear
              />
              
              {/* 筛选条件 */}
              <Row gutter={8}>
                <Col span={12}>
                  <Select
                    style={{ width: '100%' }}
                    placeholder="Status"
                    allowClear
                    size="small"
                    onChange={(value) => setFilters({ ...filters, status: value })}
                  >
                    {PROJECT_STATUS_OPTIONS.map(opt => (
                      <Option key={opt.value} value={opt.value}>
                        {opt.label}
                      </Option>
                    ))}
                  </Select>
                </Col>
                <Col span={12}>
                  <Select
                    style={{ width: '100%' }}
                    placeholder="Priority"
                    allowClear
                    size="small"
                    onChange={(value) => setFilters({ ...filters, priority: value })}
                  >
                    {PROJECT_PRIORITY_OPTIONS.map(opt => (
                      <Option key={opt.value} value={opt.value}>
                        {opt.label}
                      </Option>
                    ))}
                  </Select>
                </Col>
              </Row>

              {/* 搜索和重置按钮 */}
              <Row gutter={8}>
                <Col span={12}>
                  <Button 
                    type="primary" 
                    icon={<SearchOutlined />} 
                    onClick={() => loadProjects(false)}
                    size="small"
                    block
                  >
                    Search
                  </Button>
                </Col>
                <Col span={12}>
                  <Button
                    onClick={() => {
                      setSearchText('');
                      setFilters({});
                      setCurrentPage(1);
                    }}
                    size="small"
                    block
                  >
                    Reset
                  </Button>
                </Col>
              </Row>

              {/* 显示总数 */}
              <Text type="secondary" style={{ fontSize: '12px' }}>
                Total: {total} events
              </Text>
            </Space>
          </div>
          
          {/* 事件卡片列表 */}
          <div style={{ 
            flex: 1, 
            overflowY: 'auto', 
            padding: '16px',
            display: 'flex',
            flexDirection: 'column',
            gap: '8px'
          }}>
            <Spin spinning={loading}>
              {projects.length === 0 ? (
                <Empty 
                  description="No events found"
                  image={Empty.PRESENTED_IMAGE_SIMPLE}
                  style={{ marginTop: '40px' }}
                >
                  <Button 
                    type="primary" 
                    icon={<PlusOutlined />}
                    onClick={() => setCreateModalVisible(true)}
                  >
                    Create First Event
                  </Button>
                </Empty>
              ) : (
                projects.map(project => (
                  <ProjectCard
                    key={project.id}
                    project={project}
                    isSelected={selectedProject?.id === project.id}
                    onClick={handleSelectProject}
                  />
                ))
              )}
            </Spin>
          </div>
        </Sider>

        {/* 右侧内容区域 - 事件详情 */}
        <Content style={{ 
          background: '#fff', 
          height: '100%', 
          overflowY: 'auto',
          padding: '24px'
        }}>
          {renderProjectDetail()}
        </Content>
      </Layout>

      {/* 创建项目模态框 */}
      <Modal
        title="Create Event"
        open={createModalVisible}
        onOk={() => form.submit()}
        onCancel={() => {
          setCreateModalVisible(false);
          form.resetFields();
        }}
        width={700}
        okText="Create"
        cancelText="Cancel"
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleCreateProject}
        >
          <Form.Item
            label="Event Name"
            name="project_name"
            rules={[{ required: true, message: 'Please enter event name' }]}
          >
            <Input placeholder="e.g., Tesla Deep Research" />
          </Form.Item>

          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label="Event Type"
                name="project_type"
                rules={[{ required: true, message: 'Please select event type' }]}
              >
                <Select placeholder="Select event type">
                  {PROJECT_TYPE_OPTIONS.map(opt => (
                    <Option key={opt.value} value={opt.value}>
                      {opt.icon} {opt.label}
                    </Option>
                  ))}
                </Select>
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="Priority" name="priority">
                <Select placeholder="Select priority">
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
            <Col span={16}>
              <Form.Item 
                label="Tickers" 
                name="tickers"
                tooltip="You can add multiple tickers, press Enter after each one"
              >
                <Select
                  mode="tags"
                  placeholder="e.g., TSLA.US, AAPL.US, 700.HK (Press Enter to add)"
                  tokenSeparators={[',', ' ']}
                  style={{ width: '100%' }}
                />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="Region" name="region">
                <Select placeholder="Select region">
                  <Option value="US">US Market</Option>
                  <Option value="HK">HK Market</Option>
                  <Option value="CN">A-Share Market</Option>
                </Select>
              </Form.Item>
            </Col>
          </Row>
          
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item label="Industry" name="industry">
                <Input placeholder="e.g., Electric Vehicles" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="Sector" name="sector">
                <Input placeholder="e.g., Consumer Cyclical" />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item label="Description" name="description">
            <TextArea rows={3} placeholder="Brief description of event goals and scope..." />
          </Form.Item>

          <Form.Item label="Start Date" name="start_date">
            <DatePicker style={{ width: '100%' }} />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default ProjectsPage;


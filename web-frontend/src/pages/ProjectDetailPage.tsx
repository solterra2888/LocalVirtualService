/**
 * Investment Project Detail Page
 * 投资项目详情页面 - 包含多个Tab（里程碑、新闻、文件、成员、设置）
 */

import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Card,
  Tabs,
  Typography,
  Row,
  Col,
  Button,
  Space,
  Tag,
  Breadcrumb,
  Spin,
  message,
  Statistic,
  Alert,
  Descriptions,
  Divider,
  ConfigProvider,
  theme as antdTheme
} from 'antd';
import {
  ArrowLeftOutlined,
  FolderOutlined,
  FileTextOutlined,
  FlagOutlined,
  PaperClipOutlined,
  TeamOutlined,
  SettingOutlined,
  ReloadOutlined,
  DownOutlined,
  UpOutlined,
  CalendarOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';

import { InvestmentProject } from '../types/project';
import { getProject, getMyPermissions } from '../services/projects';
import { PROJECT_STATUS_OPTIONS, PROJECT_TYPE_OPTIONS, PROJECT_PRIORITY_OPTIONS } from '../types/project';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

// Tab组件
import ProjectMilestonesTab from '../components/projects/ProjectMilestonesTab';
import ProjectNewsTab from '../components/projects/ProjectNewsTab';
import ProjectFilesTab from '../components/projects/ProjectFilesTab';
import ProjectMembersTab from '../components/projects/ProjectMembersTab';
import ProjectSettingsTab from '../components/projects/ProjectSettingsTab';

const { Title, Text, Paragraph } = Typography;
const { TabPane } = Tabs;

const THEME_COLORS = {
  light: {
    pageBg: '#f0f2f5',
    cardBg: '#fff',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    descriptionBg: '#fafafa',
    descriptionText: '#595959',
  },
  dark: {
    pageBg: '#1a202c',
    cardBg: '#1a2234',
    text: '#e0e6f1',
    textSecondary: 'rgba(255, 255, 255, 0.5)',
    descriptionBg: 'rgba(255, 255, 255, 0.04)',
    descriptionText: 'rgba(255, 255, 255, 0.65)',
  },
} as const;

const ProjectDetailPage: React.FC = () => {
  const { projectId } = useParams<{ projectId: string }>();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const navigate = useNavigate();
  
  const [project, setProject] = useState<InvestmentProject | null>(null);
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState('milestones');
  const [permissions, setPermissions] = useState<any>(null);
  const [detailsExpanded, setDetailsExpanded] = useState(false);

  // 加载项目详情
  const loadProject = async () => {
    if (!projectId) return;
    
    setLoading(true);
    try {
      const data = await getProject(parseInt(projectId), true);
      setProject(data);
      
      // 加载权限
      const perms = await getMyPermissions(parseInt(projectId));
      setPermissions(perms);
    } catch (error: any) {
      message.error(`Failed to load event: ${error.response?.data?.detail || error.message}`);
      // 如果加载失败，返回列表页
      setTimeout(() => navigate('/projects'), 1500);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadProject();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectId]);

  // 返回列表页
  const handleBack = () => {
    navigate('/projects');
  };

  // 刷新项目信息
  const handleRefresh = () => {
    loadProject();
  };

  if (loading) {
    return (
      <ConfigProvider theme={{ algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm }}>
        <div style={{ padding: '24px', textAlign: 'center', backgroundColor: colors.pageBg, minHeight: '100vh' }}>
          <Spin size="large" tip="Loading event information..." />
        </div>
      </ConfigProvider>
    );
  }

  if (!project) {
    return (
      <ConfigProvider theme={{ algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm }}>
        <div style={{ padding: '24px', backgroundColor: colors.pageBg, minHeight: '100vh' }}>
          <Alert
            message="Event Not Found"
            description="This event may have been deleted or you do not have access permission"
            type="warning"
            showIcon
            action={
              <Button type="primary" onClick={handleBack}>
                Back to List
              </Button>
            }
          />
        </div>
      </ConfigProvider>
    );
  }

  // 获取状态、类型、优先级的显示信息
  const statusOption = PROJECT_STATUS_OPTIONS.find(opt => opt.value === project.status);
  const typeOption = PROJECT_TYPE_OPTIONS.find(opt => opt.value === project.project_type);
  const priorityOption = PROJECT_PRIORITY_OPTIONS.find(opt => opt.value === project.priority);

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: colors.cardBg,
            colorBorderSecondary: '#364152',
          },
        }),
      }}
    >
    <div style={{ padding: '24px', backgroundColor: colors.pageBg, minHeight: '100vh' }}>
      {/* 面包屑导航 */}
      <Breadcrumb style={{ marginBottom: 16 }}>
        <Breadcrumb.Item>
          <Button type="link" onClick={handleBack} style={{ padding: 0 }}>
            Investment Tracking
          </Button>
        </Breadcrumb.Item>
        <Breadcrumb.Item>{project.project_name}</Breadcrumb.Item>
      </Breadcrumb>

      {/* 项目头部信息 */}
      <Card style={{ marginBottom: 16 }}>
        {/* 基本信息行 */}
        <Row gutter={[16, 16]} align="middle">
          <Col xs={24} lg={12} xl={14}>
            <Space direction="vertical" size={8} style={{ width: '100%' }}>
              <Space wrap>
                <Button icon={<ArrowLeftOutlined />} onClick={handleBack}>
                  Back
                </Button>
                <Title level={3} style={{ margin: 0 }}>
                  <FolderOutlined style={{ marginRight: 8 }} />
                  {project.project_name}
                </Title>
              </Space>
              
              <Space size="middle" wrap>
                <Tag color={typeOption?.icon}>
                  {typeOption?.icon} {typeOption?.label}
                </Tag>
                <Tag color={statusOption?.color}>
                  {statusOption?.label}
                </Tag>
                <Tag color={priorityOption?.color}>
                  Priority: {priorityOption?.label}
                </Tag>
                {project.tickers && project.tickers.length > 0 && (
                  <>
                    {project.tickers.map((ticker, index) => (
                      <Tag key={index} color="blue">{ticker}</Tag>
                    ))}
                  </>
                )}
                {project.industry && (
                  <Tag color="cyan">{project.industry}</Tag>
                )}
                {project.sector && (
                  <Tag color="purple">{project.sector}</Tag>
                )}
                {project.region && (
                  <Tag>{project.region}</Tag>
                )}
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  Event Code: {project.project_code}
                </Text>
              </Space>
            </Space>
          </Col>

          {/* 统计信息 */}
          <Col xs={24} sm={12} lg={6} xl={6}>
            <Row gutter={[16, 16]}>
              <Col span={8} sm={24} lg={8}>
                <Statistic
                  title="Members"
                  value={project.member_count || 0}
                  prefix={<TeamOutlined />}
                />
              </Col>
              <Col span={8} sm={24} lg={8}>
                <Statistic
                  title="记录"
                  value={project.milestone_count || 0}
                  prefix={<FlagOutlined />}
                />
              </Col>
              <Col span={8} sm={24} lg={8}>
                <Statistic
                  title="Files"
                  value={project.file_count || 0}
                  prefix={<PaperClipOutlined />}
                />
              </Col>
            </Row>
          </Col>

          {/* 操作按钮 */}
          <Col xs={24} sm={12} lg={6} xl={4}>
            <Space wrap style={{ width: '100%', justifyContent: 'flex-end' }}>
              <Button 
                icon={detailsExpanded ? <UpOutlined /> : <DownOutlined />}
                onClick={() => setDetailsExpanded(!detailsExpanded)}
              >
                {detailsExpanded ? 'Hide' : 'Details'}
              </Button>
              <Button icon={<ReloadOutlined />} onClick={handleRefresh}>
                Refresh
              </Button>
            </Space>
          </Col>
        </Row>

        {/* 可展开的详细信息区域 */}
        {detailsExpanded && (
          <>
            <Divider style={{ margin: '16px 0' }} />
            <Row gutter={[16, 16]}>
              <Col span={24}>
                <Descriptions column={2} bordered size="small">
                  {project.current_position && (
                    <Descriptions.Item label="Current Position" span={1}>
                      {project.current_position}
                    </Descriptions.Item>
                  )}

                  {project.start_date && (
                    <Descriptions.Item label="Start Date" span={1}>
                      <Space>
                        <CalendarOutlined />
                        {dayjs(project.start_date).format('YYYY-MM-DD')}
                      </Space>
                    </Descriptions.Item>
                  )}

                  <Descriptions.Item label="Created At" span={1}>
                    {dayjs(project.created_at).format('YYYY-MM-DD HH:mm')}
                  </Descriptions.Item>

                  <Descriptions.Item label="Last Updated" span={1}>
                    {dayjs(project.updated_at).format('YYYY-MM-DD HH:mm')}
                  </Descriptions.Item>
                </Descriptions>
              </Col>

              {/* 项目描述 */}
              {project.description && (
                <Col span={24}>
                  <Card 
                    size="small" 
                    title={<Text strong>Description</Text>}
                    style={{ backgroundColor: colors.descriptionBg }}
                  >
                    <Paragraph 
                      style={{ 
                        whiteSpace: 'pre-wrap', 
                        marginBottom: 0,
                        color: colors.descriptionText 
                      }}
                    >
                      {project.description}
                    </Paragraph>
                  </Card>
                </Col>
              )}
            </Row>
          </>
        )}
      </Card>

      {/* Tab内容区域 */}
      <Card>
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          type="card"
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

          {/* News Tab - 第二位 */}
          <TabPane
            tab={
              <span>
                <FileTextOutlined />
                News
              </span>
            }
            key="news"
          >
            <ProjectNewsTab 
              projectId={project.id}
              permissions={permissions}
            />
          </TabPane>

          {/* Files Tab - 第三位 */}
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

          {/* Members Tab - 第四位 */}
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
              onUpdate={loadProject}
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
              onUpdate={loadProject}
            />
          </TabPane>
        </Tabs>
      </Card>
    </div>
    </ConfigProvider>
  );
};

export default ProjectDetailPage;


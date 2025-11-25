/**
 * Investment Project Detail Page
 * 投资项目详情页面 - 包含多个Tab（概览、新闻、里程碑、文件、成员、设置）
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
  Alert
} from 'antd';
import {
  ArrowLeftOutlined,
  FolderOutlined,
  InfoCircleOutlined,
  FileTextOutlined,
  FlagOutlined,
  PaperClipOutlined,
  TeamOutlined,
  SettingOutlined,
  ReloadOutlined
} from '@ant-design/icons';

import { InvestmentProject } from '../types/project';
import { getProject, getMyPermissions } from '../services/projects';
import { PROJECT_STATUS_OPTIONS, PROJECT_TYPE_OPTIONS, PROJECT_PRIORITY_OPTIONS } from '../types/project';

// Tab组件（后续实现）
import ProjectOverviewTab from '../components/projects/ProjectOverviewTab';
import ProjectNewsTab from '../components/projects/ProjectNewsTab';
import ProjectMilestonesTab from '../components/projects/ProjectMilestonesTab';
import ProjectFilesTab from '../components/projects/ProjectFilesTab';
import ProjectMembersTab from '../components/projects/ProjectMembersTab';
import ProjectSettingsTab from '../components/projects/ProjectSettingsTab';

const { Title, Text } = Typography;
const { TabPane } = Tabs;

const ProjectDetailPage: React.FC = () => {
  const { projectId } = useParams<{ projectId: string }>();
  const navigate = useNavigate();
  
  const [project, setProject] = useState<InvestmentProject | null>(null);
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState('overview');
  const [permissions, setPermissions] = useState<any>(null);

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
      message.error(`加载项目失败: ${error.response?.data?.detail || error.message}`);
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
      <div style={{ padding: '24px', textAlign: 'center' }}>
        <Spin size="large" tip="加载项目信息..." />
      </div>
    );
  }

  if (!project) {
    return (
      <div style={{ padding: '24px' }}>
        <Alert
          message="项目不存在"
          description="该项目可能已被删除或您没有访问权限"
          type="warning"
          showIcon
          action={
            <Button type="primary" onClick={handleBack}>
              返回列表
            </Button>
          }
        />
      </div>
    );
  }

  // 获取状态、类型、优先级的显示信息
  const statusOption = PROJECT_STATUS_OPTIONS.find(opt => opt.value === project.status);
  const typeOption = PROJECT_TYPE_OPTIONS.find(opt => opt.value === project.project_type);
  const priorityOption = PROJECT_PRIORITY_OPTIONS.find(opt => opt.value === project.priority);

  return (
    <div style={{ padding: '24px', backgroundColor: '#f0f2f5', minHeight: '100vh' }}>
      {/* 面包屑导航 */}
      <Breadcrumb style={{ marginBottom: 16 }}>
        <Breadcrumb.Item>
          <Button type="link" onClick={handleBack} style={{ padding: 0 }}>
            投资项目
          </Button>
        </Breadcrumb.Item>
        <Breadcrumb.Item>{project.project_name}</Breadcrumb.Item>
      </Breadcrumb>

      {/* 项目头部信息 */}
      <Card style={{ marginBottom: 16 }}>
        <Row gutter={24} align="middle">
          <Col flex="auto">
            <Space direction="vertical" size={8}>
              <Space>
                <Button icon={<ArrowLeftOutlined />} onClick={handleBack}>
                  返回
                </Button>
                <Title level={3} style={{ margin: 0 }}>
                  <FolderOutlined style={{ marginRight: 8 }} />
                  {project.project_name}
                </Title>
              </Space>
              
              <Space size="middle">
                <Tag color={typeOption?.icon}>
                  {typeOption?.icon} {typeOption?.label}
                </Tag>
                <Tag color={statusOption?.color}>
                  {statusOption?.label}
                </Tag>
                <Tag color={priorityOption?.color}>
                  优先级: {priorityOption?.label}
                </Tag>
                {project.ticker_symbol && (
                  <Tag color="blue">{project.ticker_symbol}</Tag>
                )}
                {project.industry && (
                  <Tag color="cyan">{project.industry}</Tag>
                )}
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  项目编号: {project.project_code}
                </Text>
              </Space>
            </Space>
          </Col>

          {/* 统计信息 */}
          <Col>
            <Row gutter={32}>
              <Col>
                <Statistic
                  title="成员"
                  value={project.member_count || 0}
                  prefix={<TeamOutlined />}
                />
              </Col>
              <Col>
                <Statistic
                  title="新闻"
                  value={project.news_count || 0}
                  prefix={<FileTextOutlined />}
                />
              </Col>
              <Col>
                <Statistic
                  title="里程碑"
                  value={project.milestone_count || 0}
                  prefix={<FlagOutlined />}
                />
              </Col>
              <Col>
                <Statistic
                  title="文件"
                  value={project.file_count || 0}
                  prefix={<PaperClipOutlined />}
                />
              </Col>
            </Row>
          </Col>

          {/* 操作按钮 */}
          <Col>
            <Button icon={<ReloadOutlined />} onClick={handleRefresh}>
              刷新
            </Button>
          </Col>
        </Row>
      </Card>

      {/* Tab内容区域 */}
      <Card>
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          type="card"
          size="large"
        >
          <TabPane
            tab={
              <span>
                <InfoCircleOutlined />
                概览
              </span>
            }
            key="overview"
          >
            <ProjectOverviewTab 
              project={project} 
              permissions={permissions}
              onUpdate={loadProject}
            />
          </TabPane>

          <TabPane
            tab={
              <span>
                <FileTextOutlined />
                新闻 ({project.news_count || 0})
              </span>
            }
            key="news"
          >
            <ProjectNewsTab 
              projectId={project.id}
              permissions={permissions}
            />
          </TabPane>

          <TabPane
            tab={
              <span>
                <FlagOutlined />
                里程碑 ({project.milestone_count || 0})
              </span>
            }
            key="milestones"
          >
            <ProjectMilestonesTab 
              projectId={project.id}
              permissions={permissions}
            />
          </TabPane>

          <TabPane
            tab={
              <span>
                <PaperClipOutlined />
                文件 ({project.file_count || 0})
              </span>
            }
            key="files"
          >
            <ProjectFilesTab 
              projectId={project.id}
              permissions={permissions}
            />
          </TabPane>

          <TabPane
            tab={
              <span>
                <TeamOutlined />
                成员 ({project.member_count || 0})
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

          <TabPane
            tab={
              <span>
                <SettingOutlined />
                设置
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
  );
};

export default ProjectDetailPage;


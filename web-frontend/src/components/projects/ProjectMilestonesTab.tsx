/**
 * Project Milestones Tab Component
 * 项目记录管理Tab - 会议纪要、决策记录等
 */

import React, { useState, useEffect } from 'react';
import {
  Timeline,
  Card,
  Button,
  Space,
  Tag,
  Modal,
  Form,
  Input,
  Select,
  DatePicker,
  message,
  Popconfirm,
  Empty,
  Row,
  Col,
  Typography,
  Upload,
  Tabs,
  Transfer,
  List,
  Divider,
  Spin,
  Switch,
  Alert,
  Progress,
  Cascader,
  AutoComplete
} from 'antd';
import {
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  FileTextOutlined,
  CheckCircleOutlined,
  ClockCircleOutlined,
  UploadOutlined,
  PaperClipOutlined,
  DownloadOutlined,
  FileOutlined,
  RobotOutlined,
  ThunderboltOutlined,
  TagOutlined
} from '@ant-design/icons';
import type { UploadFile, UploadProps, RcFile } from 'antd/es/upload/interface';
import dayjs from 'dayjs';
import ReactMarkdown from 'react-markdown';
import { api } from '../../services/api';
import { filesAPI, SectorOption, TickerOption } from '../../services/files';
import TemplateVariablesEditor from '../TemplateVariablesEditor';
import promptTemplateService, { PromptTemplate } from '../../services/promptTemplateService';

import {
  ProjectMilestone,
  MilestoneCreateRequest,
  MilestoneUpdateRequest,
  MILESTONE_TYPE_OPTIONS,
  DECISION_TYPE_OPTIONS,
  ProjectFile,
  ProjectFileCreateRequest,
  FileCategory
} from '../../types/project';

const { TextArea } = Input;
const { Option } = Select;
const { Title, Text, Paragraph } = Typography;

interface ProjectMilestonesTabProps {
  projectId: number;
  permissions: any;
}

// API服务

const getProjectMilestones = async (projectId: number): Promise<ProjectMilestone[]> => {
  return await api.get(`/projects/${projectId}/milestones`);
};

const createMilestone = async (
  projectId: number,
  data: MilestoneCreateRequest
): Promise<ProjectMilestone> => {
  return await api.post(`/projects/${projectId}/milestones`, data);
};

const updateMilestone = async (
  projectId: number,
  milestoneId: number,
  data: MilestoneUpdateRequest
): Promise<ProjectMilestone> => {
  return await api.put(`/projects/${projectId}/milestones/${milestoneId}`, data);
};

const deleteMilestone = async (
  projectId: number,
  milestoneId: number
): Promise<any> => {
  return await api.delete(`/projects/${projectId}/milestones/${milestoneId}`);
};

const getProjectFiles = async (projectId: number): Promise<ProjectFile[]> => {
  return await api.get(`/projects/${projectId}/files`);
};

const addProjectFile = async (
  projectId: number,
  data: ProjectFileCreateRequest
): Promise<ProjectFile> => {
  return await api.post(`/projects/${projectId}/files`, data);
};

const ProjectMilestonesTab: React.FC<ProjectMilestonesTabProps> = ({
  projectId,
  permissions
}) => {
  const [milestones, setMilestones] = useState<ProjectMilestone[]>([]);
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [viewModalVisible, setViewModalVisible] = useState(false);
  const [currentMilestone, setCurrentMilestone] = useState<ProjectMilestone | null>(null);
  const [createForm] = Form.useForm();
  const [editForm] = Form.useForm();

  // 文件相关状态
  const [uploadFileList, setUploadFileList] = useState<UploadFile[]>([]);
  const [uploading, setUploading] = useState(false);
  const [projectFiles, setProjectFiles] = useState<ProjectFile[]>([]);
  const [selectedExistingFileIds, setSelectedExistingFileIds] = useState<number[]>([]);
  const [uploadedFileIds, setUploadedFileIds] = useState<number[]>([]);
  const [filesLoading, setFilesLoading] = useState(false);
  const [activeFileTab, setActiveFileTab] = useState('upload');

  // AI分析相关状态
  const [enableAIAnalysis, setEnableAIAnalysis] = useState(false);
  const [selectedTemplateId, setSelectedTemplateId] = useState<number | undefined>(undefined);
  const [templateVariables, setTemplateVariables] = useState<Record<string, any>>({});
  const [allTemplates, setAllTemplates] = useState<PromptTemplate[]>([]);
  const [analyzing, setAnalyzing] = useState(false);
  const [analysisProgress, setAnalysisProgress] = useState(0);
  const [analysisStatus, setAnalysisStatus] = useState<string>('');

  // 文件标签相关状态（sector、industry、ticker）
  const [sectors, setSectors] = useState<SectorOption[]>([]);
  const [selectedSector, setSelectedSector] = useState<string>('');
  const [selectedIndustry, setSelectedIndustry] = useState<string>('');
  const [tickerOptions, setTickerOptions] = useState<TickerOption[]>([]);
  const [selectedTicker, setSelectedTicker] = useState<string>('');
  const [tickerSearchValue, setTickerSearchValue] = useState<string>('');
  const [loadingSectors, setLoadingSectors] = useState(false);
  const [loadingTickers, setLoadingTickers] = useState(false);

  // 加载记录列表
  const loadMilestones = async () => {
    try {
      const data = await getProjectMilestones(projectId);
      setMilestones(data);
    } catch (error: any) {
      message.error(`加载记录失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 加载项目文件列表
  const loadProjectFiles = async () => {
    try {
      setFilesLoading(true);
      const data = await getProjectFiles(projectId);
      setProjectFiles(data);
    } catch (error: any) {
      message.error(`加载文件列表失败: ${error.response?.data?.detail || error.message}`);
    } finally {
      setFilesLoading(false);
    }
  };

  // 加载模板列表（只加载文件分析类别的模板）
  const loadTemplates = async () => {
    try {
      // 先获取文件分析分类
      const categories = await promptTemplateService.getCategories(true);
      const fileAnalysisCategory = categories.find(cat => cat.category_code === 'file_analysis');
      
      if (fileAnalysisCategory) {
        // 只加载文件分析分类的模板
        const templates = await promptTemplateService.listTemplates({
          category_id: fileAnalysisCategory.id,
          active_only: true
        });
        setAllTemplates(templates);
      } else {
        console.warn('未找到文件分析分类');
        setAllTemplates([]);
      }
    } catch (error: any) {
      console.error('加载模板列表失败:', error);
      setAllTemplates([]);
    }
  };

  // 加载行业分类数据
  const loadSectors = async () => {
    try {
      setLoadingSectors(true);
      const sectorsData = await filesAPI.getSectors();
      setSectors(sectorsData);
    } catch (error: any) {
      console.error('加载行业分类失败:', error);
      message.error('加载行业分类失败');
    } finally {
      setLoadingSectors(false);
    }
  };

  // 处理 sector 和 industry 选择
  const handleSectorIndustryChange = (value: any) => {
    if (value && value.length === 2) {
      setSelectedSector(value[0]);
      setSelectedIndustry(value[1]);
      setSelectedTicker('');
      setTickerSearchValue('');
    } else {
      setSelectedSector('');
      setSelectedIndustry('');
      setSelectedTicker('');
      setTickerSearchValue('');
      setTickerOptions([]);
    }
  };

  // 搜索 ticker
  const handleTickerSearch = async (searchValue: string) => {
    setTickerSearchValue(searchValue);
    
    if (!searchValue || searchValue.length < 1) {
      setTickerOptions([]);
      return;
    }

    try {
      setLoadingTickers(true);
      const tickers = await filesAPI.searchTickers({
        sector: selectedSector,
        industry: selectedIndustry,
        search: searchValue
      });
      setTickerOptions(tickers);
    } catch (error: any) {
      console.error('搜索ticker失败:', error);
    } finally {
      setLoadingTickers(false);
    }
  };

  // 选择 ticker
  const handleTickerSelect = (value: string, option: any) => {
    setSelectedTicker(value);
    setTickerSearchValue(option.label || value);
    
    console.log('选择的 ticker option:', option);
    
    // 自动设置 sector 和 industry（从 option 中直接获取）
    if (option && option.sector && option.industry) {
      console.log('自动设置 sector 和 industry:', option.sector, option.industry);
      setSelectedSector(option.sector);
      setSelectedIndustry(option.industry);
      message.success(`自动选择行业: ${option.sector} → ${option.industry}`);
    } else {
      // 如果 option 中没有，尝试从 tickerOptions 中查找
      const selectedTickerInfo = tickerOptions.find(ticker => ticker.value === value);
      console.log('从 tickerOptions 查找 ticker info:', selectedTickerInfo);
      
      if (selectedTickerInfo && selectedTickerInfo.sector && selectedTickerInfo.industry) {
        console.log('设置 sector 和 industry:', selectedTickerInfo.sector, selectedTickerInfo.industry);
        setSelectedSector(selectedTickerInfo.sector);
        setSelectedIndustry(selectedTickerInfo.industry);
        message.success(`自动选择行业: ${selectedTickerInfo.sector} → ${selectedTickerInfo.industry}`);
      } else {
        console.warn('未找到 ticker 的 sector/industry 信息:', value);
      }
    }
  };

  useEffect(() => {
    loadMilestones();
    loadProjectFiles();
    loadTemplates();
    loadSectors();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [projectId]);

  // 上传文件处理
  const handleUploadFiles = async () => {
    if (uploadFileList.length === 0) {
      return;
    }

    // 验证必填字段
    if (!selectedSector || !selectedIndustry) {
      message.error('请选择行业和产业（必填）');
      return;
    }

    // 如果启用AI分析，验证模板选择
    if (enableAIAnalysis && !selectedTemplateId) {
      message.error('请选择分析模板');
      return;
    }

    try {
      setUploading(true);

      // 步骤1: 上传文件到服务器
      const filesToUpload: File[] = uploadFileList
        .map(file => file.originFileObj)
        .filter((file): file is RcFile => file !== undefined) as File[];
      
      if (filesToUpload.length === 0) {
        message.error('没有有效的文件可上传');
        setUploading(false);
        return;
      }

      const uploadResponse = await filesAPI.uploadFiles(filesToUpload);

      // 步骤2: 将上传的文件关联到项目
      const newFileIds: number[] = [];
      const filePaths: string[] = [];
      const rawFileIds: number[] = []; // 原始文件ID（用于标签）
      
      for (const uploadedFile of uploadResponse) {
        const projectFile = await addProjectFile(projectId, {
          file_name: uploadedFile.filename,
          file_path: uploadedFile.file_path,
          file_type: uploadedFile.file_type,
          file_size: uploadedFile.file_size,
          category: 'other' as FileCategory,
          description: '从记录上传'
        });
        newFileIds.push(projectFile.id);
        filePaths.push(uploadedFile.file_path);
        // 获取原始上传的文件ID
        if ((uploadedFile as any).id) {
          rawFileIds.push((uploadedFile as any).id);
        }
      }

      // 步骤2.5: 为上传的文件设置标签（sector、industry、ticker）
      if (rawFileIds.length > 0) {
        try {
          await filesAPI.batchCreateFileTags({
            file_ids: rawFileIds,
            sector: selectedSector,
            industry: selectedIndustry,
            ticker: selectedTicker || undefined
          });
          console.log(`已为 ${rawFileIds.length} 个文件设置标签:`, {
            sector: selectedSector,
            industry: selectedIndustry,
            ticker: selectedTicker
          });
        } catch (tagError: any) {
          console.error('设置文件标签失败:', tagError);
          message.warning('文件上传成功，但标签设置失败');
        }
      }

      setUploadedFileIds([...uploadedFileIds, ...newFileIds]);
      message.success(`成功上传 ${newFileIds.length} 个文件并设置标签`);
      setUploadFileList([]);
      
      // 刷新项目文件列表
      loadProjectFiles();

      // 步骤3: 如果启用AI分析，触发分析任务
      console.log('检查AI分析条件:', {
        enableAIAnalysis,
        selectedTemplateId,
        filePathsLength: filePaths.length,
        filePaths
      });

      if (enableAIAnalysis && selectedTemplateId) {
        console.log('开始AI分析，文件路径:', filePaths);
        await handleAnalyzeFiles(filePaths);
      } else {
        console.log('跳过AI分析:', { enableAIAnalysis, selectedTemplateId });
      }
    } catch (error: any) {
      console.error('文件上传或分析失败:', error);
      message.error(`文件上传失败: ${error.response?.data?.detail || error.message}`);
    } finally {
      setUploading(false);
    }
  };

  // AI分析文件
  const handleAnalyzeFiles = async (filePaths: string[]) => {
    try {
      console.log('handleAnalyzeFiles 开始执行');
      console.log('文件路径:', filePaths);
      console.log('模板ID:', selectedTemplateId);
      console.log('模板变量:', templateVariables);

      setAnalyzing(true);
      setAnalysisProgress(0);
      setAnalysisStatus('正在提交分析任务...');

      // 调用文件处理API
      console.log('准备调用 /files/process API');
      const requestData = {
        file_paths: filePaths,
        task_type: 'local_llm_analysis',  // ⭐ 必填字段
        analysis_mode: 'local_llm',
        template_id: selectedTemplateId,
        template_variables: templateVariables,
        custom_inputs: {},
        params: {
          language: ['ch_sim', 'en'],
          fast_mode: false,
          disable_ocr: false,
          max_pages: 10
        }
      };
      console.log('API请求数据:', requestData);

      const response = await api.post('/files/process', requestData);
      console.log('API响应:', response);

      const taskId = response.task_id;
      console.log('获得任务ID:', taskId);
      message.success('AI分析任务已提交');

      // 开始轮询任务状态
      pollAnalysisStatus(taskId);
    } catch (error: any) {
      console.error('AI分析失败:', error);
      console.error('错误详情:', error.response?.data);
      message.error(`启动AI分析失败: ${error.response?.data?.detail || error.message}`);
      setAnalyzing(false);
    }
  };

  // 轮询分析状态
  const pollAnalysisStatus = async (taskId: string) => {
    const pollInterval = setInterval(async () => {
      try {
        const status = await api.get(`/files/tasks/${taskId}/status`);
        
        setAnalysisProgress(status.progress || 0);
        setAnalysisStatus(status.message || '分析中...');

        if (status.status === 'completed') {
          clearInterval(pollInterval);
          setAnalyzing(false);
          setAnalysisProgress(100);
          setAnalysisStatus('分析完成！');
          message.success('文件AI分析完成');
          
          // 重置分析相关状态
          setTimeout(() => {
            setAnalysisProgress(0);
            setAnalysisStatus('');
          }, 3000);
        } else if (status.status === 'failed') {
          clearInterval(pollInterval);
          setAnalyzing(false);
          setAnalysisStatus('分析失败');
          message.error('文件AI分析失败');
        }
      } catch (error: any) {
        clearInterval(pollInterval);
        setAnalyzing(false);
        console.error('获取分析状态失败:', error);
      }
    }, 2000); // 每2秒轮询一次
  };

  // Upload组件配置
  const uploadProps: UploadProps = {
    onRemove: (file) => {
      const index = uploadFileList.indexOf(file);
      const newFileList = uploadFileList.slice();
      newFileList.splice(index, 1);
      setUploadFileList(newFileList);
    },
    beforeUpload: (file) => {
      // 手动构造 UploadFile 对象，包含 originFileObj
      const uploadFile: UploadFile = {
        uid: `${Date.now()}-${file.name}`,
        name: file.name,
        status: 'done',
        size: file.size,
        type: file.type,
        originFileObj: file as RcFile,
      };
      setUploadFileList([...uploadFileList, uploadFile]);
      return false; // 阻止自动上传
    },
    fileList: uploadFileList,
    multiple: true
  };

  // 获取关联的文件列表
  const getAttachedFiles = (fileIds?: number[]): ProjectFile[] => {
    if (!fileIds || fileIds.length === 0) return [];
    return projectFiles.filter(file => fileIds.includes(file.id));
  };

  // 创建记录
  const handleCreate = async (values: any) => {
    try {
      // 合并所有文件IDs：上传的 + 选择的
      const allFileIds = [...uploadedFileIds, ...selectedExistingFileIds];

      const milestoneData: MilestoneCreateRequest = {
        milestone_type: values.milestone_type,
        title: values.title,
        content: values.content,
        file_ids: allFileIds.length > 0 ? allFileIds : undefined,
        decision_type: values.decision_type,
        decision_rationale: values.decision_rationale,
        milestone_date: values.milestone_date ? values.milestone_date.format('YYYY-MM-DD') : undefined
      };

      await createMilestone(projectId, milestoneData);
      message.success('记录创建成功');
      setCreateModalVisible(false);
      createForm.resetFields();
      
      // 重置文件相关状态
      setUploadFileList([]);
      setUploadedFileIds([]);
      setSelectedExistingFileIds([]);
      setActiveFileTab('upload');
      
      loadMilestones();
    } catch (error: any) {
      message.error(`创建失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 打开编辑模态框
  const handleEdit = (milestone: ProjectMilestone) => {
    setCurrentMilestone(milestone);
    editForm.setFieldsValue({
      milestone_type: milestone.milestone_type,
      title: milestone.title,
      content: milestone.content,
      decision_type: milestone.decision_type,
      decision_rationale: milestone.decision_rationale,
      milestone_date: milestone.milestone_date ? dayjs(milestone.milestone_date) : null
    });
    
    // 设置已有的文件IDs
    setSelectedExistingFileIds(milestone.file_ids || []);
    setUploadedFileIds([]);
    setUploadFileList([]);
    
    setEditModalVisible(true);
  };

  // 更新记录
  const handleUpdate = async (values: any) => {
    if (!currentMilestone) return;

    try {
      // 合并所有文件IDs：原有的 + 新上传的 + 新选择的
      const allFileIds = [...uploadedFileIds, ...selectedExistingFileIds];

      const updateData: MilestoneUpdateRequest = {
        milestone_type: values.milestone_type,
        title: values.title,
        content: values.content,
        file_ids: allFileIds.length > 0 ? allFileIds : undefined,
        decision_type: values.decision_type,
        decision_rationale: values.decision_rationale,
        milestone_date: values.milestone_date ? values.milestone_date.format('YYYY-MM-DD') : undefined
      };

      await updateMilestone(projectId, currentMilestone.id, updateData);
      message.success('记录更新成功');
      setEditModalVisible(false);
      
      // 重置文件相关状态
      setUploadFileList([]);
      setUploadedFileIds([]);
      setSelectedExistingFileIds([]);
      
      loadMilestones();
    } catch (error: any) {
      message.error(`更新失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 删除记录
  const handleDelete = async (milestoneId: number, title: string) => {
    try {
      await deleteMilestone(projectId, milestoneId);
      message.success(`记录已删除: ${title}`);
      loadMilestones();
    } catch (error: any) {
      message.error(`删除失败: ${error.response?.data?.detail || error.message}`);
    }
  };

  // 查看详情
  const handleView = (milestone: ProjectMilestone) => {
    setCurrentMilestone(milestone);
    setViewModalVisible(true);
  };

  // 获取记录类型信息
  const getMilestoneTypeOption = (type: string) => {
    return MILESTONE_TYPE_OPTIONS.find(opt => opt.value === type);
  };

  // 获取决策类型信息
  const getDecisionTypeOption = (type: string) => {
    return DECISION_TYPE_OPTIONS.find(opt => opt.value === type);
  };

  const canEdit = permissions?.permissions?.can_edit || false;

  return (
    <div>
      {/* 操作栏 */}
      <Space style={{ marginBottom: 16 }}>
        {canEdit && (
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setCreateModalVisible(true)}
          >
            创建记录
          </Button>
        )}
      </Space>

      {/* 记录时间线 - 左右交替显示 */}
      {milestones.length > 0 ? (
        <Timeline mode="alternate">
          {milestones.map((milestone, index) => {
            const typeOption = getMilestoneTypeOption(milestone.milestone_type);
            const decisionOption = milestone.decision_type
              ? getDecisionTypeOption(milestone.decision_type)
              : null;
            const isLeft = index % 2 === 0;

            return (
              <Timeline.Item
                key={milestone.id}
                label={
                  <div style={{ 
                    textAlign: isLeft ? 'right' : 'left',
                    paddingRight: isLeft ? 12 : 0,
                    paddingLeft: isLeft ? 0 : 12,
                    fontWeight: 500,
                    color: '#666'
                  }}>
                    {milestone.milestone_date
                      ? dayjs(milestone.milestone_date).format('YYYY-MM-DD')
                      : dayjs(milestone.created_at).format('YYYY-MM-DD')}
                  </div>
                }
                dot={
                  milestone.milestone_type === 'decision' ? (
                    <CheckCircleOutlined style={{ fontSize: 16, color: '#52c41a' }} />
                  ) : (
                    <ClockCircleOutlined style={{ fontSize: 16, color: '#1890ff' }} />
                  )
                }
              >
                <Card
                  size="small"
                  style={{ 
                    maxWidth: 360,
                    marginLeft: isLeft ? 0 : 'auto',
                    marginRight: isLeft ? 'auto' : 0,
                    boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
                    borderRadius: 8
                  }}
                  title={
                    <Space size={4} wrap>
                      <span>{typeOption?.icon}</span>
                      <span style={{ fontSize: 14 }}>{milestone.title}</span>
                      {decisionOption && (
                        <Tag color={decisionOption.color} style={{ marginLeft: 4 }}>
                          {decisionOption.label}
                        </Tag>
                      )}
                      {milestone.file_ids && milestone.file_ids.length > 0 && (
                        <Tag icon={<PaperClipOutlined />} color="blue">
                          {milestone.file_ids.length}
                        </Tag>
                      )}
                    </Space>
                  }
                  extra={
                    canEdit && (
                      <Space size={0}>
                        <Button
                          type="link"
                          size="small"
                          icon={<FileTextOutlined />}
                          onClick={() => handleView(milestone)}
                          title="View"
                        />
                        <Button
                          type="link"
                          size="small"
                          icon={<EditOutlined />}
                          onClick={() => handleEdit(milestone)}
                          title="Edit"
                        />
                        <Popconfirm
                          title="确认删除记录？"
                          onConfirm={() => handleDelete(milestone.id, milestone.title)}
                          okText="确认"
                          cancelText="取消"
                        >
                          <Button
                            type="link"
                            danger
                            size="small"
                            icon={<DeleteOutlined />}
                            title="Delete"
                          />
                        </Popconfirm>
                      </Space>
                    )
                  }
                >
                  <Paragraph
                    ellipsis={{ rows: 2, expandable: true }}
                    style={{ marginBottom: 8, fontSize: 13 }}
                  >
                    {milestone.content || 'No content'}
                  </Paragraph>
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    {milestone.created_by_username || `User ${milestone.created_by}`}
                    {' · '}
                    {dayjs(milestone.created_at).format('MM-DD HH:mm')}
                  </Text>
                </Card>
              </Timeline.Item>
            );
          })}
        </Timeline>
      ) : (
        <Empty
          description="暂无记录，点击'创建记录'开始记录事件进展"
          style={{ padding: '60px 0' }}
        />
      )}

      {/* 创建记录模态框 */}
      <Modal
        title="创建记录"
        open={createModalVisible}
        onOk={() => createForm.submit()}
        onCancel={() => {
          setCreateModalVisible(false);
          createForm.resetFields();
          // 重置文件相关状态
          setUploadFileList([]);
          setUploadedFileIds([]);
          setSelectedExistingFileIds([]);
          // 重置标签选择状态
          setSelectedSector('');
          setSelectedIndustry('');
          setSelectedTicker('');
          setTickerSearchValue('');
          setTickerOptions([]);
          // 重置AI分析状态
          setEnableAIAnalysis(false);
          setSelectedTemplateId(undefined);
          setTemplateVariables({});
        }}
        width={800}
        okText="创建"
        cancelText="取消"
      >
        <Form
          form={createForm}
          layout="vertical"
          onFinish={handleCreate}
        >
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label="记录类型"
                name="milestone_type"
                rules={[{ required: true, message: '请选择记录类型' }]}
              >
                <Select placeholder="选择类型">
                  {MILESTONE_TYPE_OPTIONS.map(opt => (
                    <Option key={opt.value} value={opt.value}>
                      {opt.icon} {opt.label}
                    </Option>
                  ))}
                </Select>
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item
                label="日期"
                name="milestone_date"
              >
                <DatePicker style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item
            label="标题"
            name="title"
            rules={[{ required: true, message: '请输入标题' }]}
          >
            <Input placeholder="例如：Q1季度回顾会议" />
          </Form.Item>

          <Form.Item
            label="内容"
            name="content"
            tooltip="支持 Markdown 格式"
          >
            <TextArea
              rows={8}
              placeholder="详细记录会议纪要、研究结果、决策理由等...&#10;支持 Markdown 格式"
            />
          </Form.Item>

          <Form.Item
            noStyle
            shouldUpdate={(prevValues, currentValues) =>
              prevValues.milestone_type !== currentValues.milestone_type
            }
          >
            {({ getFieldValue }) =>
              getFieldValue('milestone_type') === 'decision' && (
                <>
                  <Form.Item
                    label="决策类型"
                    name="decision_type"
                  >
                    <Select placeholder="选择决策类型">
                      {DECISION_TYPE_OPTIONS.map(opt => (
                        <Option key={opt.value} value={opt.value}>
                          {opt.label}
                        </Option>
                      ))}
                    </Select>
                  </Form.Item>

                  <Form.Item
                    label="决策理由"
                    name="decision_rationale"
                  >
                    <TextArea rows={4} placeholder="解释做出此决策的原因..." />
                  </Form.Item>
                </>
              )
            }
          </Form.Item>

          <Divider orientation="left">文件附件</Divider>

          <Tabs activeKey={activeFileTab} onChange={setActiveFileTab}>
            <Tabs.TabPane tab="上传新文件" key="upload">
              <Space direction="vertical" style={{ width: '100%' }} size="large">
                {/* 行业和股票选择（必填） */}
                <Card size="small" title={<Space><TagOutlined /> 文件标签（必填）</Space>} style={{ background: '#fffbe6', border: '1px solid #ffe58f' }}>
                  <Space direction="vertical" style={{ width: '100%' }}>
                    <div>
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        * 所有上传的文件必须设置行业和产业标签
                      </Text>
                    </div>
                    
                    <Form.Item label="Sector & Industry" required style={{ marginBottom: 8 }}>
                      <Cascader
                        options={sectors}
                        value={selectedSector && selectedIndustry ? [selectedSector, selectedIndustry] : []}
                        onChange={handleSectorIndustryChange}
                        placeholder="选择 sector 和 industry"
                        loading={loadingSectors}
                        showSearch={{
                          filter: (inputValue, path) => {
                            return path.some(option => 
                              option.label.toLowerCase().includes(inputValue.toLowerCase())
                            );
                          }
                        }}
                        style={{ width: '100%' }}
                      />
                    </Form.Item>

                    <Form.Item label="Ticker（可选）" style={{ marginBottom: 0 }}>
                      <AutoComplete
                        value={tickerSearchValue}
                        options={tickerOptions.map(ticker => ({
                          value: ticker.value,
                          label: ticker.label,
                          sector: ticker.sector,
                          industry: ticker.industry
                        }))}
                        onSearch={handleTickerSearch}
                        onSelect={handleTickerSelect}
                        placeholder="搜索 ticker（可选）"
                        style={{ width: '100%' }}
                        filterOption={false}
                        notFoundContent={loadingTickers ? 'Loading...' : 'No tickers found'}
                      />
                    </Form.Item>

                    {(selectedSector && selectedIndustry) && (
                      <Alert
                        message={
                          <span>
                            <strong>已选择:</strong> {selectedSector} → {selectedIndustry}
                            {selectedTicker && <><br /><strong>Ticker:</strong> {selectedTicker}</>}
                          </span>
                        }
                        type="success"
                        showIcon
                        style={{ marginTop: 8 }}
                      />
                    )}
                  </Space>
                </Card>

                <Upload {...uploadProps}>
                  <Button icon={<UploadOutlined />} disabled={!selectedSector || !selectedIndustry}>
                    选择文件
                  </Button>
                </Upload>
                {(!selectedSector || !selectedIndustry) && (
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    请先选择 sector 和 industry 后才能上传文件
                  </Text>
                )}

                {/* AI分析选项 */}
                {uploadFileList.length > 0 && (
                  <Card size="small" style={{ background: '#f0f5ff' }}>
                    <Space direction="vertical" style={{ width: '100%' }}>
                      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                        <Space>
                          <RobotOutlined style={{ color: '#1890ff', fontSize: 16 }} />
                          <Text strong>AI 智能分析</Text>
                        </Space>
                        <Switch
                          checked={enableAIAnalysis}
                          onChange={setEnableAIAnalysis}
                          checkedChildren="启用"
                          unCheckedChildren="跳过"
                        />
                      </div>
                      
                      {enableAIAnalysis && (
                        <>
                          <Alert
                            message="文件将通过 LlamaParse 解析并使用 AI 进行智能分析"
                            type="info"
                            showIcon
                            icon={<ThunderboltOutlined />}
                            style={{ marginTop: 8 }}
                          />
                          
                          <Select
                            placeholder="选择分析模板"
                            value={selectedTemplateId}
                            onChange={setSelectedTemplateId}
                            style={{ width: '100%' }}
                            showSearch
                            optionFilterProp="children"
                          >
                            {allTemplates.map(template => (
                              <Select.Option key={template.id} value={template.id}>
                                <Space>
                                  <RobotOutlined style={{ color: '#1890ff' }} />
                                  <span>{template.template_name}</span>
                                  {template.is_default && <Tag color="blue">默认</Tag>}
                                </Space>
                              </Select.Option>
                            ))}
                          </Select>
                          
                          {selectedTemplateId && (
                            <TemplateVariablesEditor
                              templateId={selectedTemplateId}
                              values={templateVariables}
                              onChange={setTemplateVariables}
                            />
                          )}
                        </>
                      )}
                    </Space>
                  </Card>
                )}

                {uploadFileList.length > 0 && (
                  <Button
                    type="primary"
                    size="large"
                    onClick={handleUploadFiles}
                    loading={uploading || analyzing}
                    icon={enableAIAnalysis ? <RobotOutlined /> : <UploadOutlined />}
                    block
                  >
                    {uploading ? '上传中...' : analyzing ? '分析中...' : 
                     enableAIAnalysis ? `上传并分析 ${uploadFileList.length} 个文件` : 
                     `上传 ${uploadFileList.length} 个文件`}
                  </Button>
                )}

                {/* 分析进度 */}
                {analyzing && (
                  <Card size="small">
                    <Space direction="vertical" style={{ width: '100%' }}>
                      <Text strong>AI 分析进度</Text>
                      <Progress percent={analysisProgress} status="active" />
                      <Text type="secondary" style={{ fontSize: 12 }}>{analysisStatus}</Text>
                    </Space>
                  </Card>
                )}

                {uploadedFileIds.length > 0 && !analyzing && (
                  <Tag color="success" icon={<CheckCircleOutlined />}>
                    已上传 {uploadedFileIds.length} 个文件
                  </Tag>
                )}
              </Space>
            </Tabs.TabPane>

            <Tabs.TabPane tab="选择已有文件" key="select">
              <Spin spinning={filesLoading}>
                <Transfer
                  dataSource={projectFiles.map(file => ({
                    key: file.id.toString(),
                    title: file.file_name,
                    description: `${dayjs(file.uploaded_at).format('YYYY-MM-DD')} | ${(file.file_size || 0) > 0 ? ((file.file_size || 0) / 1024).toFixed(2) + ' KB' : '-'}`
                  }))}
                  titles={['可选文件', '已选文件']}
                  targetKeys={selectedExistingFileIds.map(id => id.toString())}
                  onChange={(targetKeys) => {
                    setSelectedExistingFileIds(targetKeys.map(key => parseInt(String(key))));
                  }}
                  render={item => `${item.title} - ${item.description}`}
                  listStyle={{
                    width: 300,
                    height: 300,
                  }}
                />
              </Spin>
            </Tabs.TabPane>
          </Tabs>

          {(uploadedFileIds.length > 0 || selectedExistingFileIds.length > 0) && (
            <div style={{ marginTop: 8 }}>
              <Text type="secondary">
                总共将关联 {uploadedFileIds.length + selectedExistingFileIds.length} 个文件
              </Text>
            </div>
          )}
        </Form>
      </Modal>

      {/* 编辑记录模态框 */}
      <Modal
        title="编辑记录"
        open={editModalVisible}
        onOk={() => editForm.submit()}
        onCancel={() => {
          setEditModalVisible(false);
          setCurrentMilestone(null);
          // 重置文件相关状态
          setUploadFileList([]);
          setUploadedFileIds([]);
          setSelectedExistingFileIds([]);
          // 重置标签选择状态
          setSelectedSector('');
          setSelectedIndustry('');
          setSelectedTicker('');
          setTickerSearchValue('');
          setTickerOptions([]);
        }}
        width={800}
        okText="保存"
        cancelText="取消"
      >
        <Form
          form={editForm}
          layout="vertical"
          onFinish={handleUpdate}
        >
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item
                label="记录类型"
                name="milestone_type"
                rules={[{ required: true, message: '请选择记录类型' }]}
              >
                <Select>
                  {MILESTONE_TYPE_OPTIONS.map(opt => (
                    <Option key={opt.value} value={opt.value}>
                      {opt.icon} {opt.label}
                    </Option>
                  ))}
                </Select>
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="日期" name="milestone_date">
                <DatePicker style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item
            label="标题"
            name="title"
            rules={[{ required: true, message: '请输入标题' }]}
          >
            <Input />
          </Form.Item>

          <Form.Item label="内容" name="content">
            <TextArea rows={8} />
          </Form.Item>

          <Form.Item
            noStyle
            shouldUpdate={(prevValues, currentValues) =>
              prevValues.milestone_type !== currentValues.milestone_type
            }
          >
            {({ getFieldValue }) =>
              getFieldValue('milestone_type') === 'decision' && (
                <>
                  <Form.Item label="决策类型" name="decision_type">
                    <Select>
                      {DECISION_TYPE_OPTIONS.map(opt => (
                        <Option key={opt.value} value={opt.value}>
                          {opt.label}
                        </Option>
                      ))}
                    </Select>
                  </Form.Item>

                  <Form.Item label="决策理由" name="decision_rationale">
                    <TextArea rows={4} />
                  </Form.Item>
                </>
              )
            }
          </Form.Item>

          <Divider orientation="left">文件附件</Divider>

          <Tabs activeKey={activeFileTab} onChange={setActiveFileTab}>
            <Tabs.TabPane tab="上传新文件" key="upload">
              <Space direction="vertical" style={{ width: '100%' }} size="large">
                {/* 行业和股票选择（必填） */}
                <Card size="small" title={<Space><TagOutlined /> 文件标签（必填）</Space>} style={{ background: '#fffbe6', border: '1px solid #ffe58f' }}>
                  <Space direction="vertical" style={{ width: '100%' }}>
                    <div>
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        * 所有上传的文件必须设置行业和产业标签
                      </Text>
                    </div>
                    
                    <Form.Item label="Sector & Industry" required style={{ marginBottom: 8 }}>
                      <Cascader
                        options={sectors}
                        value={selectedSector && selectedIndustry ? [selectedSector, selectedIndustry] : []}
                        onChange={handleSectorIndustryChange}
                        placeholder="选择 sector 和 industry"
                        loading={loadingSectors}
                        showSearch={{
                          filter: (inputValue, path) => {
                            return path.some(option => 
                              option.label.toLowerCase().includes(inputValue.toLowerCase())
                            );
                          }
                        }}
                        style={{ width: '100%' }}
                      />
                    </Form.Item>

                    <Form.Item label="Ticker（可选）" style={{ marginBottom: 0 }}>
                      <AutoComplete
                        value={tickerSearchValue}
                        options={tickerOptions.map(ticker => ({
                          value: ticker.value,
                          label: ticker.label,
                          sector: ticker.sector,
                          industry: ticker.industry
                        }))}
                        onSearch={handleTickerSearch}
                        onSelect={handleTickerSelect}
                        placeholder="搜索 ticker（可选）"
                        style={{ width: '100%' }}
                        filterOption={false}
                        notFoundContent={loadingTickers ? 'Loading...' : 'No tickers found'}
                      />
                    </Form.Item>

                    {(selectedSector && selectedIndustry) && (
                      <Alert
                        message={
                          <span>
                            <strong>已选择:</strong> {selectedSector} → {selectedIndustry}
                            {selectedTicker && <><br /><strong>Ticker:</strong> {selectedTicker}</>}
                          </span>
                        }
                        type="success"
                        showIcon
                        style={{ marginTop: 8 }}
                      />
                    )}
                  </Space>
                </Card>

                <Upload {...uploadProps}>
                  <Button icon={<UploadOutlined />} disabled={!selectedSector || !selectedIndustry}>
                    选择文件
                  </Button>
                </Upload>
                {(!selectedSector || !selectedIndustry) && (
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    请先选择 sector 和 industry 后才能上传文件
                  </Text>
                )}

                {uploadFileList.length > 0 && (
                  <Button
                    type="primary"
                    onClick={handleUploadFiles}
                    loading={uploading}
                    icon={<UploadOutlined />}
                  >
                    上传 {uploadFileList.length} 个文件
                  </Button>
                )}
                {uploadedFileIds.length > 0 && (
                  <Tag color="success">
                    已上传 {uploadedFileIds.length} 个文件
                  </Tag>
                )}
              </Space>
            </Tabs.TabPane>

            <Tabs.TabPane tab="选择已有文件" key="select">
              <Spin spinning={filesLoading}>
                <Transfer
                  dataSource={projectFiles.map(file => ({
                    key: file.id.toString(),
                    title: file.file_name,
                    description: `${dayjs(file.uploaded_at).format('YYYY-MM-DD')} | ${(file.file_size || 0) > 0 ? ((file.file_size || 0) / 1024).toFixed(2) + ' KB' : '-'}`
                  }))}
                  titles={['可选文件', '已选文件']}
                  targetKeys={selectedExistingFileIds.map(id => id.toString())}
                  onChange={(targetKeys) => {
                    setSelectedExistingFileIds(targetKeys.map(key => parseInt(String(key))));
                  }}
                  render={item => `${item.title} - ${item.description}`}
                  listStyle={{
                    width: 300,
                    height: 300,
                  }}
                />
              </Spin>
            </Tabs.TabPane>
          </Tabs>

          {(uploadedFileIds.length > 0 || selectedExistingFileIds.length > 0) && (
            <div style={{ marginTop: 8 }}>
              <Text type="secondary">
                总共将关联 {uploadedFileIds.length + selectedExistingFileIds.length} 个文件
              </Text>
            </div>
          )}
        </Form>
      </Modal>

      {/* 查看详情模态框 */}
      <Modal
        title={
          currentMilestone && (
            <Space>
              <span>{getMilestoneTypeOption(currentMilestone.milestone_type)?.icon}</span>
              <span>{currentMilestone.title}</span>
            </Space>
          )
        }
        open={viewModalVisible}
        onCancel={() => {
          setViewModalVisible(false);
          setCurrentMilestone(null);
        }}
        width={800}
        footer={[
          <Button key="close" onClick={() => setViewModalVisible(false)}>
            关闭
          </Button>
        ]}
      >
        {currentMilestone && (
          <div>
            <Space direction="vertical" size="large" style={{ width: '100%' }}>
              <div>
                <Text type="secondary">日期: </Text>
                <Text>
                  {currentMilestone.milestone_date
                    ? dayjs(currentMilestone.milestone_date).format('YYYY-MM-DD')
                    : dayjs(currentMilestone.created_at).format('YYYY-MM-DD')}
                </Text>
              </div>

              {currentMilestone.decision_type && (
                <div>
                  <Text type="secondary">决策类型: </Text>
                  <Tag color={getDecisionTypeOption(currentMilestone.decision_type)?.color}>
                    {getDecisionTypeOption(currentMilestone.decision_type)?.label}
                  </Tag>
                </div>
              )}

              {currentMilestone.content && (
                <div>
                  <Title level={5}>内容</Title>
                  <Card>
                    <ReactMarkdown>{currentMilestone.content}</ReactMarkdown>
                  </Card>
                </div>
              )}

              {currentMilestone.decision_rationale && (
                <div>
                  <Title level={5}>决策理由</Title>
                  <Card>
                    <Paragraph>{currentMilestone.decision_rationale}</Paragraph>
                  </Card>
                </div>
              )}

              {currentMilestone.file_ids && currentMilestone.file_ids.length > 0 && (
                <div>
                  <Title level={5}>附件文件 ({currentMilestone.file_ids.length})</Title>
                  <Card>
                    <List
                      size="small"
                      dataSource={getAttachedFiles(currentMilestone.file_ids)}
                      renderItem={(file) => (
                        <List.Item
                          actions={[
                            <Button
                              key="download"
                              type="link"
                              size="small"
                              icon={<DownloadOutlined />}
                              href={file.file_path}
                              target="_blank"
                            >
                              下载
                            </Button>
                          ]}
                        >
                          <List.Item.Meta
                            avatar={<FileOutlined style={{ fontSize: 20, color: '#1890ff' }} />}
                            title={file.file_name}
                            description={
                              <Space split="|">
                                <Text type="secondary" style={{ fontSize: 12 }}>
                                  {dayjs(file.uploaded_at).format('YYYY-MM-DD HH:mm')}
                                </Text>
                                <Text type="secondary" style={{ fontSize: 12 }}>
                                  {file.file_size ? `${(file.file_size / 1024).toFixed(2)} KB` : '-'}
                                </Text>
                                {file.uploaded_by_username && (
                                  <Text type="secondary" style={{ fontSize: 12 }}>
                                    上传者: {file.uploaded_by_username}
                                  </Text>
                                )}
                              </Space>
                            }
                          />
                        </List.Item>
                      )}
                    />
                  </Card>
                </div>
              )}

              <div>
                <Text type="secondary">
                  创建者: {currentMilestone.created_by_username || `用户 ${currentMilestone.created_by}`}
                  {' | '}
                  创建时间: {dayjs(currentMilestone.created_at).format('YYYY-MM-DD HH:mm')}
                </Text>
              </div>
            </Space>
          </div>
        )}
      </Modal>
    </div>
  );
};

export default ProjectMilestonesTab;


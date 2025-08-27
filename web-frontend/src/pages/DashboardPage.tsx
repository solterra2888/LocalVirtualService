import React, { useState, useEffect } from 'react';
import { Row, Col, Layout, Card, Tree, Input, Tabs, List, Typography, Tag, Empty, Spin, message, Avatar, Divider, Space, Button } from 'antd';
import {
  SearchOutlined, 
  StockOutlined, 
  FileTextOutlined,
  RobotOutlined, 
  TrophyOutlined,
  CalendarOutlined,
  DollarOutlined
} from '@ant-design/icons';
import { useGlobalLogger } from '../contexts/LoggerContext';
import { dashboardAPI, type CompanyInfo, type SectorInfo, type DashboardNewsItem, type DashboardAnalysisResult } from '../services/dashboard';
import type { DataNode } from 'antd/es/tree';

const { Content, Sider } = Layout;
const { Title, Text, Paragraph } = Typography;
const { TabPane } = Tabs;

// 数据状态类型定义
interface SectorData {
  sector: string;
  companies: CompanyInfo[];
}

const DashboardPage: React.FC = () => {
  const logger = useGlobalLogger();
  
  // 状态管理
  const [selectedKey, setSelectedKey] = useState<string>('');
  const [selectedType, setSelectedType] = useState<'sector' | 'company' | ''>('');
  const [selectedCompany, setSelectedCompany] = useState<CompanyInfo | null>(null);
  const [selectedSector, setSelectedSector] = useState<string>('');
  const [searchValue, setSearchValue] = useState<string>('');
  
  // 数据状态
  const [sectors, setSectors] = useState<SectorData[]>([]);
  const [companies, setCompanies] = useState<CompanyInfo[]>([]);
  const [treeData, setTreeData] = useState<DataNode[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  
  // 右侧内容数据
  const [newsData, setNewsData] = useState<DashboardNewsItem[]>([]);
  const [analysisData, setAnalysisData] = useState<DashboardAnalysisResult[]>([]);
  const [reportsData, setReportsData] = useState<DashboardAnalysisResult[]>([]);
  const [contentLoading, setContentLoading] = useState<boolean>(false);

  // 初始化数据
  useEffect(() => {
    loadSectorsAndCompanies();
    logger.logInfo('Dashboard页面已加载', 'dashboard');
  }, []);

  // 搜索过滤
  useEffect(() => {
    filterTreeData();
  }, [searchValue, sectors, companies]);

  // 当选择改变时加载相关内容
  useEffect(() => {
    if (selectedKey) {
      loadRelatedContent();
    }
  }, [selectedKey, selectedType]);

  const loadSectorsAndCompanies = async () => {
    setLoading(true);
    try {
      // 调用API获取行业和公司数据
      const sectorsData = await dashboardAPI.getSectors();
      
      // 转换为本地数据格式
      const transformedSectors: SectorData[] = sectorsData.map(sector => ({
        sector: sector.sector,
        companies: sector.companies
      }));
      
      setSectors(transformedSectors);
      
      // 扁平化公司列表
      const allCompanies = transformedSectors.flatMap(sector => sector.companies);
      setCompanies(allCompanies);
      
      logger.logInfo(`加载了 ${transformedSectors.length} 个行业，${allCompanies.length} 家公司`, 'dashboard');
      
    } catch (error) {
      message.error('加载数据失败');
      logger.logError('加载行业和公司数据失败', 'dashboard', error);
      
      // 如果API失败，使用默认数据保证界面能正常工作
      const fallbackSectors: SectorData[] = [
        {
          sector: '科技',
          companies: [
            { id: 1, companyname: '苹果公司', primaryticker: 'AAPL', sector: '科技', industry: '消费电子' },
            { id: 2, companyname: '微软公司', primaryticker: 'MSFT', sector: '科技', industry: '软件' },
          ]
        }
      ];
      setSectors(fallbackSectors);
      setCompanies(fallbackSectors.flatMap(s => s.companies));
    } finally {
      setLoading(false);
    }
  };

  const filterTreeData = () => {
    const filteredSectors = sectors.filter(sector => {
      if (!searchValue) return true;
      
      const sectorMatch = sector.sector.toLowerCase().includes(searchValue.toLowerCase());
      const companyMatch = sector.companies.some(company => 
        company.companyname.toLowerCase().includes(searchValue.toLowerCase()) ||
        company.primaryticker.toLowerCase().includes(searchValue.toLowerCase()) ||
        company.industry.toLowerCase().includes(searchValue.toLowerCase())
      );
      
      return sectorMatch || companyMatch;
    });

    const treeNodes: DataNode[] = filteredSectors.map(sector => ({
      title: sector.sector,
      key: `sector-${sector.sector}`,
      icon: <StockOutlined />,
      children: sector.companies
        .filter(company => {
          if (!searchValue) return true;
          return company.companyname.toLowerCase().includes(searchValue.toLowerCase()) ||
                 company.primaryticker.toLowerCase().includes(searchValue.toLowerCase()) ||
                 company.industry.toLowerCase().includes(searchValue.toLowerCase());
        })
        .map(company => ({
          title: (
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span>{company.companyname}</span>
              <Tag color="blue" style={{ fontSize: '10px' }}>{company.primaryticker}</Tag>
            </div>
          ),
          key: `company-${company.id}`,
          icon: <TrophyOutlined />,
          isLeaf: true,
        }))
    }));

    setTreeData(treeNodes);
  };

  const handleTreeSelect = (selectedKeys: React.Key[], info: any) => {
    if (selectedKeys.length === 0) {
      setSelectedKey('');
      setSelectedType('');
      setSelectedCompany(null);
      setSelectedSector('');
      return;
    }

    const key = selectedKeys[0] as string;
    setSelectedKey(key);

    if (key.startsWith('sector-')) {
      setSelectedType('sector');
      const sectorName = key.replace('sector-', '');
      setSelectedSector(sectorName);
      setSelectedCompany(null);
      logger.logInfo(`选择了行业: ${sectorName}`, 'dashboard');
    } else if (key.startsWith('company-')) {
      setSelectedType('company');
      const companyId = parseInt(key.replace('company-', ''));
      const company = companies.find(c => c.id === companyId);
      setSelectedCompany(company || null);
      setSelectedSector('');
      logger.logInfo(`选择了公司: ${company?.companyname} (${company?.primaryticker})`, 'dashboard');
    }
  };

  const loadRelatedContent = async () => {
    setContentLoading(true);
    try {
      // 根据选择类型加载不同的内容
      if (selectedType === 'company' && selectedCompany) {
        await Promise.all([
          loadCompanyNews(selectedCompany.primaryticker),
          loadCompanyAnalysis(selectedCompany.primaryticker),
          loadCompanyReports(selectedCompany.primaryticker)
        ]);
      } else if (selectedType === 'sector' && selectedSector) {
        await Promise.all([
          loadSectorNews(selectedSector),
          loadSectorAnalysis(selectedSector),
          loadSectorReports(selectedSector)
        ]);
      }
    } catch (error) {
      message.error('加载相关内容失败');
      logger.logError('加载相关内容失败', 'dashboard', error);
    } finally {
      setContentLoading(false);
    }
  };

  const loadCompanyNews = async (ticker: string) => {
    try {
      const news = await dashboardAPI.getCompanyNews(ticker, { limit: 20 });
      setNewsData(news);
      logger.logInfo(`加载公司新闻: ${ticker}, ${news.length} 条`, 'dashboard');
    } catch (error) {
      logger.logError(`加载公司新闻失败: ${ticker}`, 'dashboard', error);
      setNewsData([]);
    }
  };

  const loadCompanyAnalysis = async (ticker: string) => {
    try {
      const analysis = await dashboardAPI.getCompanyAnalysis(ticker, { limit: 10 });
      setAnalysisData(analysis);
      logger.logInfo(`加载公司AI分析: ${ticker}, ${analysis.length} 条`, 'dashboard');
    } catch (error) {
      logger.logError(`加载公司AI分析失败: ${ticker}`, 'dashboard', error);
      setAnalysisData([]);
    }
  };

  const loadCompanyReports = async (ticker: string) => {
    try {
      const reports = await dashboardAPI.getCompanyReports(ticker, { limit: 10 });
      setReportsData(reports);
      logger.logInfo(`加载公司研报: ${ticker}, ${reports.length} 条`, 'dashboard');
    } catch (error) {
      logger.logError(`加载公司研报失败: ${ticker}`, 'dashboard', error);
      setReportsData([]);
    }
  };

  const loadSectorNews = async (sector: string) => {
    try {
      const news = await dashboardAPI.getSectorNews(sector, { limit: 20 });
      setNewsData(news);
      logger.logInfo(`加载行业新闻: ${sector}, ${news.length} 条`, 'dashboard');
    } catch (error) {
      logger.logError(`加载行业新闻失败: ${sector}`, 'dashboard', error);
      setNewsData([]);
    }
  };

  const loadSectorAnalysis = async (sector: string) => {
    try {
      const analysis = await dashboardAPI.getSectorAnalysis(sector, { limit: 10 });
      setAnalysisData(analysis);
      logger.logInfo(`加载行业AI分析: ${sector}, ${analysis.length} 条`, 'dashboard');
    } catch (error) {
      logger.logError(`加载行业AI分析失败: ${sector}`, 'dashboard', error);
      setAnalysisData([]);
    }
  };

  const loadSectorReports = async (sector: string) => {
    try {
      const reports = await dashboardAPI.getSectorReports(sector, { limit: 10 });
      setReportsData(reports);
      logger.logInfo(`加载行业研报: ${sector}, ${reports.length} 条`, 'dashboard');
    } catch (error) {
      logger.logError(`加载行业研报失败: ${sector}`, 'dashboard', error);
      setReportsData([]);
    }
  };

  const getSentimentColor = (polarity?: number) => {
    if (!polarity) return 'default';
    if (polarity > 0.3) return 'green';
    if (polarity < -0.3) return 'red';
    return 'orange';
  };

  const getSentimentText = (polarity?: number) => {
    if (!polarity) return '中性';
    if (polarity > 0.3) return '积极';
    if (polarity < -0.3) return '消极';
    return '中性';
  };

  const renderNewsTab = () => (
    <div>
      {newsData.length === 0 ? (
        <Empty 
          description="暂无相关新闻" 
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      ) : (
        <List
          itemLayout="vertical"
          dataSource={newsData}
          renderItem={item => (
            <List.Item
              key={item.id}
              extra={
                <Space direction="vertical" align="end">
                  <Tag color={getSentimentColor(item.sentiment_polarity)}>
                    {getSentimentText(item.sentiment_polarity)}
                  </Tag>
                  <Text type="secondary" style={{ fontSize: '12px' }}>
                    <CalendarOutlined /> {item.date}
                  </Text>
                </Space>
              }
            >
              <List.Item.Meta
                title={
                  <a href={item.link} target="_blank" rel="noopener noreferrer">
                    {item.title}
                  </a>
                }
                description={
                  <Space>
                    <Text type="secondary">{item.source}</Text>
                    {item.symbols.map(symbol => (
                      <Tag key={symbol}>{symbol}</Tag>
                    ))}
                  </Space>
                }
              />
              <Paragraph ellipsis={{ rows: 2, expandable: true }}>
                {item.content}
              </Paragraph>
            </List.Item>
          )}
        />
      )}
        </div>
  );

  const renderAnalysisTab = () => (
    <div>
      {analysisData.length === 0 ? (
        <Empty 
          description="暂无AI分析结果" 
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      ) : (
        <List
          dataSource={analysisData}
          renderItem={item => (
            <List.Item key={item.id}>
              <Card 
                size="small" 
                title={
                  <Space>
                    <RobotOutlined />
                    <span>{item.filename}</span>
                  </Space>
                }
                extra={
                  <Space>
                    <Tag color="blue">{item.total_tokens} tokens</Tag>
                    <Tag color="green">
                      <DollarOutlined />{item.cost.toFixed(3)}
                    </Tag>
                  </Space>
                }
              >
                <Paragraph ellipsis={{ rows: 3, expandable: true }}>
                  {item.content}
                </Paragraph>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  <CalendarOutlined /> {new Date(item.created_at).toLocaleString()}
                </Text>
              </Card>
            </List.Item>
          )}
        />
      )}
    </div>
  );

  const renderReportsTab = () => (
    <div>
      {reportsData.length === 0 ? (
        <Empty 
          description="暂无相关研报" 
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      ) : (
        <List
          dataSource={reportsData}
          renderItem={item => (
            <List.Item 
              key={item.id}
              actions={[
                <Button 
                  type="link" 
                  icon={<FileTextOutlined />}
                  onClick={async () => {
                    try {
                      const fileContent = await dashboardAPI.getFileContent(item.id);
                      message.info({
                        content: fileContent.content || '文件内容为空',
                        duration: 10,
                        style: { whiteSpace: 'pre-line', maxWidth: '600px' }
                      });
                      logger.logInfo(`查看原始研报: ${item.filename}`, 'dashboard');
                    } catch (error) {
                      message.error('获取文件内容失败');
                      logger.logError(`获取文件内容失败: ${item.filename}`, 'dashboard', error);
                    }
                  }}
                >
                  查看原文
                </Button>
              ]}
            >
              <List.Item.Meta
                avatar={<Avatar icon={<FileTextOutlined />} />}
                title={item.filename}
                description={
                  <Space>
                    <Tag color="purple">{item.file_type.toUpperCase()}</Tag>
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      <CalendarOutlined /> {new Date(item.created_at).toLocaleString()}
                    </Text>
                  </Space>
                }
              />
            </List.Item>
          )}
        />
      )}
    </div>
  );

  return (
    <div style={{ height: '100%', background: '#f5f5f5' }}>
      <Layout style={{ height: '100%' }}>
        {/* 左侧选择器 */}
        <Sider 
          width={300} 
          style={{ 
            background: '#fff', 
            borderRight: '1px solid #f0f0f0',
            height: '100%',
            overflowY: 'auto'
          }}
        >
          <div style={{ padding: '16px' }}>
            <Title level={4} style={{ marginBottom: '16px' }}>
              行业 & 个股
            </Title>
            
            <Input
              placeholder="搜索行业或公司..."
              prefix={<SearchOutlined />}
              value={searchValue}
              onChange={e => setSearchValue(e.target.value)}
              style={{ marginBottom: '16px' }}
              allowClear
            />
            
            <Spin spinning={loading}>
              <Tree
                showIcon
                selectedKeys={selectedKey ? [selectedKey] : []}
                treeData={treeData}
                onSelect={handleTreeSelect}
                height={600}
                style={{ background: 'transparent' }}
              />
            </Spin>
          </div>
        </Sider>

        {/* 右侧内容区域 */}
        <Content style={{ background: '#fff', margin: '0', height: '100%' }}>
          <div style={{ padding: '24px', height: '100%' }}>
            {selectedKey ? (
              <>
                <div style={{ marginBottom: '24px' }}>
                  <Title level={3}>
                    {selectedType === 'company' 
                      ? `${selectedCompany?.companyname} (${selectedCompany?.primaryticker})`
                      : `${selectedSector} 行业`
                    }
                  </Title>
                  {selectedType === 'company' && selectedCompany && (
                    <Space size="large">
                      <Tag color="blue">{selectedCompany.sector}</Tag>
                      <Tag color="green">{selectedCompany.industry}</Tag>
                    </Space>
                  )}
                </div>

                <Spin spinning={contentLoading}>
                  <Tabs defaultActiveKey="news" size="large">
                    <TabPane 
                      tab={
                        <span>
                          <StockOutlined />
                          相关新闻
                        </span>
                      } 
                      key="news"
                    >
                      {renderNewsTab()}
                    </TabPane>
                    
                    <TabPane 
                      tab={
                        <span>
                          <RobotOutlined />
                          AI分析
                        </span>
                      } 
                      key="analysis"
                    >
                      {renderAnalysisTab()}
                    </TabPane>
                    
                    <TabPane 
                      tab={
                        <span>
                          <FileTextOutlined />
                          原始研报
                        </span>
                      } 
                      key="reports"
                    >
                      {renderReportsTab()}
                    </TabPane>
                  </Tabs>
                </Spin>
              </>
            ) : (
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
                      请在左侧选择一个行业或公司<br />
                      查看相关新闻、AI分析和研报
                    </span>
                  }
                />
              </div>
            )}
      </div>
        </Content>
      </Layout>
    </div>
  );
};

export default DashboardPage; 
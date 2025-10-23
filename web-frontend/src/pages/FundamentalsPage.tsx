import React, { useState, useEffect } from 'react';
import {
  Card,
  Input,
  Button,
  Row,
  Col,
  Typography,
  Spin,
  message,
  Tabs,
  Table,
  Statistic,
  Progress,
  Tag,
  Space,
  Divider,
  Empty,
  Select,
  List,
  AutoComplete,
  Modal,
  Form,
  DatePicker
} from 'antd';
import {
  SearchOutlined,
  ReloadOutlined,
  DollarOutlined,
  LineChartOutlined,
  BankOutlined,
  BuildOutlined,
  InfoCircleOutlined,
  BarChartOutlined,
  CalculatorOutlined,
  StarOutlined,
  DashboardOutlined,
  HeartOutlined,
  TableOutlined,
  MoneyCollectOutlined,
  FundOutlined,
  TeamOutlined,
  BankOutlined as InstitutionOutlined,
  FundOutlined as FundIcon,
  DownloadOutlined,
  SwapOutlined,
  CalendarOutlined,
  FileOutlined,
  SettingOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import { fundamentalsAPI, fundamentalsUtils, CompanyFundamentalsResponse, CompanyOfficer, OutstandingShares, InsiderTransaction, CompanyFundamentalsUpdateResponse, FinancialStatementsExportRequest } from '../services/fundamentals';
import { dashboardAPI, type StockSearchResult } from '../services/dashboard';
import { getErrorMessage } from '../utils/errorHandler';

const { Title, Text, Paragraph } = Typography;
const { TabPane } = Tabs;

interface FundamentalsPageState {
  loading: boolean;
  searchQuery: string;
  fundamentalsData: CompanyFundamentalsResponse | null;
  updateResult: CompanyFundamentalsUpdateResponse | null;
  isUpdating: boolean;
  autoUpdateEnabled: boolean;
  isAdmin: boolean;
  // 模糊搜索相关状态
  searchResults: StockSearchResult[];
  searchLoading: boolean;
  showSearchResults: boolean;
}

const FundamentalsPage: React.FC = () => {
  // const { user, isAuthenticated } = useAuthStore(); // Temporarily commented out unused variables
  const [state, setState] = useState<FundamentalsPageState>({
    loading: false,
    searchQuery: '',
    fundamentalsData: null,
    updateResult: null,
    isUpdating: false,
    autoUpdateEnabled: true, // 默认启用自动更新
    isAdmin: false, // 需要从用户认证系统获取
    // 模糊搜索相关状态
    searchResults: [],
    searchLoading: false,
    showSearchResults: false
  });

  // Export dialog state
  const [exportDialogVisible, setExportDialogVisible] = useState(false);
  const [exportLoading, setExportLoading] = useState(false);
  const [form] = Form.useForm();

  // Check user permissions on component mount
  useEffect(() => {
    const checkUserPermissions = () => {
      try {
        // 检查多种可能的用户信息存储位置
        let userInfo = null;
        
        // 1. 检查localStorage中的userInfo
        const userInfoStr = localStorage.getItem('userInfo');
        if (userInfoStr) {
          userInfo = JSON.parse(userInfoStr);
          console.log('Found userInfo in localStorage:', userInfo);
        }
        
        // 2. 检查localStorage中的user
        const userStr = localStorage.getItem('user');
        if (userStr && !userInfo) {
          userInfo = JSON.parse(userStr);
          console.log('Found user in localStorage:', userInfo);
        }
        
        // 3. 检查localStorage中的authToken（可能包含用户信息）
        const authToken = localStorage.getItem('authToken');
        if (authToken && !userInfo) {
          try {
            // 尝试解析JWT token的payload
            const payload = JSON.parse(atob(authToken.split('.')[1]));
            userInfo = payload;
            console.log('Found user info in authToken:', userInfo);
          } catch (e) {
            console.log('Could not parse authToken');
          }
        }
        
        // 4. 检查sessionStorage
        const sessionUserInfo = sessionStorage.getItem('userInfo');
        if (sessionUserInfo && !userInfo) {
          userInfo = JSON.parse(sessionUserInfo);
          console.log('Found userInfo in sessionStorage:', userInfo);
        }
        
        if (userInfo) {
          // 检查多种可能的admin标识
          const isAdmin = userInfo.role === 'admin' || 
                         userInfo.isAdmin === true || 
                         userInfo.is_admin === true ||
                         userInfo.userRole === 'admin' ||
                         userInfo.permissions?.includes('admin') ||
                         userInfo.role === 'ADMIN' ||
                         userInfo.isAdmin === 'true';
          
          console.log('User admin status:', isAdmin, 'User info:', userInfo);
          setState(prev => ({ ...prev, isAdmin }));
        } else {
          console.log('No user info found, setting as non-admin');
          setState(prev => ({ ...prev, isAdmin: false }));
        }
      } catch (error) {
        console.error('Failed to check user permissions:', error);
        setState(prev => ({ ...prev, isAdmin: false }));
      }
    };

    checkUserPermissions();
  }, []);

  // 搜索股票（用于模糊搜索）
  const searchStocks = async (query: string) => {
    if (!query.trim()) {
      setState(prev => ({ 
        ...prev, 
        searchResults: [], 
        showSearchResults: false 
      }));
      return;
    }

    setState(prev => ({ ...prev, searchLoading: true }));
    try {
      const results = await dashboardAPI.searchStocks(query, 10);
      setState(prev => ({ 
        ...prev, 
        searchResults: results, 
        showSearchResults: true,
        searchLoading: false 
      }));
    } catch (error) {
      console.error('搜索股票失败:', error);
      setState(prev => ({ 
        ...prev, 
        searchResults: [], 
        showSearchResults: false,
        searchLoading: false 
      }));
    }
  };

  // 防抖搜索
  useEffect(() => {
    const timer = setTimeout(() => {
      searchStocks(state.searchQuery);
    }, 300);

    return () => clearTimeout(timer);
  }, [state.searchQuery]);

  // 选择股票
  const handleSelectStock = (stock: StockSearchResult) => {
    setState(prev => ({ 
      ...prev, 
      searchQuery: stock.ticker,
      showSearchResults: false 
    }));
  };

  // 处理输入框变化
  const handleInputChange = (value: string) => {
    setState(prev => ({ ...prev, searchQuery: value }));
  };

  // 处理输入框焦点
  const handleInputFocus = () => {
    if (state.searchResults.length > 0) {
      setState(prev => ({ ...prev, showSearchResults: true }));
    }
  };

  // 处理输入框失焦
  const handleInputBlur = () => {
    // 延迟隐藏搜索结果，以便点击事件能够触发
    setTimeout(() => {
      setState(prev => ({ ...prev, showSearchResults: false }));
    }, 200);
  };

  // Execute search with optional data update
  const handleSearch = async () => {
    if (!state.searchQuery.trim()) {
      message.warning('Please enter a stock code or company name');
      return;
    }

    setState(prev => ({ ...prev, loading: true, isUpdating: state.autoUpdateEnabled, updateResult: null, showSearchResults: false }));
    
    try {
      let searchResult: CompanyFundamentalsResponse;
      let updateResult: CompanyFundamentalsUpdateResponse | null = null;

      if (state.autoUpdateEnabled) {
        // 启用自动更新：使用搜索并更新API
        const result = await fundamentalsAPI.searchAndUpdateCompany(
          state.searchQuery.trim(),
          false
        );
        searchResult = result.searchResult;
        updateResult = result.updateResult;

        // 显示更新结果消息
        if (updateResult.success) {
          const updatedComponents = updateResult.data_updated ? 
            Object.entries(updateResult.data_updated).filter(([, updated]) => updated).map(([key]) => key) : [];
          
          if (updatedComponents.length > 0) {
            message.success(
              `Successfully updated ${updateResult.company_info?.name || state.searchQuery} data. ` +
              `Updated ${updatedComponents.length} components: ${updatedComponents.join(', ')}`
            );
          } else {
            message.info(updateResult.message);
          }
        } else {
          message.warning(`Data update: ${updateResult.message}`);
        }
      } else {
        // 禁用自动更新：仅搜索数据库数据
        searchResult = await fundamentalsAPI.searchCompany(state.searchQuery.trim());
        message.info('Search completed (auto-update disabled)');
      }

      setState(prev => ({
        ...prev,
        fundamentalsData: searchResult,
        updateResult: updateResult,
        loading: false,
        isUpdating: false
      }));
    } catch (error) {
      console.error('Failed to query fundamental information:', error);
      getErrorMessage(error);
      
      // Try fallback to search only if update fails
      try {
        setState(prev => ({ ...prev, isUpdating: false }));
        if (state.autoUpdateEnabled) {
          message.warning('Data update failed, falling back to search only');
        }
        
        const data = await fundamentalsAPI.searchCompany(state.searchQuery.trim());
        setState(prev => ({
          ...prev,
          fundamentalsData: data,
          loading: false
        }));
        
        message.info('Search completed');
      } catch (searchError) {
        console.error('Search fallback also failed:', searchError);
        const searchErrorMessage = getErrorMessage(searchError);
        
        // Avoid duplicate "no data found" messages
        if (searchErrorMessage.includes('no data found')) {
          message.error(searchErrorMessage);
        } else {
          message.error(`Query failed: ${searchErrorMessage}`);
        }
        setState(prev => ({ ...prev, loading: false, isUpdating: false }));
      }
    }
  };


  // Toggle auto-update setting
  const handleToggleAutoUpdate = () => {
    setState(prev => ({
      ...prev,
      autoUpdateEnabled: !prev.autoUpdateEnabled
    }));
    message.info(`Auto-update ${!state.autoUpdateEnabled ? 'enabled' : 'disabled'}`);
  };

  // Clear results
  const handleClear = () => {
    setState(prev => ({
      ...prev,
      searchQuery: '',
      fundamentalsData: null,
      updateResult: null,
      isUpdating: false
    }));
  };

  // 导出股东信息数据
  const exportHoldingsData = (data: any[], dataType: string) => {
    if (!data || data.length === 0) {
      message.warning('No data to export');
      return;
    }

    try {
      // 准备CSV数据
      const headers = [
        'Institution/Fund Name',
        'Date',
        'Shares Held',
        'Ownership %',
        'Change in Shares',
        'Change %'
      ];

      const csvData = data.map(item => [
        item.name || '',
        fundamentalsUtils.formatDate(item.date),
        item.currentshares ? fundamentalsUtils.formatNumber(item.currentshares, 0) : 'N/A',
        item.totalshares ? `${item.totalshares.toFixed(2)}%` : 'N/A',
        item.change !== null && item.change !== undefined ? 
          `${item.change >= 0 ? '+' : ''}${fundamentalsUtils.formatNumber(item.change, 0)}` : 'N/A',
        item.change_p !== null && item.change_p !== undefined ? 
          `${item.change_p >= 0 ? '+' : ''}${item.change_p.toFixed(2)}%` : 'N/A'
      ]);

      // 创建CSV内容
      const csvContent = [
        headers.join(','),
        ...csvData.map(row => row.map(cell => `"${cell}"`).join(','))
      ].join('\n');

      // 创建并下载文件
      const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8;' });
      const link = document.createElement('a');
      const url = URL.createObjectURL(blob);
      link.setAttribute('href', url);
      
      const companyName = state.fundamentalsData?.company?.name || 'Company';
      const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-');
      link.setAttribute('download', `${companyName}_${dataType}_${timestamp}.csv`);
      
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      message.success(`${dataType} data exported successfully`);
    } catch (error) {
      console.error('Export failed:', error);
      message.error('Export failed, please try again');
    }
  };

  // 导出高管信息数据
  const exportOfficersData = (data: CompanyOfficer[]) => {
    if (!data || data.length === 0) {
      message.warning('No officers data to export');
      return;
    }

    try {
      // 准备CSV数据
      const headers = [
        'Name',
        'Title',
        'Year Born'
      ];

      const csvData = data.map(item => [
        item.name || '-',
        item.title || '-',
        item.yearborn || '-'
      ]);

      // 创建CSV内容
      const csvContent = [
        headers.join(','),
        ...csvData.map(row => row.map(cell => `"${cell}"`).join(','))
      ].join('\n');

      // 创建并下载文件
      const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8;' });
      const link = document.createElement('a');
      const url = URL.createObjectURL(blob);
      link.setAttribute('href', url);
      
      const companyName = state.fundamentalsData?.company?.name || 'Company';
      const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-');
      link.setAttribute('download', `${companyName}_Officers_${timestamp}.csv`);
      
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      message.success('Officers data exported successfully');
    } catch (error) {
      console.error('Export failed:', error);
      message.error('Export failed, please try again');
    }
  };

  // 导出流通股数据
  const exportOutstandingSharesData = (data: OutstandingShares[], type?: string) => {
    if (!data || data.length === 0) {
      message.warning('No outstanding shares data to export');
      return;
    }

    try {
      // 准备CSV数据
      const headers = [
        'Date',
        'Formatted Date',
        'Shares (Million)',
        'Total Shares',
        'Type'
      ];

      const csvData = data.map(item => [
        item.date || '-',
        item.dateformatted || '-',
        item.sharesmln || '-',
        item.shares ? item.shares.toLocaleString() : '-',
        item.type || '-'
      ]);

      // 创建CSV内容
      const csvContent = [
        headers.join(','),
        ...csvData.map(row => row.map(cell => `"${cell}"`).join(','))
      ].join('\n');

      // 创建并下载文件
      const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8;' });
      const link = document.createElement('a');
      const url = URL.createObjectURL(blob);
      link.setAttribute('href', url);
      
      const companyName = state.fundamentalsData?.company?.name || 'Company';
      const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-');
      const filename = type ? `${companyName}_OutstandingShares_${type}_${timestamp}.csv` : `${companyName}_OutstandingShares_${timestamp}.csv`;
      link.setAttribute('download', filename);
      
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      message.success(`Outstanding shares ${type ? type + ' ' : ''}data exported successfully`);
    } catch (error) {
      console.error('Export failed:', error);
      message.error('Export failed, please try again');
    }
  };

  // 渲染股东信息数据
  const renderHoldingsData = () => {
    if (!state.fundamentalsData) return null;
    
    const { institutional_holdings } = state.fundamentalsData;
    
    if (!institutional_holdings || institutional_holdings.length === 0) {
      return (
        <Empty
          description="No holdings data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    // 按持股类型分组
    const institutions = institutional_holdings.filter((h: any) => h.holder_type === 'Institution');
    const funds = institutional_holdings.filter((h: any) => h.holder_type === 'Fund');

    return (
      <div>
        <Tabs defaultActiveKey="institutions">
          <TabPane tab={<span><InstitutionOutlined /> Institutional Holdings</span>} key="institutions">
            {renderInstitutionsTable(institutions)}
          </TabPane>
          <TabPane tab={<span><FundIcon /> Fund Holdings</span>} key="funds">
            {renderFundsTable(funds)}
          </TabPane>
        </Tabs>
      </div>
    );
  };

  // 渲染机构持股表格
  const renderInstitutionsTable = (institutions: any[]) => {
    if (!institutions || institutions.length === 0) {
      return (
        <Empty
          description="No institutional holdings data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    const columns: ColumnsType<any> = [
      {
        title: 'Institution Name',
        dataIndex: 'name',
        key: 'name',
        width: 200,
        ellipsis: true,
      },
      {
        title: 'Date',
        dataIndex: 'date',
        key: 'date',
        width: 120,
        render: (date: string) => fundamentalsUtils.formatDate(date),
      },
      {
        title: 'Shares Held',
        dataIndex: 'currentshares',
        key: 'currentshares',
        width: 150,
        align: 'right',
        render: (value: number) => value ? fundamentalsUtils.formatNumber(value, 0) : '-',
      },
      {
        title: 'Ownership %',
        dataIndex: 'totalshares',
        key: 'totalshares',
        width: 120,
        align: 'right',
        render: (value: number) => value ? `${value.toFixed(2)}%` : '-',
      },
      {
        title: 'Change in Shares',
        dataIndex: 'change',
        key: 'change',
        width: 150,
        align: 'right',
        render: (value: number) => {
          if (value === null || value === undefined) return '-';
          const formatted = fundamentalsUtils.formatNumber(value, 0);
          return (
            <span style={{ color: value >= 0 ? '#52c41a' : '#ff4d4f' }}>
              {value >= 0 ? '+' : ''}{formatted}
            </span>
          );
        },
      },
      {
        title: 'Change %',
        dataIndex: 'change_p',
        key: 'change_p',
        width: 100,
        align: 'right',
        render: (value: number) => {
          if (value === null || value === undefined) return '-';
          return (
            <span style={{ color: value >= 0 ? '#52c41a' : '#ff4d4f' }}>
              {value >= 0 ? '+' : ''}{value.toFixed(2)}%
            </span>
          );
        },
      },
    ];

    return (
      <div>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Text strong style={{ fontSize: '16px' }}>
            <InstitutionOutlined /> Institutional Holdings
          </Text>
          <Space>
            <Text type="secondary">
              {institutions.length} records
            </Text>
            <Button 
              type="primary" 
              icon={<DownloadOutlined />} 
              size="small"
              onClick={() => exportHoldingsData(institutions, 'Institutional Holdings')}
            >
              Export Data
            </Button>
          </Space>
        </div>
        <Table
          columns={columns}
          dataSource={institutions}
          rowKey="id"
          pagination={{
            pageSize: 20,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} records`,
          }}
          scroll={{ x: 800 }}
          size="small"
        />
      </div>
    );
  };

  // 渲染基金持股表格
  const renderFundsTable = (funds: any[]) => {
    if (!funds || funds.length === 0) {
      return (
        <Empty
          description="No fund holdings data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    const columns: ColumnsType<any> = [
      {
        title: 'Fund Name',
        dataIndex: 'name',
        key: 'name',
        width: 200,
        ellipsis: true,
      },
      {
        title: 'Date',
        dataIndex: 'date',
        key: 'date',
        width: 120,
        render: (date: string) => fundamentalsUtils.formatDate(date),
      },
      {
        title: 'Shares Held',
        dataIndex: 'currentshares',
        key: 'currentshares',
        width: 150,
        align: 'right',
        render: (value: number) => value ? fundamentalsUtils.formatNumber(value, 0) : '-',
      },
      {
        title: 'Ownership %',
        dataIndex: 'totalshares',
        key: 'totalshares',
        width: 120,
        align: 'right',
        render: (value: number) => value ? `${value.toFixed(2)}%` : '-',
      },
      {
        title: 'Change in Shares',
        dataIndex: 'change',
        key: 'change',
        width: 150,
        align: 'right',
        render: (value: number) => {
          if (value === null || value === undefined) return '-';
          const formatted = fundamentalsUtils.formatNumber(value, 0);
          return (
            <span style={{ color: value >= 0 ? '#52c41a' : '#ff4d4f' }}>
              {value >= 0 ? '+' : ''}{formatted}
            </span>
          );
        },
      },
      {
        title: 'Change %',
        dataIndex: 'change_p',
        key: 'change_p',
        width: 100,
        align: 'right',
        render: (value: number) => {
          if (value === null || value === undefined) return '-';
          return (
            <span style={{ color: value >= 0 ? '#52c41a' : '#ff4d4f' }}>
              {value >= 0 ? '+' : ''}{value.toFixed(2)}%
            </span>
          );
        },
      },
    ];

    return (
      <div>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Text strong style={{ fontSize: '16px' }}>
            <FundIcon /> Fund Holdings
          </Text>
          <Space>
            <Text type="secondary">
              {funds.length} records
            </Text>
            <Button 
              type="primary" 
              icon={<DownloadOutlined />} 
              size="small"
              onClick={() => exportHoldingsData(funds, 'Fund Holdings')}
            >
              Export Data
            </Button>
          </Space>
        </div>
        <Table
          columns={columns}
          dataSource={funds}
          rowKey="id"
          pagination={{
            pageSize: 20,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} records`,
          }}
          scroll={{ x: 800 }}
          size="small"
        />
      </div>
    );
  };

  // 渲染高管信息数据
  const renderOfficersData = () => {
    if (!state.fundamentalsData) return null;
    
    const { officers } = state.fundamentalsData;
    
    if (!officers || officers.length === 0) {
      return (
        <Empty
          description="No officers data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    const columns: ColumnsType<CompanyOfficer> = [
      {
        title: 'Name',
        dataIndex: 'name',
        key: 'name',
        width: 200,
        ellipsis: true,
        render: (name: string) => (
          <Text strong>{name || '-'}</Text>
        ),
      },
      {
        title: 'Title',
        dataIndex: 'title',
        key: 'title',
        width: 300,
        ellipsis: true,
        render: (title: string) => (
          <Tag color="blue">{title || '-'}</Tag>
        ),
      },
      {
        title: 'Year Born',
        dataIndex: 'yearborn',
        key: 'yearborn',
        width: 120,
        align: 'center',
        render: (yearborn: string) => {
          if (!yearborn || yearborn === '-') return '-';
          return (
            <Text type="secondary">{yearborn}</Text>
          );
        },
      },
    ];

    return (
      <div>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Text strong style={{ fontSize: '16px' }}>
            <TeamOutlined /> Company Officers
          </Text>
          <Space>
            <Text type="secondary">
              {officers.length} officers
            </Text>
            <Button 
              type="primary" 
              icon={<DownloadOutlined />} 
              size="small"
              onClick={() => exportOfficersData(officers)}
            >
              Export Data
            </Button>
          </Space>
        </div>
        <Table
          columns={columns}
          dataSource={officers}
          rowKey="id"
          pagination={{
            pageSize: 20,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} officers`,
          }}
          scroll={{ x: 600 }}
          size="small"
        />
      </div>
    );
  };

  // 渲染内部交易数据
  const renderInsiderTransactionsData = () => {
    if (!state.fundamentalsData) return null;
    
    const { insider_transactions } = state.fundamentalsData;
    
    if (!insider_transactions || insider_transactions.length === 0) {
      return (
        <Empty
          description="No insider transactions data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    const columns: ColumnsType<InsiderTransaction> = [
      {
        title: 'Insider Name',
        dataIndex: 'ownername',
        key: 'ownername',
        width: 200,
        ellipsis: true,
        render: (name: string) => (
          <Text strong>{name || '-'}</Text>
        ),
      },
      {
        title: 'Transaction Date',
        dataIndex: 'transactiondate',
        key: 'transactiondate',
        width: 120,
        align: 'center',
        render: (date: string) => {
          if (!date) return '-';
          return (
            <Text type="secondary">{fundamentalsUtils.formatDate(date)}</Text>
          );
        },
      },
      {
        title: 'Transaction Code',
        dataIndex: 'transactioncode',
        key: 'transactioncode',
        width: 100,
        align: 'center',
        render: (code: string) => {
          if (!code) return '-';
          return (
            <Tag color="blue">{code}</Tag>
          );
        },
      },
      {
        title: 'Amount',
        dataIndex: 'transactionamount',
        key: 'transactionamount',
        width: 120,
        align: 'right',
        render: (amount: number) => {
          if (!amount) return '-';
          return (
            <Text strong style={{ color: '#1890ff' }}>
              {fundamentalsUtils.formatNumber(amount, 0)}
            </Text>
          );
        },
      },
      {
        title: 'Price',
        dataIndex: 'transactionprice',
        key: 'transactionprice',
        width: 100,
        align: 'right',
        render: (price: number) => {
          if (!price) return '-';
          return (
            <Text strong style={{ color: '#52c41a' }}>
              {fundamentalsUtils.formatCurrency(price, state.fundamentalsData?.company?.currencycode || 'USD')}
            </Text>
          );
        },
      },
      {
        title: 'Type',
        dataIndex: 'transactionacquireddisposed',
        key: 'transactionacquireddisposed',
        width: 100,
        align: 'center',
        render: (type: string) => {
          if (!type) return '-';
          const color = type === 'A' ? '#52c41a' : '#ff4d4f';
          const text = type === 'A' ? 'Acquired' : 'Disposed';
          return (
            <Tag color={color}>{text}</Tag>
          );
        },
      },
      {
        title: 'Post Amount',
        dataIndex: 'posttransactionamount',
        key: 'posttransactionamount',
        width: 120,
        align: 'right',
        render: (amount: number) => {
          if (!amount) return '-';
          return (
            <Text type="secondary">
              {fundamentalsUtils.formatNumber(amount, 0)}
            </Text>
          );
        },
      },
      {
        title: 'SEC Link',
        dataIndex: 'seclink',
        key: 'seclink',
        width: 100,
        align: 'center',
        render: (link: string) => {
          if (!link) return '-';
          return (
            <Button 
              type="link" 
              size="small"
              onClick={() => window.open(link, '_blank')}
            >
              View
            </Button>
          );
        },
      },
    ];

    return (
      <div>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Text strong style={{ fontSize: '16px' }}>
            <SwapOutlined /> Insider Transactions
          </Text>
          <Space>
            <Text type="secondary">
              {insider_transactions.length} transactions
            </Text>
            <Button 
              type="primary" 
              icon={<DownloadOutlined />} 
              size="small"
              onClick={() => exportInsiderTransactionsData(insider_transactions)}
            >
              Export Data
            </Button>
          </Space>
        </div>
        <Table
          columns={columns}
          dataSource={insider_transactions}
          rowKey="id"
          pagination={{
            pageSize: 20,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} transactions`,
          }}
          scroll={{ x: 1000 }}
          size="small"
        />
      </div>
    );
  };

  // 渲染流通股数据
  const renderOutstandingSharesData = () => {
    if (!state.fundamentalsData) return null;

    const { outstanding_shares } = state.fundamentalsData;
    
    if (!outstanding_shares || outstanding_shares.length === 0) {
      return (
        <Empty
          description="No outstanding shares data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    // 分离年度和季度数据
    const annualData = outstanding_shares.filter(item => item.type === 'annual');
    const quarterlyData = outstanding_shares.filter(item => item.type === 'quarterly');

    return (
      <div>
        <Tabs defaultActiveKey="annual">
          <TabPane tab={<span><CalendarOutlined /> Annual Shares</span>} key="annual">
            {renderOutstandingSharesAnnualTable(annualData)}
          </TabPane>
          <TabPane tab={<span><BarChartOutlined /> Quarterly Shares</span>} key="quarterly">
            {renderOutstandingSharesQuarterlyTable(quarterlyData)}
          </TabPane>
        </Tabs>
      </div>
    );
  };

  // 渲染年度流通股数据表格
  const renderOutstandingSharesAnnualTable = (data: OutstandingShares[]) => {
    if (!data || data.length === 0) {
      return (
        <Empty
          description="No annual outstanding shares data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    const columns: ColumnsType<OutstandingShares> = [
      {
        title: 'Year',
        dataIndex: 'date',
        key: 'date',
        width: 100,
        align: 'center',
        render: (date: string) => (
          <Text strong style={{ fontSize: '14px' }}>{date || '-'}</Text>
        ),
      },
      {
        title: 'Date',
        dataIndex: 'dateformatted',
        key: 'dateformatted',
        width: 120,
        align: 'center',
        render: (dateformatted: string) => {
          if (!dateformatted || dateformatted === '-') return '-';
          return (
            <Text type="secondary">{fundamentalsUtils.formatDate(dateformatted)}</Text>
          );
        },
      },
      {
        title: 'Shares (Million)',
        dataIndex: 'sharesmln',
        key: 'sharesmln',
        width: 150,
        align: 'right',
        render: (sharesmln: string) => {
          if (!sharesmln || sharesmln === '-') return '-';
          return (
            <Text strong style={{ color: '#1890ff', fontSize: '14px' }}>
              {parseFloat(sharesmln).toLocaleString()}
            </Text>
          );
        },
      },
      {
        title: 'Total Shares',
        dataIndex: 'shares',
        key: 'shares',
        width: 180,
        align: 'right',
        render: (shares: number) => {
          if (!shares || shares === 0) return '-';
          return (
            <Text strong style={{ color: '#52c41a', fontSize: '14px' }}>
              {shares.toLocaleString()}
            </Text>
          );
        },
      },
    ];

    return (
      <div>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Text strong style={{ fontSize: '16px' }}>
            <CalendarOutlined /> Annual Outstanding Shares
          </Text>
          <Space>
            <Text type="secondary">
              {data.length} annual records
            </Text>
            <Button 
              type="primary" 
              icon={<DownloadOutlined />} 
              size="small"
              onClick={() => exportOutstandingSharesData(data, 'annual')}
            >
              Export Annual Data
            </Button>
          </Space>
        </div>
        <Table
          columns={columns}
          dataSource={data}
          rowKey="id"
          pagination={{
            pageSize: 10,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} records`,
          }}
          scroll={{ x: 600 }}
        />
      </div>
    );
  };

  // 渲染季度流通股数据表格
  const renderOutstandingSharesQuarterlyTable = (data: OutstandingShares[]) => {
    if (!data || data.length === 0) {
      return (
        <Empty
          description="No quarterly outstanding shares data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    const columns: ColumnsType<OutstandingShares> = [
      {
        title: 'Quarter',
        dataIndex: 'date',
        key: 'date',
        width: 120,
        align: 'center',
        render: (date: string) => (
          <Text strong style={{ fontSize: '14px' }}>{date || '-'}</Text>
        ),
      },
      {
        title: 'Date',
        dataIndex: 'dateformatted',
        key: 'dateformatted',
        width: 120,
        align: 'center',
        render: (dateformatted: string) => {
          if (!dateformatted || dateformatted === '-') return '-';
          return (
            <Text type="secondary">{fundamentalsUtils.formatDate(dateformatted)}</Text>
          );
        },
      },
      {
        title: 'Shares (Million)',
        dataIndex: 'sharesmln',
        key: 'sharesmln',
        width: 150,
        align: 'right',
        render: (sharesmln: string) => {
          if (!sharesmln || sharesmln === '-') return '-';
          return (
            <Text strong style={{ color: '#1890ff', fontSize: '14px' }}>
              {parseFloat(sharesmln).toLocaleString()}
            </Text>
          );
        },
      },
      {
        title: 'Total Shares',
        dataIndex: 'shares',
        key: 'shares',
        width: 180,
        align: 'right',
        render: (shares: number) => {
          if (!shares || shares === 0) return '-';
          return (
            <Text strong style={{ color: '#52c41a', fontSize: '14px' }}>
              {shares.toLocaleString()}
            </Text>
          );
        },
      },
    ];

    return (
      <div>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Text strong style={{ fontSize: '16px' }}>
            <BarChartOutlined /> Quarterly Outstanding Shares
          </Text>
          <Space>
            <Text type="secondary">
              {data.length} quarterly records
            </Text>
            <Button 
              type="primary" 
              icon={<DownloadOutlined />} 
              size="small"
              onClick={() => exportOutstandingSharesData(data, 'quarterly')}
            >
              Export Quarterly Data
            </Button>
          </Space>
        </div>
        <Table
          columns={columns}
          dataSource={data}
          rowKey="id"
          pagination={{
            pageSize: 20,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} records`,
          }}
          scroll={{ x: 600 }}
        />
      </div>
    );
  };

  // 渲染分红数据
  const renderDividendsData = () => {
    if (!state.fundamentalsData) return null;
    
    const { dividend_by_year, dividend_info } = state.fundamentalsData;
    
    if ((!dividend_by_year || dividend_by_year.length === 0) && 
        (!dividend_info || dividend_info.length === 0)) {
      return (
        <Empty
          description="No dividend data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    return (
      <div>
        {/* Dividend Info 卡片展示 */}
        {dividend_info && dividend_info.length > 0 && (
          <Card 
            title={
              <span>
                <InfoCircleOutlined /> Dividend Information
              </span>
            }
            style={{ marginBottom: 16 }}
          >
            {renderDividendInfoCards(dividend_info)}
          </Card>
        )}
        
        {/* Dividend by Year 表格展示 */}
        {dividend_by_year && dividend_by_year.length > 0 && (
          <Card 
            title={
              <span>
                <MoneyCollectOutlined /> Dividend Distribution by Year
              </span>
            }
          >
            {renderDividendByYearTable(dividend_by_year)}
          </Card>
        )}
      </div>
    );
  };

  // 渲染分红信息卡片
  const renderDividendInfoCards = (data: any[]) => {
    if (!data || data.length === 0) {
      return (
        <Empty
          description="No dividend info data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    // 取最新的数据
    const latestData = data[0];

    return (
      <div>
        <Row gutter={[16, 16]}>
          <Col xs={24} sm={12} md={8} lg={6}>
            <Card size="small" style={{ textAlign: 'center' }}>
              <Statistic
                title="Forward Annual Dividend Rate"
                value={latestData.forwardannualdividendrate || 0}
                precision={4}
                prefix="$"
                valueStyle={{ color: '#1890ff' }}
              />
            </Card>
          </Col>
          <Col xs={24} sm={12} md={8} lg={6}>
            <Card size="small" style={{ textAlign: 'center' }}>
              <Statistic
                title="Forward Annual Dividend Yield"
                value={latestData.forwardannualdividendyield ? latestData.forwardannualdividendyield * 100 : 0}
                precision={2}
                suffix="%"
                valueStyle={{ color: '#52c41a' }}
              />
            </Card>
          </Col>
          <Col xs={24} sm={12} md={8} lg={6}>
            <Card size="small" style={{ textAlign: 'center' }}>
              <Statistic
                title="Payout Ratio"
                value={latestData.payoutratio ? latestData.payoutratio * 100 : 0}
                precision={2}
                suffix="%"
                valueStyle={{ color: '#faad14' }}
              />
            </Card>
          </Col>
          <Col xs={24} sm={12} md={8} lg={6}>
            <Card size="small" style={{ textAlign: 'center' }}>
              <Statistic
                title="Last Split Factor"
                value={latestData.lastsplitfactor || 'N/A'}
                valueStyle={{ color: '#722ed1' }}
              />
            </Card>
          </Col>
        </Row>
        
        <Divider />
        
        <Row gutter={[16, 16]}>
          <Col xs={24} sm={12} md={6}>
            <div style={{ textAlign: 'center' }}>
              <Text type="secondary">Dividend Date</Text>
              <br />
              <Text strong>
                {latestData.dividenddate ? fundamentalsUtils.formatDate(latestData.dividenddate) : 'N/A'}
              </Text>
            </div>
          </Col>
          <Col xs={24} sm={12} md={6}>
            <div style={{ textAlign: 'center' }}>
              <Text type="secondary">Ex-Dividend Date</Text>
              <br />
              <Text strong>
                {latestData.exdividenddate ? fundamentalsUtils.formatDate(latestData.exdividenddate) : 'N/A'}
              </Text>
            </div>
          </Col>
          <Col xs={24} sm={12} md={6}>
            <div style={{ textAlign: 'center' }}>
              <Text type="secondary">Last Split Date</Text>
              <br />
              <Text strong>
                {latestData.lastsplitdate ? fundamentalsUtils.formatDate(latestData.lastsplitdate) : 'N/A'}
              </Text>
            </div>
          </Col>
          <Col xs={24} sm={12} md={6}>
            <div style={{ textAlign: 'center' }}>
              <Text type="secondary">Data Date</Text>
              <br />
              <Text strong>
                {latestData.data_date ? fundamentalsUtils.formatDate(latestData.data_date) : 'N/A'}
              </Text>
            </div>
          </Col>
        </Row>
      </div>
    );
  };

  // 渲染年度分红统计表格
  const renderDividendByYearTable = (data: any[]) => {
    if (!data || data.length === 0) {
      return (
        <Empty
          description="No dividend by year data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    const columns: ColumnsType<any> = [
      {
        title: 'Year',
        dataIndex: 'year',
        key: 'year',
        width: 100,
        align: 'center',
        render: (year: number) => (
          <Text strong style={{ fontSize: '14px' }}>{year || '-'}</Text>
        ),
      },
      {
        title: 'Dividend Count',
        dataIndex: 'count',
        key: 'count',
        width: 150,
        align: 'center',
        render: (count: number) => {
          if (!count || count === 0) return '-';
          return (
            <Text strong style={{ color: '#52c41a', fontSize: '14px' }}>
              {count}
            </Text>
          );
        },
      },
    ];

    return (
      <div>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Space>
            <Text type="secondary">
              {data.length} years
            </Text>
            <Button 
              type="primary" 
              icon={<DownloadOutlined />} 
              size="small"
              onClick={() => exportDividendData(data, 'by_year')}
            >
              Export Data
            </Button>
          </Space>
        </div>
        <Table
          columns={columns}
          dataSource={data}
          rowKey="id"
          pagination={{
            pageSize: 10,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} records`,
          }}
          scroll={{ x: 400 }}
        />
      </div>
    );
  };

  // 导出分红数据
  const exportDividendData = (data: any[], dataType: string) => {
    if (!data || data.length === 0) {
      message.warning('No data to export');
      return;
    }

    try {
      let headers: string[] = [];
      let csvContent = '';

      if (dataType === 'by_year') {
        headers = ['Year', 'Dividend Count'];
        csvContent = headers.join(',') + '\n';
        csvContent += data.map(item => [
          item.year || '',
          item.count || ''
        ].join(',')).join('\n');
      } else if (dataType === 'info') {
        headers = [
          'Forward Annual Dividend Rate',
          'Forward Annual Dividend Yield',
          'Payout Ratio',
          'Dividend Date',
          'Ex-Dividend Date',
          'Last Split Factor',
          'Last Split Date',
          'Data Date'
        ];
        csvContent = headers.join(',') + '\n';
        csvContent += data.map(item => [
          item.forwardannualdividendrate || '',
          item.forwardannualdividendyield || '',
          item.payoutratio || '',
          item.dividenddate || '',
          item.exdividenddate || '',
          item.lastsplitfactor || '',
          item.lastsplitdate || '',
          item.data_date || ''
        ].join(',')).join('\n');
      }

      const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
      const link = document.createElement('a');
      const url = URL.createObjectURL(blob);
      link.setAttribute('href', url);
      link.setAttribute('download', `dividend_${dataType}_${new Date().toISOString().split('T')[0]}.csv`);
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      message.success(`Successfully exported ${dataType} dividend data`);
    } catch (error) {
      console.error('Export failed:', error);
      message.error('Export failed, please try again');
    }
  };

  // 导出内部交易数据
  const exportInsiderTransactionsData = (data: InsiderTransaction[]) => {
    if (!data || data.length === 0) {
      message.warning('No insider transactions data to export');
      return;
    }

    try {
      // 准备CSV数据
      const headers = [
        'Insider Name',
        'Transaction Date',
        'Transaction Code',
        'Transaction Amount',
        'Transaction Price',
        'Acquired/Disposed',
        'Post Transaction Amount',
        'SEC Link'
      ];

      const csvData = data.map(item => [
        item.ownername || '-',
        item.transactiondate ? fundamentalsUtils.formatDate(item.transactiondate) : '-',
        item.transactioncode || '-',
        item.transactionamount ? fundamentalsUtils.formatNumber(item.transactionamount, 0) : '-',
        item.transactionprice ? fundamentalsUtils.formatCurrency(item.transactionprice, state.fundamentalsData?.company?.currencycode || 'USD') : '-',
        item.transactionacquireddisposed || '-',
        item.posttransactionamount ? fundamentalsUtils.formatNumber(item.posttransactionamount, 0) : '-',
        item.seclink || '-'
      ]);

      // 创建CSV内容
      const csvContent = [
        headers.join(','),
        ...csvData.map(row => row.map(cell => `"${cell}"`).join(','))
      ].join('\n');

      // 创建并下载文件
      const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8;' });
      const link = document.createElement('a');
      const url = URL.createObjectURL(blob);
      link.setAttribute('href', url);
      
      const companyName = state.fundamentalsData?.company?.name || 'Company';
      const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-');
      link.setAttribute('download', `${companyName}_InsiderTransactions_${timestamp}.csv`);
      
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      message.success('Insider transactions data exported successfully');
    } catch (error) {
      console.error('Export failed:', error);
      message.error('Export failed, please try again');
    }
  };

  // Render comprehensive overview section
  const renderOverview = () => {
    if (!state.fundamentalsData) return null;
    
    const { company, highlights, valuations, analyst_ratings } = state.fundamentalsData;
    
    return (
      <div>
        {/* Company Header */}
        <Card style={{ marginBottom: 24 }}>
          <Row gutter={[24, 16]}>
            <Col span={24}>
              <div style={{ display: 'flex', alignItems: 'center', marginBottom: 16 }}>
                {company.logourl && (
                  <img 
                    src={company.logourl} 
                    alt={company.name}
                    style={{ width: 64, height: 64, marginRight: 20, objectFit: 'contain' }}
                    onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }}
                  />
                )}
                <div style={{ flex: 1 }}>
                  <Title level={2} style={{ margin: 0, marginBottom: 8 }}>
                    {company.name}
                  </Title>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 8 }}>
                    <Tag color="blue" style={{ fontSize: '14px', padding: '4px 12px' }}>
                      {company.code}
                    </Tag>
                    <Tag color="green" style={{ fontSize: '14px', padding: '4px 12px' }}>
                      {company.exchange}
                    </Tag>
                    <Tag color="orange" style={{ fontSize: '14px', padding: '4px 12px' }}>
                      {company.countryname}
                    </Tag>
                  </div>
                  <Text type="secondary" style={{ fontSize: '16px' }}>
                    {company.sector && company.industry ? `${company.sector} • ${company.industry}` : company.sector || company.industry || 'N/A'}
                  </Text>
                  {state.updateResult && (
                    <div style={{ marginTop: 8 }}>
                      <Text type="secondary" style={{ fontSize: '14px' }}>
                        Last updated: {state.updateResult.message.includes('Lately updated:') 
                          ? state.updateResult.message.split('Lately updated: ')[1] 
                          : new Date().toLocaleString()}
                      </Text>
                    </div>
                  )}
                </div>
              </div>
            </Col>
          </Row>
        </Card>

        {/* Basic Information */}
        <Card 
          title={
            <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <InfoCircleOutlined style={{ color: '#1890ff' }} />
              Basic Information
            </span>
          } 
          style={{ marginBottom: 24 }}
        >
          <Row gutter={[24, 16]}>
            <Col xs={24} lg={12}>
              <div style={{ 
                display: 'grid', 
                gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', 
                gap: '16px' 
              }}>
                <div>
                  <Text strong>Stock Code:</Text>
                  <br />
                  <Text>{company.code}</Text>
                </div>
                <div>
                  <Text strong>Exchange:</Text>
                  <br />
                  <Text>{company.exchange}</Text>
                </div>
                <div>
                  <Text strong>Currency:</Text>
                  <br />
                  <Text>{company.currencycode || 'N/A'}</Text>
                </div>
                <div>
                  <Text strong>Country:</Text>
                  <br />
                  <Text>{company.countryname || 'N/A'}</Text>
                </div>
                <div>
                  <Text strong>Sector:</Text>
                  <br />
                  <Text>{company.sector || 'N/A'}</Text>
                </div>
                <div>
                  <Text strong>Industry:</Text>
                  <br />
                  <Text>{company.industry || 'N/A'}</Text>
                </div>
                <div>
                  <Text strong>Employees:</Text>
                  <br />
                  <Text>{company.fulltimeemployees ? company.fulltimeemployees.toLocaleString() : 'N/A'}</Text>
                </div>
                <div>
                  <Text strong>Type:</Text>
                  <br />
                  <Text>{company.type || 'N/A'}</Text>
                </div>
              </div>
            </Col>
            <Col xs={24} lg={12}>
              <div>
                <Text strong>Contact Information:</Text>
                <div style={{ marginTop: 8 }}>
                  {company.phone && (
                    <div style={{ marginBottom: 4 }}>
                      <Text>Phone: {company.phone}</Text>
                    </div>
                  )}
                  {company.weburl && (
                    <div style={{ marginBottom: 4 }}>
                      <Text>Website: </Text>
                      <a href={company.weburl} target="_blank" rel="noopener noreferrer">
                        {company.weburl}
                      </a>
                    </div>
                  )}
                  {company.address && (
                    <div>
                      <Text>Address: {company.address}</Text>
                    </div>
                  )}
                </div>
              </div>
            </Col>
            {company.description && (
              <Col span={24}>
                <Divider />
                <div>
                  <Text strong>Company Description:</Text>
                  <Paragraph style={{ marginTop: 8, fontSize: '14px', lineHeight: '1.6' }}>
                    {company.description}
                  </Paragraph>
                </div>
              </Col>
            )}
          </Row>
        </Card>

        {/* Key Metrics */}
        {highlights && (
          <Card 
            title={
              <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <BarChartOutlined style={{ color: '#52c41a' }} />
                Key Metrics
              </span>
            } 
            style={{ marginBottom: 24 }}
          >
            <Row gutter={[16, 16]}>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Market Cap"
                  value={fundamentalsUtils.formatCurrency(highlights.marketcapitalization, company.currencycode)}
                  prefix={<DollarOutlined />}
                  valueStyle={{ color: '#1890ff', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="P/E Ratio"
                  value={fundamentalsUtils.formatNumber(highlights.peratio)}
                  prefix={<LineChartOutlined />}
                  valueStyle={{ color: fundamentalsUtils.getRatioColor(highlights.peratio), fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Revenue (TTM)"
                  value={fundamentalsUtils.formatCurrency(highlights.revenuettm, company.currencycode)}
                  prefix={<BankOutlined />}
                  valueStyle={{ color: '#52c41a', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="EPS (TTM)"
                  value={fundamentalsUtils.formatNumber(highlights.earningsshare)}
                  valueStyle={{ color: fundamentalsUtils.getRatioColor(highlights.earningsshare), fontSize: '16px' }}
                />
              </Col>
              
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Dividend Yield"
                  value={fundamentalsUtils.formatPercentage(highlights.dividendyield)}
                  valueStyle={{ color: '#faad14', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="ROE (TTM)"
                  value={fundamentalsUtils.formatPercentage(highlights.returnonequityttm)}
                  valueStyle={{ color: fundamentalsUtils.getRatioColor(highlights.returnonequityttm), fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Profit Margin"
                  value={fundamentalsUtils.formatPercentage(highlights.profitmargin)}
                  valueStyle={{ color: fundamentalsUtils.getRatioColor(highlights.profitmargin), fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Operating Margin"
                  value={fundamentalsUtils.formatPercentage(highlights.operatingmarginttm)}
                  valueStyle={{ color: fundamentalsUtils.getRatioColor(highlights.operatingmarginttm), fontSize: '16px' }}
                />
              </Col>
            </Row>
          </Card>
        )}

        {/* Analyst Ratings */}
        {analyst_ratings && analyst_ratings.length > 0 && (
          <Card 
            title={
              <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <StarOutlined style={{ color: '#722ed1' }} />
                Analyst Ratings
              </span>
            } 
            style={{ marginBottom: 24 }}
          >
            <Row gutter={[16, 16]}>
              <Col span={24}>
                {analyst_ratings.map((rating, index) => {
                  // Safely handle rating data
                  const safeRating = rating?.rating || 'N/A';
                  const safeAnalystName = rating?.analyst_name || 'Unknown Analyst';
                  const safeTargetPrice = rating?.target_price;
                  const safeRatingDate = rating?.rating_date;
                  const safeBrokerage = rating?.brokerage;
                  const safeRatingValue = rating?.rating_value;
                  const safeTotalAnalysts = rating?.total_analysts || 0;
                  const safeRatingBreakdown = rating?.rating_breakdown;
                  
                  return (
                    <div key={index} style={{ 
                      marginBottom: '16px',
                      padding: '16px', 
                      border: '1px solid #f0f0f0', 
                      borderRadius: '8px',
                      backgroundColor: '#fafafa'
                    }}>
                      {/* 主要评级信息 */}
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                        <div>
                          <Text strong style={{ fontSize: '16px' }}>
                            {safeAnalystName}
                          </Text>
                          {safeRatingValue && (
                            <Text type="secondary" style={{ marginLeft: 8, fontSize: '14px' }}>
                              (Rating: {safeRatingValue.toFixed(2)})
                            </Text>
                          )}
                        </div>
                        <Tag color={fundamentalsUtils.getRatingColor(safeRating)} style={{ fontSize: '14px', padding: '4px 12px' }}>
                          {safeRating}
                        </Tag>
                      </div>
                      
                      {/* 目标价格和日期 */}
                      <Row gutter={[16, 8]} style={{ marginBottom: 12 }}>
                        <Col span={12}>
                          <Text strong>Target Price: </Text>
                          <Text>{safeTargetPrice ? fundamentalsUtils.formatCurrency(safeTargetPrice, company.currencycode) : 'N/A'}</Text>
                        </Col>
                        <Col span={12}>
                          <Text strong>Date: </Text>
                          <Text>{fundamentalsUtils.formatDate(safeRatingDate)}</Text>
                        </Col>
                      </Row>
                      
                      {/* 评级分布 */}
                      {safeRatingBreakdown && safeTotalAnalysts > 0 && (
                        <div style={{ marginTop: 12 }}>
                          <Text strong style={{ fontSize: '14px', marginBottom: 8, display: 'block' }}>
                            Rating Distribution ({safeTotalAnalysts} analysts):
                          </Text>
                          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
                            {safeRatingBreakdown.strong_buy.count > 0 && (
                              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                <div style={{ width: '12px', height: '12px', backgroundColor: '#52c41a', borderRadius: '2px' }}></div>
                                <Text style={{ fontSize: '12px' }}>
                                  Strong Buy: {safeRatingBreakdown.strong_buy.count} ({safeRatingBreakdown.strong_buy.percentage}%)
                                </Text>
                              </div>
                            )}
                            {safeRatingBreakdown.buy.count > 0 && (
                              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                <div style={{ width: '12px', height: '12px', backgroundColor: '#73d13d', borderRadius: '2px' }}></div>
                                <Text style={{ fontSize: '12px' }}>
                                  Buy: {safeRatingBreakdown.buy.count} ({safeRatingBreakdown.buy.percentage}%)
                                </Text>
                              </div>
                            )}
                            {safeRatingBreakdown.hold.count > 0 && (
                              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                <div style={{ width: '12px', height: '12px', backgroundColor: '#faad14', borderRadius: '2px' }}></div>
                                <Text style={{ fontSize: '12px' }}>
                                  Hold: {safeRatingBreakdown.hold.count} ({safeRatingBreakdown.hold.percentage}%)
                                </Text>
                              </div>
                            )}
                            {safeRatingBreakdown.sell.count > 0 && (
                              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                <div style={{ width: '12px', height: '12px', backgroundColor: '#ff7a45', borderRadius: '2px' }}></div>
                                <Text style={{ fontSize: '12px' }}>
                                  Sell: {safeRatingBreakdown.sell.count} ({safeRatingBreakdown.sell.percentage}%)
                                </Text>
                              </div>
                            )}
                            {safeRatingBreakdown.strong_sell.count > 0 && (
                              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                <div style={{ width: '12px', height: '12px', backgroundColor: '#ff4d4f', borderRadius: '2px' }}></div>
                                <Text style={{ fontSize: '12px' }}>
                                  Strong Sell: {safeRatingBreakdown.strong_sell.count} ({safeRatingBreakdown.strong_sell.percentage}%)
                                </Text>
                              </div>
                            )}
                          </div>
                        </div>
                      )}
                      
                      {/* 券商信息 */}
                      {safeBrokerage && (
                        <div style={{ marginTop: 8, fontSize: '12px', color: '#666' }}>
                          <Text>
                            {safeBrokerage === 'Multiple Brokers' ? safeBrokerage : `Firm: ${safeBrokerage}`}
                          </Text>
                        </div>
                      )}
                    </div>
                  );
                })}
              </Col>
            </Row>
          </Card>
        )}

        {/* Valuation Metrics */}
        {valuations && (
          <Card 
            title={
              <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <CalculatorOutlined style={{ color: '#faad14' }} />
                Valuation Metrics
              </span>
            } 
            style={{ marginBottom: 24 }}
          >
            <Row gutter={[16, 16]}>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Trailing P/E"
                  value={fundamentalsUtils.formatNumber(valuations.trailingpe)}
                  valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.trailingpe), fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Forward P/E"
                  value={fundamentalsUtils.formatNumber(valuations.forwardpe)}
                  valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.forwardpe), fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Price/Sales (TTM)"
                  value={fundamentalsUtils.formatNumber(valuations.pricesalesttm)}
                  valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.pricesalesttm), fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Price/Book (MRQ)"
                  value={fundamentalsUtils.formatNumber(valuations.pricebookmrq)}
                  valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.pricebookmrq), fontSize: '16px' }}
                />
              </Col>
              <Col xs={24} sm={12} lg={12}>
                <Statistic
                  title="Enterprise Value"
                  value={fundamentalsUtils.formatCurrency(valuations.enterprisevalue, company.currencycode)}
                  valueStyle={{ color: '#1890ff', fontSize: '16px' }}
                />
              </Col>
              <Col xs={24} sm={12} lg={12}>
                <Statistic
                  title="EV/Revenue"
                  value={fundamentalsUtils.formatNumber(valuations.enterprisevaluerevenue)}
                  valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.enterprisevaluerevenue), fontSize: '16px' }}
                />
              </Col>
            </Row>
          </Card>
        )}

        {/* Technical Indicators */}
        {state.fundamentalsData?.technicals && (
          <Card 
            title={
              <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <DashboardOutlined style={{ color: '#13c2c2' }} />
                Technical Indicators
              </span>
            } 
            style={{ marginBottom: 24 }}
          >
            <Row gutter={[16, 16]}>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Beta"
                  value={fundamentalsUtils.formatNumber(state.fundamentalsData.technicals.beta)}
                  valueStyle={{ 
                    color: state.fundamentalsData.technicals.beta && state.fundamentalsData.technicals.beta > 1 
                      ? '#ff4d4f' : '#52c41a',
                    fontSize: '16px' 
                  }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="52-Week High"
                  value={fundamentalsUtils.formatCurrency(state.fundamentalsData.technicals['52WeekHigh'], company.currencycode)}
                  valueStyle={{ color: '#52c41a', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="52-Week Low"
                  value={fundamentalsUtils.formatCurrency(state.fundamentalsData.technicals['52WeekLow'], company.currencycode)}
                  valueStyle={{ color: '#ff4d4f', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="50-Day MA"
                  value={fundamentalsUtils.formatCurrency(state.fundamentalsData.technicals['50DayMA'], company.currencycode)}
                  valueStyle={{ color: '#1890ff', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="200-Day MA"
                  value={fundamentalsUtils.formatCurrency(state.fundamentalsData.technicals['200DayMA'], company.currencycode)}
                  valueStyle={{ color: '#1890ff', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Short Ratio"
                  value={fundamentalsUtils.formatNumber(state.fundamentalsData.technicals.shortratio)}
                  valueStyle={{ color: '#faad14', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Short Interest"
                  value={fundamentalsUtils.formatNumber(state.fundamentalsData.technicals.sharesshort)}
                  valueStyle={{ color: '#faad14', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Short % Float"
                  value={fundamentalsUtils.formatPercentage(state.fundamentalsData.technicals.shortpercent)}
                  valueStyle={{ color: '#faad14', fontSize: '16px' }}
                />
              </Col>
            </Row>
          </Card>
        )}

        {/* Financial Health Score */}
        {highlights && (
          <Card 
            title={
              <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <HeartOutlined style={{ color: '#ff4d4f' }} />
                Financial Health Assessment
              </span>
            } 
            style={{ marginBottom: 24 }}
          >
            <Row gutter={[16, 16]}>
              <Col span={24}>
                {(() => {
                  const latestBalanceSheet = state.fundamentalsData?.balance_sheet[0];
                  const currentRatio = latestBalanceSheet?.totalcurrentassets && latestBalanceSheet?.totalcurrentliabilities 
                    ? Number(latestBalanceSheet.totalcurrentassets) / Number(latestBalanceSheet.totalcurrentliabilities) 
                    : null;
                  
                  const healthScore = fundamentalsUtils.getFinancialHealthScore(
                    currentRatio,
                    null, // debt to equity ratio would need calculation
                    highlights.returnonequityttm ?? null,
                    highlights.profitmargin ?? null
                  );
                  
                  return (
                    <div style={{ textAlign: 'center' }}>
                      <div style={{ marginBottom: 16 }}>
                        <Text style={{ fontSize: '16px', marginRight: 16 }}>Overall Financial Health:</Text>
                        <Tag color={healthScore.color} style={{ fontSize: '16px', padding: '8px 16px' }}>
                          {healthScore.level}
                        </Tag>
                      </div>
                      <Progress
                        percent={healthScore.score}
                        strokeColor={healthScore.color}
                        style={{ maxWidth: '400px', margin: '0 auto' }}
                        strokeWidth={8}
                      />
                      <div style={{ marginTop: 16, fontSize: '14px', color: '#666' }}>
                        Score: {healthScore.score}/100
                      </div>
                    </div>
                  );
                })()}
              </Col>
            </Row>
          </Card>
        )}
      </div>
    );
  };


  // State for financials filtering
  const [financialsState, setFinancialsState] = useState({
    periodType: 'quarterly',
    searchText: '',
    unitType: 'original'
  });

  // Filter financial data based on current filters
  const getFilteredFinancialData = (data: any[], periodType: string) => {
    if (!data || data.length === 0) return [];
    
    // Filter by period type
    let filtered = data.filter(item => {
      const itemPeriodType = item.period_type?.toLowerCase();
      if (periodType === 'quarterly') {
        return itemPeriodType === 'quarterly';
      } else if (periodType === 'annual') {
        return itemPeriodType === 'annual';
      }
      return true;
    });
    
    // Sort by date descending (most recent first)
    filtered.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
    
    // Return only 4 periods for both quarterly and annual data
    return filtered.slice(0, 4);
  };

  // Format financial value based on unit type
  const formatFinancialValue = (value: any, unitType: string, currency: string) => {
    if (value === null || value === undefined || value === '') return '-';
    
    const numValue = parseFloat(value);
    if (isNaN(numValue)) return '-';
    
    let displayValue = numValue;
    let unitSuffix = '';
    
    switch (unitType) {
      case 'thousands':
        displayValue = numValue / 1000;
        unitSuffix = 'K';
        break;
      case 'millions':
        displayValue = numValue / 1000000;
        unitSuffix = 'M';
        break;
      default:
        displayValue = numValue;
        unitSuffix = '';
    }
    
    // Format the value with proper currency symbol and unit
    const currencySymbols: { [key: string]: string } = {
      'USD': '$',
      'HKD': 'HK$',
      'CNY': '¥',
      'EUR': '€',
      'GBP': '£',
      'JPY': '¥'
    };
    
    const symbol = currencySymbols[currency] || currency;
    
    // Handle very small numbers that might round to 0
    if (Math.abs(displayValue) < 0.01 && displayValue !== 0) {
      return `${symbol}0.00${unitSuffix}`;
    }
    
    const formattedValue = displayValue.toLocaleString('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
    
    return `${symbol}${formattedValue}${unitSuffix}`;
  };

  // Get comprehensive field definitions for each financial statement
  const getBalanceSheetFields = () => [
    // Assets Section
    // Current Assets
    { key: 'cash', label: 'Cash', category: 'current_assets' },
    { key: 'cashandequivalents', label: 'Cash and Equivalents', category: 'current_assets' },
    { key: 'cashandshortterminvestments', label: 'Cash and Short-term Investments', category: 'current_assets' },
    { key: 'shortterminvestments', label: 'Short-term Investments', category: 'current_assets' },
    { key: 'netreceivables', label: 'Net Receivables', category: 'current_assets' },
    { key: 'inventory', label: 'Inventory', category: 'current_assets' },
    { key: 'othercurrentassets', label: 'Other Current Assets', category: 'current_assets' },
    { key: 'totalcurrentassets', label: 'Total Current Assets', category: 'current_assets', isTotal: true },
    
    // Non-Current Assets
    { key: 'longterminvestments', label: 'Long-term Investments', category: 'non_current_assets' },
    { key: 'propertyplantequipment', label: 'Property, Plant & Equipment', category: 'non_current_assets' },
    { key: 'propertyplantandequipmentgross', label: 'PP&E Gross', category: 'non_current_assets' },
    { key: 'propertyplantandequipmentnet', label: 'PP&E Net', category: 'non_current_assets' },
    { key: 'accumulateddepreciation', label: 'Accumulated Depreciation', category: 'non_current_assets' },
    { key: 'goodwill', label: 'Goodwill', category: 'non_current_assets' },
    { key: 'intangibleassets', label: 'Intangible Assets', category: 'non_current_assets' },
    { key: 'accumulatedamortization', label: 'Accumulated Amortization', category: 'non_current_assets' },
    { key: 'noncurrrentassetsother', label: 'Other Non-current Assets', category: 'non_current_assets' },
    { key: 'deferredlongtermassetcharges', label: 'Deferred Long-term Asset Charges', category: 'non_current_assets' },
    { key: 'otherassets', label: 'Other Assets', category: 'non_current_assets' },
    { key: 'earningassets', label: 'Earning Assets', category: 'non_current_assets' },
    { key: 'nettangibleassets', label: 'Net Tangible Assets', category: 'non_current_assets' },
    { key: 'noncurrentassetstotal', label: 'Total Non-current Assets', category: 'non_current_assets', isTotal: true },
    
    // Total Assets
    { key: 'totalassets', label: 'Total Assets', category: 'total_assets', isTotal: true },

    // Liabilities Section
    // Current Liabilities
    { key: 'accountspayable', label: 'Accounts Payable', category: 'current_liabilities' },
    { key: 'shorttermdebt', label: 'Short-term Debt', category: 'current_liabilities' },
    { key: 'shortlongtermdebt', label: 'Short/Long-term Debt', category: 'current_liabilities' },
    { key: 'shortlongtermdebttotal', label: 'Short/Long-term Debt Total', category: 'current_liabilities' },
    { key: 'othercurrentliab', label: 'Other Current Liabilities', category: 'current_liabilities' },
    { key: 'currentdeferredrevenue', label: 'Current Deferred Revenue', category: 'current_liabilities' },
    { key: 'totalcurrentliabilities', label: 'Total Current Liabilities', category: 'current_liabilities', isTotal: true },
    
    // Non-Current Liabilities
    { key: 'longtermdebt', label: 'Long-term Debt', category: 'non_current_liabilities' },
    { key: 'longtermdebttotal', label: 'Total Long-term Debt', category: 'non_current_liabilities' },
    { key: 'capitalleaseobligations', label: 'Capital Lease Obligations', category: 'non_current_liabilities' },
    { key: 'deferredlongtermliab', label: 'Deferred Long-term Liabilities', category: 'non_current_liabilities' },
    { key: 'noncurrentliabilitiesother', label: 'Other Non-current Liabilities', category: 'non_current_liabilities' },
    { key: 'otherliab', label: 'Other Liabilities', category: 'non_current_liabilities' },
    { key: 'netdebt', label: 'Net Debt', category: 'non_current_liabilities' },
    { key: 'noncurrentliabilitiestotal', label: 'Total Non-current Liabilities', category: 'non_current_liabilities', isTotal: true },
    
    // Total Liabilities
    { key: 'totalliab', label: 'Total Liabilities', category: 'total_liabilities', isTotal: true },

    // Equity Section
    { key: 'commonstock', label: 'Common Stock', category: 'equity' },
    { key: 'capitalstock', label: 'Capital Stock', category: 'equity' },
    { key: 'retainedearnings', label: 'Retained Earnings', category: 'equity' },
    { key: 'otherstockholderequity', label: 'Other Stockholder Equity', category: 'equity' },
    { key: 'accumulatedothercomprehensiveincome', label: 'Accumulated Other Comprehensive Income', category: 'equity' },
    { key: 'additionalpaidincapital', label: 'Additional Paid-in Capital', category: 'equity' },
    { key: 'commonstocktotalequity', label: 'Common Stock Total Equity', category: 'equity' },
    { key: 'preferredstocktotalequity', label: 'Preferred Stock Total Equity', category: 'equity' },
    { key: 'retainedearningstotalequity', label: 'Retained Earnings Total Equity', category: 'equity' },
    { key: 'treasurystock', label: 'Treasury Stock', category: 'equity' },
    { key: 'totalpermanentequity', label: 'Total Permanent Equity', category: 'equity' },
    { key: 'noncontrollinginterestinconsolidatedentity', label: 'Non-controlling Interest in Consolidated Entity', category: 'equity' },
    { key: 'temporaryequityredeemablenoncontrollinginterests', label: 'Temporary Equity Redeemable Non-controlling Interests', category: 'equity' },
    { key: 'negativegoodwill', label: 'Negative Goodwill', category: 'equity' },
    { key: 'warrants', label: 'Warrants', category: 'equity' },
    { key: 'preferredstockredeemable', label: 'Preferred Stock Redeemable', category: 'equity' },
    { key: 'capitalsurpluse', label: 'Capital Surplus', category: 'equity' },
    { key: 'liabilitiesandstockholdersequity', label: 'Liabilities and Stockholders Equity', category: 'equity' },
    { key: 'networkingcapital', label: 'Net Working Capital', category: 'equity' },
    { key: 'netinvestedcapital', label: 'Net Invested Capital', category: 'equity' },
    { key: 'commonstocksharesoutstanding', label: 'Common Stock Shares Outstanding', category: 'equity' },
    { key: 'totalstockholderequity', label: 'Total Stockholder Equity', category: 'equity', isTotal: true }
  ];

  const getIncomeStatementFields = () => [
    // Revenue Section
    { key: 'totalrevenue', label: 'Total Revenue', category: 'revenue' },
    { key: 'costofrevenue', label: 'Cost of Revenue', category: 'revenue' },
    { key: 'grossprofit', label: 'Gross Profit', category: 'revenue', isTotal: true },
    
    // Operating Expenses Section
    { key: 'totaloperatingexpenses', label: 'Total Operating Expenses', category: 'expenses' },
    { key: 'researchdevelopment', label: 'Research & Development', category: 'expenses' },
    { key: 'sellinggeneraladministrative', label: 'Selling, General & Administrative', category: 'expenses' },
    { key: 'sellingandmarketingexpenses', label: 'Selling and Marketing Expenses', category: 'expenses' },
    { key: 'otheroperatingexpenses', label: 'Other Operating Expenses', category: 'expenses' },
    
    // Operating Income Section
    { key: 'operatingincome', label: 'Operating Income', category: 'operating', isTotal: true },
    { key: 'ebit', label: 'EBIT', category: 'operating' },
    { key: 'ebitda', label: 'EBITDA', category: 'operating' },
    
    // Interest Section
    { key: 'interestincome', label: 'Interest Income', category: 'interest' },
    { key: 'interestexpense', label: 'Interest Expense', category: 'interest' },
    { key: 'netinterestincome', label: 'Net Interest Income', category: 'interest' },
    
    // Other Income/Expense Section
    { key: 'totalotherincomeexpensenet', label: 'Total Other Income/Expense Net', category: 'other' },
    { key: 'nonoperatingincomenetother', label: 'Non-Operating Income Net Other', category: 'other' },
    
    // Pre-Tax Income Section
    { key: 'incomebeforetax', label: 'Income Before Tax', category: 'pre_tax', isTotal: true },
    
    // Tax Section
    { key: 'taxprovision', label: 'Tax Provision', category: 'tax' },
    { key: 'incometaxexpense', label: 'Income Tax Expense', category: 'tax' },
    
    // Net Income Section
    { key: 'netincome', label: 'Net Income', category: 'net', isTotal: true },
    { key: 'netincomefromcontinuingops', label: 'Net Income from Continuing Operations', category: 'net' },
    { key: 'netincomeapplicabletocommonshares', label: 'Net Income Applicable to Common Shares', category: 'net' },
    
    // Depreciation & Amortization Section
    { key: 'reconcileddepreciation', label: 'Reconciled Depreciation', category: 'depreciation' },
    { key: 'depreciationandamortization', label: 'Depreciation and Amortization', category: 'depreciation' },
    
    // Special Items Section
    { key: 'extraordinaryitems', label: 'Extraordinary Items', category: 'special' },
    { key: 'nonrecurring', label: 'Non-Recurring', category: 'special' },
    { key: 'otheritems', label: 'Other Items', category: 'special' },
    { key: 'discontinuedoperations', label: 'Discontinued Operations', category: 'special' },
    { key: 'effectofaccountingcharges', label: 'Effect of Accounting Charges', category: 'special' },
    
    // Minority Interest & Adjustments Section
    { key: 'minorityinterest', label: 'Minority Interest', category: 'adjustments' },
    { key: 'preferredstockandotheradjustments', label: 'Preferred Stock and Other Adjustments', category: 'adjustments' }
  ];

  const getCashFlowFields = () => [
    // Operating Activities
    { key: 'netincome', label: 'Net Income', category: 'operating' },
    { key: 'depreciation', label: 'Depreciation', category: 'operating' },
    { key: 'stockbasedcompensation', label: 'Stock Based Compensation', category: 'operating' },
    { key: 'changeinworkingcapital', label: 'Change in Working Capital', category: 'operating' },
    { key: 'changetoaccountreceivables', label: 'Change to Account Receivables', category: 'operating' },
    { key: 'changetoinventory', label: 'Change to Inventory', category: 'operating' },
    { key: 'changetoliabilities', label: 'Change to Liabilities', category: 'operating' },
    { key: 'changetooperatingactivities', label: 'Change to Operating Activities', category: 'operating' },
    { key: 'changetonetincome', label: 'Change to Net Income', category: 'operating' },
    { key: 'changereceivables', label: 'Change Receivables', category: 'operating' },
    { key: 'othernoncashitems', label: 'Other Non-Cash Items', category: 'operating' },
    { key: 'cashflowsotheroperating', label: 'Cash Flows Other Operating', category: 'operating' },
    { key: 'totalcashfromoperatingactivities', label: 'Total Cash from Operating Activities', category: 'operating', isTotal: true },
    
    // Investing Activities
    { key: 'capitalexpenditures', label: 'Capital Expenditures', category: 'investing' },
    { key: 'investments', label: 'Investments', category: 'investing' },
    { key: 'othercashflowsfrominvestingactivities', label: 'Other Cash Flows from Investing', category: 'investing' },
    { key: 'salepurchaseofstock', label: 'Sale/Purchase of Stock', category: 'investing' },
    { key: 'totalcashflowsfrominvestingactivities', label: 'Total Cash from Investing Activities', category: 'investing', isTotal: true },
    
    // Financing Activities
    { key: 'dividendspaid', label: 'Dividends Paid', category: 'financing' },
    { key: 'netborrowings', label: 'Net Borrowings', category: 'financing' },
    { key: 'issuanceofcapitalstock', label: 'Issuance of Capital Stock', category: 'financing' },
    { key: 'othercashflowsfromfinancingactivities', label: 'Other Cash Flows from Financing', category: 'financing' },
    { key: 'totalcashfromfinancingactivities', label: 'Total Cash from Financing Activities', category: 'financing', isTotal: true },
    
    // Cash Flow Summary
    { key: 'changeincash', label: 'Change in Cash', category: 'summary' },
    { key: 'beginperiodcashflow', label: 'Beginning Period Cash Flow', category: 'summary' },
    { key: 'endperiodcashflow', label: 'End Period Cash Flow', category: 'summary' },
    { key: 'exchangeratechanges', label: 'Exchange Rate Changes', category: 'summary' },
    { key: 'cashandcashequivalentschanges', label: 'Cash and Cash Equivalents Changes', category: 'summary' },
    { key: 'freecashflow', label: 'Free Cash Flow', category: 'summary', isTotal: true }
  ];

  // Export financial data to CSV
  const exportFinancialData = (dataType: string, data: any[], fields: any[]) => {
    if (!data || data.length === 0) {
      message.warning('No data to export');
      return;
    }

    const company = state.fundamentalsData?.company;
    const companyCode = company?.code || 'company';
    const currency = company?.currencycode || 'USD';
    
    // Create CSV content
    const headers = ['Financial Item', 'Field Name', ...data.map(item => fundamentalsUtils.formatDate(item.date))];
    const csvRows = [headers.join(',')];
    
    fields.forEach(field => {
      const row = [field.label, field.key];
      data.forEach(item => {
        const value = item[field.key];
        if (value !== null && value !== undefined && value !== '') {
          row.push(value.toString());
        } else {
          row.push('');
        }
      });
      csvRows.push(row.join(','));
    });
    
    // Add metadata
    const metadata = [
      `Company Code: ${companyCode}`,
      `Statement Type: ${dataType}`,
      `Period Type: ${financialsState.periodType === 'quarterly' ? 'Quarterly' : 'Annual'}`,
      `Currency: ${currency}`,
      `Export Time: ${new Date().toLocaleString()}`,
      `Data Periods: ${data.length}`,
      `Financial Items: ${fields.length}`,
      ''
    ];
    
    const csvContent = metadata.join('\n') + csvRows.join('\n');
    
    // Download file
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', `${companyCode}_${dataType}_${financialsState.periodType}_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    message.success('Data exported successfully');
  };

  // Export all financial statements to Excel
  const handleExportAllData = async () => {
    if (!state.fundamentalsData?.company) {
      message.error('No company data available for export');
      return;
    }

    setExportLoading(true);
    try {
      const formValues = await form.validateFields();
      
      const exportRequest: FinancialStatementsExportRequest = {
        query: state.fundamentalsData.company.code || state.fundamentalsData.company.name,
        year_range: formValues.yearRange ? [formValues.yearRange[0].year(), formValues.yearRange[1].year()] : undefined,
        period_type: formValues.periodType,
        display_unit: formValues.displayUnit || 'original'
      };

      const blob = await fundamentalsAPI.exportFinancialStatements(exportRequest);
      
      // Create download link
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      
      // Generate filename
      const companyCode = state.fundamentalsData.company.code || 'company';
      const periodSuffix = formValues.periodType === 'quarterly' ? 'quarterly' : 'yearly';
      const yearSuffix = formValues.yearRange 
        ? `_${formValues.yearRange[0].year()}-${formValues.yearRange[1].year()}`
        : '';
      
      a.download = `${companyCode}_financial_statements_${periodSuffix}${yearSuffix}.xlsx`;
      
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      
      message.success('Financial statements exported successfully!');
      setExportDialogVisible(false);
    } catch (error) {
      message.error('Failed to export financial statements. Please try again.');
      console.error('Export error:', error);
    } finally {
      setExportLoading(false);
    }
  };

  // Render financial statements tables
  const renderFinancialStatements = () => {
    if (!state.fundamentalsData) return null;

    const { balance_sheet, income_statement, cash_flow, company } = state.fundamentalsData;
    const currency = company.currencycode || 'USD';

    // Filter data based on current settings
    const filteredBalanceSheet = getFilteredFinancialData(balance_sheet, financialsState.periodType);
    const filteredIncomeStatement = getFilteredFinancialData(income_statement, financialsState.periodType);
    const filteredCashFlow = getFilteredFinancialData(cash_flow, financialsState.periodType);

    // Create comprehensive table data with search filtering
    const createTableData = (fields: any[], data: any[]) => {
      let filteredFields = fields;
      
      // Apply search filter
      if (financialsState.searchText) {
        const searchLower = financialsState.searchText.toLowerCase();
        filteredFields = fields.filter(field => 
          field.label.toLowerCase().includes(searchLower) ||
          field.key.toLowerCase().includes(searchLower)
        );
      }
      
      return filteredFields.map(field => {
        const row: any = { key: field.key, label: field.label, category: field.category, isTotal: field.isTotal };
        data.forEach((item, index) => {
          row[`period_${index}`] = item[field.key];
          row[`date_${index}`] = item.date;
        });
        return row;
      });
    };

    const balanceSheetData = createTableData(getBalanceSheetFields(), filteredBalanceSheet);
    const incomeStatementData = createTableData(getIncomeStatementFields(), filteredIncomeStatement);
    const cashFlowData = createTableData(getCashFlowFields(), filteredCashFlow);

    // Create columns for each period
    const createColumns = (data: any[], title: string) => {
      const columns: ColumnsType<any> = [
        {
          title: 'Financial Item',
          dataIndex: 'label',
          key: 'label',
          fixed: 'left',
          width: 200,
          render: (text: string, record: any) => (
            <div style={{ 
              fontWeight: record.isTotal ? 'bold' : 'normal',
              color: record.isTotal ? '#1890ff' : 'inherit',
              paddingLeft: record.category.includes('_') ? '20px' : '0px'
            }}>
              {text}
            </div>
          )
        }
      ];

      data.slice(0, 4).forEach((item, index) => {
        columns.push({
          title: fundamentalsUtils.formatDate(item.date),
          dataIndex: `period_${index}`,
          key: `period_${index}`,
          width: 120,
          align: 'right' as const,
          render: (value: any, record: any) => (
            <span style={{ 
              color: value && value < 0 ? '#ff4d4f' : 'inherit',
              fontWeight: record?.isTotal ? 'bold' : 'normal'
            }}>
              {formatFinancialValue(value, financialsState.unitType, currency)}
            </span>
          )
        });
      });

      return columns;
    };

    return (
      <div>
        {/* Financial Data Controls */}
        <Card style={{ marginBottom: 16 }}>
          <Row gutter={[16, 16]} align="middle">
            <Col xs={24} sm={6}>
              <div>
                <Text strong>Period Type:</Text>
                <Select
                  value={financialsState.periodType}
                  onChange={(value) => setFinancialsState(prev => ({ ...prev, periodType: value }))}
                  style={{ width: '100%', marginTop: 4 }}
                >
                  <Select.Option value="quarterly">Quarterly</Select.Option>
                  <Select.Option value="annual">Annual</Select.Option>
                </Select>
              </div>
            </Col>
            <Col xs={24} sm={6}>
              <div>
                <Text strong>Search:</Text>
                <Input
                  placeholder="Search financial items..."
                  value={financialsState.searchText}
                  onChange={(e) => setFinancialsState(prev => ({ ...prev, searchText: e.target.value }))}
                  style={{ marginTop: 4 }}
                  prefix={<SearchOutlined />}
                />
              </div>
            </Col>
            <Col xs={24} sm={6}>
              <div>
                <Text strong>Display Unit:</Text>
                <Select
                  value={financialsState.unitType}
                  onChange={(value) => setFinancialsState(prev => ({ ...prev, unitType: value }))}
                  style={{ width: '100%', marginTop: 4 }}
                >
                  <Select.Option value="original">Original</Select.Option>
                  <Select.Option value="thousands">Thousands</Select.Option>
                  <Select.Option value="millions">Millions</Select.Option>
                </Select>
              </div>
            </Col>
            <Col xs={24} sm={6}>
              <div>
                <Text strong>Export:</Text>
                <Button
                  type="primary"
                  icon={<DownloadOutlined />}
                  onClick={() => setExportDialogVisible(true)}
                  style={{ width: '100%', marginTop: 4 }}
                  disabled={!state.fundamentalsData}
                >
                  Export All Data
                </Button>
              </div>
            </Col>
          </Row>
        </Card>

        <Tabs defaultActiveKey="balance_sheet">
          <TabPane tab={<span><TableOutlined /> Balance Sheet</span>} key="balance_sheet">
            <Card>
              <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Text strong style={{ fontSize: '16px' }}>
                  Showing {balanceSheetData.length} balance sheet items - {financialsState.periodType === 'quarterly' ? 'Quarterly' : 'Annual'} data
                </Text>
                <Button 
                  size="small" 
                  onClick={() => exportFinancialData('Balance Sheet', filteredBalanceSheet, getBalanceSheetFields())}
                  disabled={!filteredBalanceSheet || filteredBalanceSheet.length === 0}
                  icon={<TableOutlined />}
                >
                  Export Data
                </Button>
              </div>
              <Table
                columns={createColumns(filteredBalanceSheet, 'Balance Sheet')}
                dataSource={balanceSheetData}
                rowKey="key"
                size="small"
                pagination={false}
                scroll={{ x: 800 }}
                locale={{ emptyText: <Empty description="No balance sheet data available" /> }}
                rowClassName={(record) => record.isTotal ? 'financial-total-row' : ''}
              />
            </Card>
          </TabPane>
          <TabPane tab={<span><MoneyCollectOutlined /> Income Statement</span>} key="income_statement">
            <Card>
              <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Text strong style={{ fontSize: '16px' }}>
                  Showing {incomeStatementData.length} income statement items - {financialsState.periodType === 'quarterly' ? 'Quarterly' : 'Annual'} data
                </Text>
                <Button 
                  size="small" 
                  onClick={() => exportFinancialData('Income Statement', filteredIncomeStatement, getIncomeStatementFields())}
                  disabled={!filteredIncomeStatement || filteredIncomeStatement.length === 0}
                  icon={<MoneyCollectOutlined />}
                >
                  Export Data
                </Button>
              </div>
              <Table
                columns={createColumns(filteredIncomeStatement, 'Income Statement')}
                dataSource={incomeStatementData}
                rowKey="key"
                size="small"
                pagination={false}
                scroll={{ x: 800 }}
                locale={{ emptyText: <Empty description="No income statement data available" /> }}
                rowClassName={(record) => record.isTotal ? 'financial-total-row' : ''}
              />
            </Card>
          </TabPane>
          <TabPane tab={<span><FundOutlined /> Cash Flow</span>} key="cash_flow">
            <Card>
              <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Text strong style={{ fontSize: '16px' }}>
                  Showing {cashFlowData.length} cash flow items - {financialsState.periodType === 'quarterly' ? 'Quarterly' : 'Annual'} data
                </Text>
                <Button 
                  size="small" 
                  onClick={() => exportFinancialData('Cash Flow', filteredCashFlow, getCashFlowFields())}
                  disabled={!filteredCashFlow || filteredCashFlow.length === 0}
                  icon={<FundOutlined />}
                >
                  Export Data
                </Button>
              </div>
              <Table
                columns={createColumns(filteredCashFlow, 'Cash Flow')}
                dataSource={cashFlowData}
                rowKey="key"
                size="small"
                pagination={false}
                scroll={{ x: 800 }}
                locale={{ emptyText: <Empty description="No cash flow data available" /> }}
                rowClassName={(record) => record.isTotal ? 'financial-total-row' : ''}
              />
            </Card>
          </TabPane>
        </Tabs>
      </div>
    );
  };

  // Render earnings data section
  const renderEarningsData = () => {
    if (!state.fundamentalsData) return null;

    const { earnings_annual, earnings_history, earnings_trend, company } = state.fundamentalsData;
    const currency = company.currencycode || 'USD';

    return (
      <div>
        <Tabs defaultActiveKey="trend">
          <TabPane tab={<span><LineChartOutlined /> Earnings Trend</span>} key="trend">
            {renderEarningsTrendTable(earnings_trend, currency)}
          </TabPane>
          <TabPane tab={<span><CalendarOutlined /> Annual Earnings</span>} key="annual">
            {renderEarningsAnnualTable(earnings_annual, currency)}
          </TabPane>
          <TabPane tab={<span><BarChartOutlined /> Earnings History</span>} key="history">
            {renderEarningsHistoryTable(earnings_history, currency)}
          </TabPane>
        </Tabs>
      </div>
    );
  };

  // Render annual earnings table
  const renderEarningsAnnualTable = (data: any[], currency: string) => {
    if (!data || data.length === 0) {
      return (
        <Empty
          description="No annual earnings data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    const columns: ColumnsType<any> = [
      {
        title: 'Date',
        dataIndex: 'date',
        key: 'date',
        width: 120,
        render: (date: string) => fundamentalsUtils.formatDate(date)
      },
      {
        title: 'Actual EPS',
        dataIndex: 'epsactual',
        key: 'epsactual',
        align: 'right' as const,
        render: (value: number) => (
          <span style={{ 
            color: value && value > 0 ? '#52c41a' : value && value < 0 ? '#ff4d4f' : 'inherit',
            fontWeight: 'bold'
          }}>
            {value !== null && value !== undefined ? fundamentalsUtils.formatNumber(value, 4) : '-'}
          </span>
        )
      }
    ];

    return (
      <div>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Text strong style={{ fontSize: '16px' }}>
            Showing {data.length} annual earnings records
          </Text>
        </div>
        <Table
          columns={columns}
          dataSource={data}
          rowKey="id"
          size="small"
          pagination={false}
          scroll={{ x: 400 }}
          locale={{ emptyText: <Empty description="No annual earnings data available" /> }}
        />
      </div>
    );
  };

  // Render earnings history table
  const renderEarningsHistoryTable = (data: any[], currency: string) => {
    if (!data || data.length === 0) {
      return (
        <Empty
          description="No earnings history data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    const columns: ColumnsType<any> = [
      {
        title: 'Report Date',
        dataIndex: 'reportdate',
        key: 'reportdate',
        width: 120,
        render: (date: string) => fundamentalsUtils.formatDate(date)
      },
      {
        title: 'Period End',
        dataIndex: 'date',
        key: 'date',
        width: 120,
        render: (date: string) => fundamentalsUtils.formatDate(date)
      },
      {
        title: 'Actual EPS',
        dataIndex: 'epsactual',
        key: 'epsactual',
        align: 'right' as const,
        render: (value: number) => (
          <span style={{ 
            color: value && value > 0 ? '#52c41a' : value && value < 0 ? '#ff4d4f' : 'inherit',
            fontWeight: 'bold'
          }}>
            {value !== null && value !== undefined ? fundamentalsUtils.formatNumber(value, 4) : '-'}
          </span>
        )
      },
      {
        title: 'Estimate EPS',
        dataIndex: 'epsestimate',
        key: 'epsestimate',
        align: 'right' as const,
        render: (value: number) => (
          <span style={{ 
            color: value && value > 0 ? '#52c41a' : value && value < 0 ? '#ff4d4f' : 'inherit',
            fontWeight: 'bold'
          }}>
            {value !== null && value !== undefined ? fundamentalsUtils.formatNumber(value, 4) : '-'}
          </span>
        )
      },
      {
        title: 'Difference',
        dataIndex: 'epsdifference',
        key: 'epsdifference',
        align: 'right' as const,
        render: (value: number) => (
          <span style={{ 
            color: value && value > 0 ? '#52c41a' : value && value < 0 ? '#ff4d4f' : 'inherit'
          }}>
            {value !== null && value !== undefined ? fundamentalsUtils.formatNumber(value, 4) : '-'}
          </span>
        )
      },
      {
        title: 'Surprise %',
        dataIndex: 'surprisepercent',
        key: 'surprisepercent',
        align: 'right' as const,
        render: (value: number) => (
          <span style={{ 
            color: value && value > 0 ? '#52c41a' : value && value < 0 ? '#ff4d4f' : 'inherit'
          }}>
            {value !== null && value !== undefined ? fundamentalsUtils.formatPercentage(value / 100, 2) : '-'}
          </span>
        )
      },
      {
        title: 'Market',
        dataIndex: 'beforeaftermarket',
        key: 'beforeaftermarket',
        width: 80,
        render: (value: string) => value || '-'
      },
      {
        title: 'Currency',
        dataIndex: 'currency',
        key: 'currency',
        width: 80,
        render: (value: string) => value || currency
      }
    ];

    return (
      <div>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Text strong style={{ fontSize: '16px' }}>
            Showing {data.length} earnings history records
          </Text>
        </div>
        <Table
          columns={columns}
          dataSource={data}
          rowKey="id"
          size="small"
          pagination={false}
          scroll={{ x: 800 }}
          locale={{ emptyText: <Empty description="No earnings history data available" /> }}
        />
      </div>
    );
  };

  // Render earnings trend table
  const renderEarningsTrendTable = (data: any[], currency: string) => {
    if (!data || data.length === 0) {
      return (
        <Empty
          description="No earnings trend data available"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    const columns: ColumnsType<any> = [
      {
        title: 'Date',
        dataIndex: 'date',
        key: 'date',
        width: 120,
        render: (date: string) => fundamentalsUtils.formatDate(date)
      },
      {
        title: 'Period',
        dataIndex: 'period',
        key: 'period',
        width: 100,
        render: (value: string) => value || '-'
      },
      {
        title: 'Growth %',
        dataIndex: 'growth',
        key: 'growth',
        align: 'right' as const,
        render: (value: number) => (
          <span style={{ 
            color: value && value > 0 ? '#52c41a' : value && value < 0 ? '#ff4d4f' : 'inherit'
          }}>
            {value !== null && value !== undefined ? fundamentalsUtils.formatPercentage(value / 100, 2) : '-'}
          </span>
        )
      },
      {
        title: 'Avg Estimate',
        dataIndex: 'earningsestimateavg',
        key: 'earningsestimateavg',
        align: 'right' as const,
        render: (value: number) => (
          <span style={{ 
            color: value && value > 0 ? '#52c41a' : value && value < 0 ? '#ff4d4f' : 'inherit',
            fontWeight: 'bold'
          }}>
            {value !== null && value !== undefined ? fundamentalsUtils.formatNumber(value, 4) : '-'}
          </span>
        )
      },
      {
        title: 'Low Estimate',
        dataIndex: 'earningsestimatelow',
        key: 'earningsestimatelow',
        align: 'right' as const,
        render: (value: number) => (
          <span style={{ 
            color: value && value > 0 ? '#52c41a' : value && value < 0 ? '#ff4d4f' : 'inherit',
            fontWeight: 'bold'
          }}>
            {value !== null && value !== undefined ? fundamentalsUtils.formatNumber(value, 4) : '-'}
          </span>
        )
      },
      {
        title: 'High Estimate',
        dataIndex: 'earningsestimatehigh',
        key: 'earningsestimatehigh',
        align: 'right' as const,
        render: (value: number) => (
          <span style={{ 
            color: value && value > 0 ? '#52c41a' : value && value < 0 ? '#ff4d4f' : 'inherit',
            fontWeight: 'bold'
          }}>
            {value !== null && value !== undefined ? fundamentalsUtils.formatNumber(value, 4) : '-'}
          </span>
        )
      },
      {
        title: 'Current Trend',
        dataIndex: 'epstrendcurrent',
        key: 'epstrendcurrent',
        align: 'right' as const,
        render: (value: number) => (
          <span style={{ 
            color: value && value > 0 ? '#52c41a' : value && value < 0 ? '#ff4d4f' : 'inherit',
            fontWeight: 'bold'
          }}>
            {value !== null && value !== undefined ? fundamentalsUtils.formatNumber(value, 4) : '-'}
          </span>
        )
      },
      {
        title: '7 Days Ago',
        dataIndex: 'epstrend7daysago',
        key: 'epstrend7daysago',
        align: 'right' as const,
        render: (value: number) => (
          <span style={{ 
            color: value && value > 0 ? '#52c41a' : value && value < 0 ? '#ff4d4f' : 'inherit',
            fontWeight: 'bold'
          }}>
            {value !== null && value !== undefined ? fundamentalsUtils.formatNumber(value, 4) : '-'}
          </span>
        )
      },
      {
        title: '30 Days Ago',
        dataIndex: 'epstrend30daysago',
        key: 'epstrend30daysago',
        align: 'right' as const,
        render: (value: number) => (
          <span style={{ 
            color: value && value > 0 ? '#52c41a' : value && value < 0 ? '#ff4d4f' : 'inherit',
            fontWeight: 'bold'
          }}>
            {value !== null && value !== undefined ? fundamentalsUtils.formatNumber(value, 4) : '-'}
          </span>
        )
      },
      {
        title: 'Analysts',
        dataIndex: 'earningsestimatenumberofanalysts',
        key: 'earningsestimatenumberofanalysts',
        align: 'right' as const,
        render: (value: number) => value || '-'
      }
    ];

    return (
      <div>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Text strong style={{ fontSize: '16px' }}>
            Showing {data.length} earnings trend records
          </Text>
        </div>
        <Table
          columns={columns}
          dataSource={data}
          rowKey="id"
          size="small"
          pagination={false}
          scroll={{ x: 1000 }}
          locale={{ emptyText: <Empty description="No earnings trend data available" /> }}
        />
      </div>
    );
  };


  return (
    <div style={{ padding: '24px' }}>
      <style>{`
        .financial-total-row {
          background-color: #f0f8ff !important;
          font-weight: bold;
        }
        .financial-total-row:hover {
          background-color: #e6f7ff !important;
        }
        .ant-table-tbody > tr.financial-total-row > td {
          border-top: 2px solid #1890ff;
        }
        .ant-table-tbody > tr.financial-total-row > td:first-child {
          color: #1890ff;
        }
      `}</style>
      {/* Page Title */}
      <div style={{ marginBottom: 24, textAlign: 'center' }}>
        <Title level={2} style={{ margin: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '12px' }}>
          <BuildOutlined style={{ color: '#1890ff', fontSize: '28px' }} />
          Company Query
        </Title>
        <Text type="secondary" style={{ fontSize: '16px' }}>
          Search and analyze company fundamental data
        </Text>
      </div>

      {/* Search area */}
      <Card style={{ marginBottom: 24 }}>
        <Row gutter={16} align="top">
          <Col flex="auto">
            <div style={{ position: 'relative' }}>
              <AutoComplete
                value={state.searchQuery}
                onChange={handleInputChange}
                onFocus={handleInputFocus}
                onBlur={handleInputBlur}
                placeholder="Enter stock code or company name to search (e.g., AAPL, Apple, Apple Inc.)"
                size="large"
                style={{ fontSize: '16px', width: '100%' }}
                options={[]}
                notFoundContent={null}
                dropdownStyle={{ display: 'none' }}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    handleSearch();
                  }
                }}
              />
              <SearchOutlined 
                style={{ 
                  position: 'absolute', 
                  right: '12px', 
                  top: '50%', 
                  transform: 'translateY(-50%)',
                  color: '#bfbfbf',
                  pointerEvents: 'none'
                }} 
              />
              
              {/* 自定义下拉列表 */}
              {state.showSearchResults && (
                <div style={{
                  position: 'absolute',
                  top: '100%',
                  left: 0,
                  right: 0,
                  zIndex: 1000,
                  backgroundColor: 'white',
                  border: '1px solid #d9d9d9',
                  borderRadius: '6px',
                  boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)',
                  maxHeight: '300px',
                  overflowY: 'auto'
                }}>
                  <Spin spinning={state.searchLoading}>
                    {state.searchResults.length === 0 && state.searchQuery ? (
                      <div style={{ padding: '12px', textAlign: 'center', color: '#999' }}>
                        No stocks found
                      </div>
                    ) : (
                      <List
                        dataSource={state.searchResults}
                        renderItem={(stock) => (
                          <List.Item
                            style={{ 
                              cursor: 'pointer',
                              padding: '8px 12px',
                              borderBottom: '1px solid #f0f0f0'
                            }}
                            onClick={() => handleSelectStock(stock)}
                            onMouseEnter={(e) => {
                              e.currentTarget.style.backgroundColor = '#f5f5f5';
                            }}
                            onMouseLeave={(e) => {
                              e.currentTarget.style.backgroundColor = 'transparent';
                            }}
                          >
                            <List.Item.Meta
                              title={
                                <Space>
                                  <Text strong style={{ color: '#1890ff' }}>{stock.ticker}</Text>
                                  <Text>{stock.company}</Text>
                                </Space>
                              }
                              description={
                                <Space>
                                  <Tag color="blue">{stock.sector}</Tag>
                                  <Tag color="green">{stock.industry}</Tag>
                                </Space>
                              }
                            />
                          </List.Item>
                        )}
                      />
                    )}
                  </Spin>
                </div>
              )}
            </div>
            {state.isAdmin && (
              <div style={{ marginTop: 8, fontSize: '12px' }}>
                <Text type="secondary">
                  Auto-update: {state.autoUpdateEnabled ? (
                    <Text type="success">Enabled</Text>
                  ) : (
                    <Text type="warning">Disabled</Text>
                  )}
                </Text>
              </div>
            )}
          </Col>
          <Col style={{ display: 'flex', alignItems: 'flex-start' }}>
            <Space style={{ marginTop: 0 }}>
              <Button
                type="primary"
                icon={<SearchOutlined />}
                onClick={handleSearch}
                loading={state.loading}
                size="large"
              >
                Search
              </Button>
              <Button
                icon={<ReloadOutlined />}
                onClick={handleClear}
                size="large"
                disabled={state.loading}
              >
                Clear
              </Button>
              {state.isAdmin && (
                <Button
                  icon={<SettingOutlined />}
                  onClick={handleToggleAutoUpdate}
                  size="large"
                  disabled={state.loading}
                  type={state.autoUpdateEnabled ? "primary" : "default"}
                  title={`Auto-update ${state.autoUpdateEnabled ? 'enabled' : 'disabled'}`}
                />
              )}
            </Space>
          </Col>
        </Row>
      </Card>

      {/* Search results */}
      {state.loading && (
        <Card>
          <div style={{ textAlign: 'center', padding: '40px' }}>
            <Spin size="large" />
            <div style={{ marginTop: 16 }}>
              <Text>
                {state.isUpdating 
                  ? 'Updating fundamental data from EODHD API...' 
                  : 'Querying fundamental information...'}
              </Text>
            </div>
          </div>
        </Card>
      )}


      {state.fundamentalsData && !state.loading && (
        <Tabs defaultActiveKey="overview" size="large">
          <TabPane tab={<span><DashboardOutlined /> Overview</span>} key="overview">
            {renderOverview()}
          </TabPane>
          <TabPane tab={<span><FileOutlined /> Financial Statements</span>} key="financials">
            <Card>
              {renderFinancialStatements()}
            </Card>
          </TabPane>
          <TabPane tab={<span><DollarOutlined /> Earnings Data</span>} key="earnings">
            <Card>
              {renderEarningsData()}
            </Card>
          </TabPane>
          <TabPane tab={<span><TeamOutlined /> Holdings</span>} key="holdings">
            <Card>
              {renderHoldingsData()}
            </Card>
          </TabPane>
          <TabPane tab={<span><TeamOutlined /> Officers</span>} key="officers">
            <Card>
              {renderOfficersData()}
            </Card>
          </TabPane>
          <TabPane tab={<span><SwapOutlined /> Insider Transactions</span>} key="insider_transactions">
            <Card>
              {renderInsiderTransactionsData()}
            </Card>
          </TabPane>
          <TabPane tab={<span><BarChartOutlined /> Outstanding Shares</span>} key="outstanding_shares">
            <Card>
              {renderOutstandingSharesData()}
            </Card>
          </TabPane>
          <TabPane tab={<span><MoneyCollectOutlined /> Dividends</span>} key="dividends">
            <Card>
              {renderDividendsData()}
            </Card>
          </TabPane>
        </Tabs>
      )}

      {!state.fundamentalsData && !state.loading && (
        <Card>
          <Empty
            description={
              <span>
                Please enter a stock code or company name to search<br />
                Supported searches: Stock codes (e.g., AAPL), Company names (e.g., Apple Inc.)
              </span>
            }
          />
        </Card>
      )}

      {/* Export Dialog */}
      <Modal
        title="Export All Financial Statements"
        open={exportDialogVisible}
        onOk={handleExportAllData}
        onCancel={() => setExportDialogVisible(false)}
        confirmLoading={exportLoading}
        width={600}
      >
        <Form
          form={form}
          layout="vertical"
          initialValues={{
            periodType: 'yearly',
            displayUnit: 'original'
          }}
        >
          <Form.Item
            label="Year Range"
            name="yearRange"
            tooltip="Select the year range for export. Leave empty to export all available data."
          >
            <DatePicker.RangePicker
              picker="year"
              style={{ width: '100%' }}
              placeholder={['Start Year', 'End Year']}
            />
          </Form.Item>

          <Form.Item
            label="Period Type"
            name="periodType"
            rules={[{ required: true, message: 'Please select period type' }]}
          >
            <Select>
              <Select.Option value="yearly">Yearly</Select.Option>
              <Select.Option value="quarterly">Quarterly</Select.Option>
            </Select>
          </Form.Item>

          <Form.Item
            label="Display Unit"
            name="displayUnit"
            rules={[{ required: true, message: 'Please select display unit' }]}
          >
            <Select>
              <Select.Option value="original">Original</Select.Option>
              <Select.Option value="thousands">Thousands</Select.Option>
              <Select.Option value="millions">Millions</Select.Option>
              <Select.Option value="billions">Billions</Select.Option>
            </Select>
          </Form.Item>

          <div style={{ marginTop: 16, padding: 12, backgroundColor: '#f5f5f5', borderRadius: 6 }}>
            <Text strong>Export will include:</Text>
            <ul style={{ marginTop: 8, marginBottom: 0 }}>
              <li>Sheet 1: Balance Sheet ({form.getFieldValue('periodType') || 'yearly'})</li>
              <li>Sheet 2: Income Statement ({form.getFieldValue('periodType') || 'yearly'})</li>
              <li>Sheet 3: Cash Flow Statement ({form.getFieldValue('periodType') || 'yearly'})</li>
            </ul>
          </div>
        </Form>
      </Modal>
    </div>
  );
};

export default FundamentalsPage;

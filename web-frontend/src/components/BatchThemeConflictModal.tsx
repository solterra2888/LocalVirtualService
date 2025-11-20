/**
 * Batch Theme Conflict Resolution Modal
 * 
 * 在一个界面统一展示和处理所有冲突
 */

import React from 'react';
import { Modal, Button, Table, Typography, Space, Alert, Checkbox, Tag, Collapse, Statistic, Row, Col, Progress, message } from 'antd';
import { ExclamationCircleOutlined, CheckCircleOutlined, PlusOutlined, MinusCircleOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

const { Text } = Typography;
const { Panel } = Collapse;

export interface ConflictTheme {
  theme_id: string;
  theme_name: string;
  category: string;
  existing_theme: {
    db_theme_id: number;
    theme_id: string;
    theme_name: string;
    category: string;
    description: string;
    group?: string;
    created_by: string;
    tickers: string[];
  };
  new_data: {
    theme_name: string;
    category: string;
    description: string;
    group?: string;
    tickers: string[];
    market: string;
  };
}

interface TickerItem {
  ticker: string;
  status: 'new' | 'existing' | 'duplicate';
  statusText: string;
  statusColor: string;
}

interface BatchThemeConflictModalProps {
  visible: boolean;
  conflicts: ConflictTheme[];
  onClose: () => void;
  onResolve: (themeId: string, selectedTickers: string[]) => Promise<void>;
}

const BatchThemeConflictModal: React.FC<BatchThemeConflictModalProps> = ({
  visible,
  conflicts,
  onClose,
  onResolve
}) => {
  // 为每个冲突主题维护选中的 tickers
  const [selectedTickersMap, setSelectedTickersMap] = React.useState<Record<string, string[]>>({});
  const [resolving, setResolving] = React.useState(false);
  const [resolvingProgress, setResolvingProgress] = React.useState(0);
  const [resolvedCount, setResolvedCount] = React.useState(0);

  // 初始化：默认全选所有 tickers
  React.useEffect(() => {
    if (visible && conflicts.length > 0) {
      const initialMap: Record<string, string[]> = {};
      
      conflicts.forEach(conflict => {
        // 合并现有和新的 tickers，去重
        const allTickers = Array.from(new Set([
          ...conflict.existing_theme.tickers,
          ...conflict.new_data.tickers
        ]));
        initialMap[conflict.theme_id] = allTickers;
      });
      
      setSelectedTickersMap(initialMap);
      setResolvingProgress(0);
      setResolvedCount(0);
    }
  }, [visible, conflicts]);

  // 计算每个冲突的 ticker 分类
  const getTickerAnalysis = (conflict: ConflictTheme) => {
    const existingSet = new Set(conflict.existing_theme.tickers);
    const newSet = new Set(conflict.new_data.tickers);
    
    // 新增的 tickers（新的有，旧的没有）
    const addedTickers = conflict.new_data.tickers.filter(t => !existingSet.has(t));
    
    // 重复的 tickers（新的有，旧的也有）
    const duplicateTickers = conflict.new_data.tickers.filter(t => existingSet.has(t));
    
    // 原有的 tickers（旧的有，新的没有）
    const existingOnlyTickers = conflict.existing_theme.tickers.filter(t => !newSet.has(t));
    
    return {
      added: addedTickers,
      duplicate: duplicateTickers,
      existingOnly: existingOnlyTickers
    };
  };

  // 判断某个主题是否没有任何变化（所有ticker都是重复的）
  const isNoChanges = (conflict: ConflictTheme) => {
    const analysis = getTickerAnalysis(conflict);
    return analysis.added.length === 0 && analysis.existingOnly.length === 0;
  };

  // 构建表格数据
  const getTableData = (conflict: ConflictTheme): TickerItem[] => {
    const analysis = getTickerAnalysis(conflict);
    const data: TickerItem[] = [];
    
    // 新增的
    analysis.added.forEach(ticker => {
      data.push({
        ticker,
        status: 'new',
        statusText: '新增',
        statusColor: 'green'
      });
    });
    
    // 重复的
    analysis.duplicate.forEach(ticker => {
      data.push({
        ticker,
        status: 'duplicate',
        statusText: '重复',
        statusColor: 'blue'
      });
    });
    
    // 原有的
    analysis.existingOnly.forEach(ticker => {
      data.push({
        ticker,
        status: 'existing',
        statusText: '原有',
        statusColor: 'default'
      });
    });
    
    return data;
  };

  // 表格列定义
  const getColumns = (themeId: string): ColumnsType<TickerItem> => [
    {
      title: 'Select',
      key: 'select',
      width: '15%',
      render: (_: any, record: TickerItem) => (
        <Checkbox
          checked={(selectedTickersMap[themeId] || []).includes(record.ticker)}
          onChange={(e) => {
            const currentSelected = selectedTickersMap[themeId] || [];
            if (e.target.checked) {
              setSelectedTickersMap({
                ...selectedTickersMap,
                [themeId]: [...currentSelected, record.ticker]
              });
            } else {
              setSelectedTickersMap({
                ...selectedTickersMap,
                [themeId]: currentSelected.filter(t => t !== record.ticker)
              });
            }
          }}
        />
      )
    },
    {
      title: 'Ticker',
      dataIndex: 'ticker',
      key: 'ticker',
      width: '45%',
      render: (ticker: string) => <Text strong code>{ticker}</Text>
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      width: '40%',
      render: (_: any, record: TickerItem) => (
        <Tag 
          color={record.statusColor}
          icon={
            record.status === 'new' ? <PlusOutlined /> :
            record.status === 'duplicate' ? <CheckCircleOutlined /> :
            <MinusCircleOutlined />
          }
        >
          {record.statusText}
        </Tag>
      ),
      filters: [
        { text: '新增', value: 'new' },
        { text: '重复', value: 'duplicate' },
        { text: '原有', value: 'existing' }
      ],
      onFilter: (value, record) => record.status === value
    }
  ];

  // 计算单个主题的统计信息
  const getThemeStatistics = React.useCallback((conflict: ConflictTheme) => {
    const analysis = getTickerAnalysis(conflict);
    const selectedTickers = selectedTickersMap[conflict.theme_id] || [];
    const selectedSet = new Set(selectedTickers);
    
    const addedCount = analysis.added.filter(t => selectedSet.has(t)).length;
    const existingCount = analysis.existingOnly.filter(t => selectedSet.has(t)).length;
    const duplicateCount = analysis.duplicate.filter(t => selectedSet.has(t)).length;
    
    const willAdd = addedCount;
    const willKeep = existingCount + duplicateCount;
    const willRemove = analysis.existingOnly.length - existingCount;
    
    return { willAdd, willKeep, willRemove, total: selectedTickers.length };
  }, [selectedTickersMap]);

  // 批量操作按钮
  const handleSelectAll = (themeId: string, conflict: ConflictTheme) => {
    const allTickers = Array.from(new Set([
      ...conflict.existing_theme.tickers,
      ...conflict.new_data.tickers
    ]));
    setSelectedTickersMap({
      ...selectedTickersMap,
      [themeId]: allTickers
    });
  };

  const handleDeselectAll = (themeId: string) => {
    setSelectedTickersMap({
      ...selectedTickersMap,
      [themeId]: []
    });
  };

  const handleSelectByStatus = (themeId: string, conflict: ConflictTheme, status: 'new' | 'existing' | 'duplicate') => {
    const analysis = getTickerAnalysis(conflict);
    const tickersToSelect = 
      status === 'new' ? analysis.added :
      status === 'duplicate' ? analysis.duplicate :
      analysis.existingOnly;
    
    const currentSelected = selectedTickersMap[themeId] || [];
    const newSelection = new Set([...currentSelected, ...tickersToSelect]);
    setSelectedTickersMap({
      ...selectedTickersMap,
      [themeId]: Array.from(newSelection)
    });
  };

  // 确认并更新全部 - 使用批量 API
  const handleResolveAll = async () => {
    try {
      setResolving(true);
      setResolvingProgress(0);
      setResolvedCount(0);
      
      // 准备批量冲突解决数据
      const conflictResolutions: Record<string, {
        theme_name: string;
        category: string;
        description: string;
        selected_tickers: string[];
        group?: string;
      }> = {};
      
      let skipCount = 0;
      conflicts.forEach(conflict => {
        const selectedTickers = selectedTickersMap[conflict.theme_id] || [];
        
        if (selectedTickers.length === 0) {
          console.log(`Skipping ${conflict.theme_id}: no tickers selected`);
          skipCount++;
          return;
        }
        
        conflictResolutions[conflict.theme_id] = {
          theme_name: conflict.theme_name,
          category: conflict.category,
          description: conflict.existing_theme.description,
          selected_tickers: selectedTickers,
          group: conflict.existing_theme.group
        };
      });
      
      if (Object.keys(conflictResolutions).length === 0) {
        message.warning('请至少为一个主题选择 Tickers');
        setResolving(false);
        return;
      }
      
      console.log(`📤 Submitting batch conflict resolution for ${Object.keys(conflictResolutions).length} themes (${skipCount} skipped)`);
      
      // 调用批量冲突解决 API
      const { batchResolveConflicts, getTaskStatus, pollTaskUntilComplete } = await import('../services/investmentThemes');
      
      const result = await batchResolveConflicts(conflictResolutions);
      
      if (!result.success) {
        throw new Error(result.message || '提交失败');
      }
      
      const taskId = result.task_id;
      console.log(`✅ Batch conflict resolution task submitted: ${taskId}`);
      message.success(`批量冲突解决任务已提交，正在处理 ${Object.keys(conflictResolutions).length} 个主题...`);
      
      // 轮询任务状态
      await pollTaskUntilComplete(
        taskId,
        (status: any) => {
          const meta = status.meta || {};
          const progress = meta.progress || 0;
          const successCount = meta.success || 0;
          const failedCount = meta.failed || 0;
          const skippedCount = meta.skipped || 0;
          const processedCount = successCount + failedCount + skippedCount;
          const totalCount = meta.total || conflicts.length;
          
          setResolvingProgress(progress);
          setResolvedCount(processedCount);
          
          console.log(`📊 Progress: ${progress}%, ${processedCount}/${totalCount} processed (${successCount} success, ${failedCount} failed, ${skippedCount} skipped)`);
        },
        1000,   // 1秒轮询间隔（更快响应）
        120000  // 2分钟超时
      );
      
      // 获取最终结果
      const finalStatus = await getTaskStatus(taskId);
      const finalResult = finalStatus.result as any;
      
      if (finalResult && finalResult.success) {
        const stats = finalResult.statistics;
        const skippedCount = stats.skipped || 0;
        
        if (skippedCount > 0) {
          message.success(
            `批量冲突解决完成: ${stats.success} 成功, ${stats.failed} 失败, ${skippedCount} 跳过（无变化，节省了 ${skippedCount} 次 LLM 调用和向量化）`
          );
        } else {
          message.success(
            `批量冲突解决完成: ${stats.success} 成功, ${stats.failed} 失败`
          );
        }
        
        // 关闭对话框
        onClose();
      } else {
        throw new Error(finalResult?.error || '任务执行失败');
      }
      
    } catch (error: any) {
      console.error('❌ Batch conflict resolution failed:', error);
      message.error(`批量冲突解决失败: ${error.message || String(error)}`);
    } finally {
      setResolving(false);
    }
  };

  // 跳过全部
  const handleSkipAll = () => {
    onClose();
  };

  // 全局统计
  const globalStats = React.useMemo(() => {
    let totalAdded = 0;
    let totalKept = 0;
    let totalRemoved = 0;
    let totalSelected = 0;
    let noChangesCount = 0;  // 统计无变化的主题数量
    
    conflicts.forEach(conflict => {
      const stats = getThemeStatistics(conflict);
      totalAdded += stats.willAdd;
      totalKept += stats.willKeep;
      totalRemoved += stats.willRemove;
      totalSelected += stats.total;
      
      // 如果是无变化的主题，计数
      if (isNoChanges(conflict)) {
        noChangesCount++;
      }
    });
    
    return { totalAdded, totalKept, totalRemoved, totalSelected, noChangesCount };
  }, [conflicts, selectedTickersMap, getThemeStatistics]);

  return (
    <Modal
      title={
        <Space>
          <ExclamationCircleOutlined style={{ color: '#faad14' }} />
          <span>批量上传冲突解决 - 统一处理</span>
        </Space>
      }
      open={visible}
      onCancel={onClose}
      width={1200}
      footer={null}
      style={{ top: 20 }}
    >
      <Space direction="vertical" style={{ width: '100%' }} size="large">
        {/* 全局统计 */}
        <Alert
          message="全局操作汇总"
          description={
            <Space direction="vertical" style={{ width: '100%' }} size="small">
              <Row gutter={16}>
                <Col span={6}>
                  <Statistic 
                    title="将新增" 
                    value={globalStats.totalAdded} 
                    suffix="个"
                    valueStyle={{ fontSize: 16, color: '#52c41a' }}
                  />
                </Col>
                <Col span={6}>
                  <Statistic 
                    title="将保留" 
                    value={globalStats.totalKept} 
                    suffix="个"
                    valueStyle={{ fontSize: 16, color: '#1890ff' }}
                  />
                </Col>
                <Col span={6}>
                  <Statistic 
                    title="将移除" 
                    value={globalStats.totalRemoved} 
                    suffix="个"
                    valueStyle={{ fontSize: 16, color: globalStats.totalRemoved > 0 ? '#cf1322' : undefined }}
                  />
                </Col>
                <Col span={6}>
                  <Statistic 
                    title="总选中" 
                    value={globalStats.totalSelected} 
                    suffix="个"
                    valueStyle={{ fontSize: 16, fontWeight: 'bold' }}
                  />
                </Col>
              </Row>
            </Space>
          }
          type="info"
        />

        {/* 进度条（解决中显示） */}
        {resolving && (
          <Alert
            message={`正在处理冲突 (${resolvedCount}/${conflicts.length})...`}
            description={
              <Progress 
                percent={resolvingProgress} 
                status={resolvingProgress === 100 ? 'success' : 'active'}
              />
            }
            type="info"
          />
        )}

        {/* 冲突列表 - 使用折叠面板 */}
        <div style={{ maxHeight: 500, overflowY: 'auto' }}>
          <Collapse 
            defaultActiveKey={[conflicts[0]?.theme_id]}
            style={{ backgroundColor: '#fff' }}
          >
            {conflicts.map((conflict, index) => {
              const tableData = getTableData(conflict);
              const stats = getThemeStatistics(conflict);
              const noChanges = isNoChanges(conflict);
              
              return (
                <Panel
                  key={conflict.theme_id}
                  header={
                    <Space>
                      <Text strong>{index + 1}.</Text>
                      <Text strong>{conflict.theme_name}</Text>
                      <Text type="secondary">-</Text>
                      <Text>{conflict.category}</Text>
                      <Tag color="blue">
                        已选 {stats.total} 个 Tickers
                      </Tag>
                      {noChanges && (
                        <Tag color="success" icon={<CheckCircleOutlined />}>
                          无变化（将跳过 LLM）
                        </Tag>
                      )}
                    </Space>
                  }
                >
                  <Space direction="vertical" style={{ width: '100%' }} size="middle">
                    {/* 主题信息 */}
                    <div>
                      <Space>
                        <Text><Text strong>主题名称:</Text> {conflict.theme_name}</Text>
                        <Text type="secondary">|</Text>
                        <Text><Text strong>分类:</Text> {conflict.category}</Text>
                        {conflict.existing_theme.group && (
                          <>
                            <Text type="secondary">|</Text>
                            <Text><Text strong>分组:</Text> {conflict.existing_theme.group}</Text>
                          </>
                        )}
                      </Space>
                    </div>

                    {/* 快捷操作按钮 */}
                    <Space>
                      <Button size="small" onClick={() => handleSelectAll(conflict.theme_id, conflict)}>
                        全选
                      </Button>
                      <Button size="small" onClick={() => handleDeselectAll(conflict.theme_id)}>
                        全不选
                      </Button>
                      <Button size="small" onClick={() => handleSelectByStatus(conflict.theme_id, conflict, 'new')}>
                        选择新增
                      </Button>
                      <Button size="small" onClick={() => handleSelectByStatus(conflict.theme_id, conflict, 'existing')}>
                        选择原有
                      </Button>
                      <Button size="small" onClick={() => handleSelectByStatus(conflict.theme_id, conflict, 'duplicate')}>
                        选择重复
                      </Button>
                    </Space>

                    {/* Ticker 表格 */}
                    <Table
                      columns={getColumns(conflict.theme_id)}
                      dataSource={tableData}
                      rowKey="ticker"
                      pagination={false}
                      size="small"
                      scroll={{ y: 250 }}
                    />

                    {/* 单个主题统计 */}
                    <Alert
                      message={
                        <Space split={<Text type="secondary">|</Text>}>
                          <Text>将新增: <Text strong style={{ color: '#52c41a' }}>{stats.willAdd}</Text> 个</Text>
                          <Text>将保留: <Text strong style={{ color: '#1890ff' }}>{stats.willKeep}</Text> 个</Text>
                          <Text>将移除: <Text strong style={{ color: stats.willRemove > 0 ? '#cf1322' : undefined }}>{stats.willRemove}</Text> 个</Text>
                          <Text>总计: <Text strong>{stats.total}</Text> 个</Text>
                        </Space>
                      }
                      type="info"
                      showIcon={false}
                    />
                  </Space>
                </Panel>
              );
            })}
          </Collapse>
        </div>

        {/* 底部操作按钮 */}
        <Space style={{ width: '100%', justifyContent: 'flex-end' }}>
          <Button onClick={handleSkipAll} disabled={resolving}>
            跳过全部
          </Button>
          <Button 
            type="primary" 
            onClick={handleResolveAll}
            loading={resolving}
            disabled={globalStats.totalSelected === 0}
          >
            确认并更新全部 ({conflicts.length} 个主题)
          </Button>
        </Space>
      </Space>
    </Modal>
  );
};

export default BatchThemeConflictModal;

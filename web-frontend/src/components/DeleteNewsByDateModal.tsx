import React, { useState } from 'react';
import {
  Modal,
  DatePicker,
  Button,
  Space,
  Alert,
  message,
  Statistic,
  Row,
  Col,
  Card,
  Typography,
  Checkbox
} from 'antd';
import {
  DeleteOutlined,
  WarningOutlined,
  EyeOutlined
} from '@ant-design/icons';
import type { Dayjs } from 'dayjs';
import { newsAPI } from '../services/news';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { getModalStyles } from '../utils/modalStyles';

const { RangePicker } = DatePicker;
const { Text } = Typography;

interface DeleteNewsByDateModalProps {
  visible: boolean;
  onClose: () => void;
  onSuccess?: () => void;
}

interface DeleteStats {
  news_count: number;
  news_symbols_count: number;
  news_tags_count: number;
  news_theme_matches_count: number;
  news_themes_unified_count: number;
  project_news_tracking_count: number;
  total_count: number;
}

const DeleteNewsByDateModal: React.FC<DeleteNewsByDateModalProps> = ({
  visible,
  onClose,
  onSuccess
}) => {
  const { themeMode } = useDashboardSettings();
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs] | null>(null);
  const [previewStats, setPreviewStats] = useState<DeleteStats | null>(null);
  const [loading, setLoading] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [confirmChecked, setConfirmChecked] = useState(false);

  // 重置状态
  const resetState = () => {
    setDateRange(null);
    setPreviewStats(null);
    setConfirmChecked(false);
  };

  // 处理关闭
  const handleClose = () => {
    resetState();
    onClose();
  };

  // 预览删除统计
  const handlePreview = async () => {
    if (!dateRange) {
      message.warning('请选择日期范围');
      return;
    }

    setLoading(true);
    try {
      const [start, end] = dateRange;
      const stats = await newsAPI.previewDeleteByDateRange({
        start_date: start.format('YYYY-MM-DD'),
        end_date: end.format('YYYY-MM-DD'),
        confirm: false
      });

      setPreviewStats(stats);
      message.success('预览统计完成');
    } catch (error: any) {
      message.error(error.response?.data?.detail || '预览失败');
      console.error('Preview delete failed:', error);
    } finally {
      setLoading(false);
    }
  };

  // 执行删除
  const handleDelete = async () => {
    if (!dateRange || !previewStats || !confirmChecked) {
      message.warning('请完成所有必要步骤');
      return;
    }

    if (previewStats.news_count === 0) {
      message.info('没有需要删除的新闻');
      return;
    }

    setDeleting(true);
    try {
      const [start, end] = dateRange;
      const result = await newsAPI.deleteByDateRange({
        start_date: start.format('YYYY-MM-DD'),
        end_date: end.format('YYYY-MM-DD'),
        confirm: true
      });

      message.success(result.message || '删除成功');
      
      // 调用成功回调
      if (onSuccess) {
        onSuccess();
      }

      // 关闭modal
      handleClose();
    } catch (error: any) {
      message.error(error.response?.data?.detail || '删除失败');
      console.error('Delete failed:', error);
    } finally {
      setDeleting(false);
    }
  };

  return (
    <Modal
      title={
        <Space>
          <DeleteOutlined style={{ color: '#ff4d4f' }} />
          <span>按日期范围删除新闻</span>
        </Space>
      }
      open={visible}
      onCancel={handleClose}
      width={700}
      footer={null}
      styles={getModalStyles(themeMode)}
    >
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        {/* 警告信息 */}
        <Alert
          message="危险操作警告"
          description={
            <div>
              <p>此操作将<strong>永久删除</strong>指定日期范围内的所有新闻及其关联数据，包括：</p>
              <ul style={{ marginBottom: 0 }}>
                <li>新闻主记录</li>
                <li>新闻-股票关联</li>
                <li>新闻-标签关联</li>
                <li>新闻-主题匹配</li>
                <li>新闻-统一主题</li>
                <li>Event News Tracking</li>
              </ul>
              <p style={{ marginTop: 8, marginBottom: 0, color: '#ff4d4f' }}>
                <strong>此操作不可逆，请谨慎操作！</strong>
              </p>
            </div>
          }
          type="error"
          showIcon
          icon={<WarningOutlined />}
        />

        {/* 日期选择 */}
        <Card size="small" title="步骤 1: 选择日期范围">
          <Space direction="vertical" style={{ width: '100%' }}>
            <Text type="secondary">选择要删除的新闻日期范围（包含开始和结束日期当天的所有新闻）</Text>
            <RangePicker
              style={{ width: '100%' }}
              value={dateRange}
              onChange={(dates) => {
                setDateRange(dates as [Dayjs, Dayjs] | null);
                setPreviewStats(null);
                setConfirmChecked(false);
              }}
              format="YYYY-MM-DD"
              placeholder={['开始日期', '结束日期']}
            />
          </Space>
        </Card>

        {/* 预览统计 */}
        <Card size="small" title="步骤 2: 预览删除统计">
          <Space direction="vertical" style={{ width: '100%' }}>
            <Button
              icon={<EyeOutlined />}
              onClick={handlePreview}
              disabled={!dateRange}
              loading={loading}
              block
            >
              预览删除统计
            </Button>

            {previewStats && (
              <div style={{ marginTop: 16 }}>
                <Alert
                  message={
                    <Space direction="vertical" size="small" style={{ width: '100%' }}>
                      <div>
                        <Text strong style={{ fontSize: 16 }}>
                          将删除 <Text style={{ color: '#cf1322', fontSize: 18 }}>{previewStats.news_count.toLocaleString()}</Text> 条新闻
                        </Text>
                      </div>
                      <div>
                        <Text type="secondary" style={{ fontSize: 12 }}>
                          以及所有关联数据，总计 <Text strong style={{ color: '#cf1322' }}>{previewStats.total_count.toLocaleString()}</Text> 条记录
                        </Text>
                      </div>
                    </Space>
                  }
                  type="warning"
                  style={{ marginBottom: 16 }}
                />

                <Row gutter={[16, 16]}>
                  <Col span={12}>
                    <Card size="small" style={{ borderColor: '#cf1322' }}>
                      <Statistic
                        title={<Text strong>新闻主记录</Text>}
                        value={previewStats.news_count}
                        valueStyle={{ color: '#cf1322', fontSize: 24 }}
                        suffix={<Text type="secondary" style={{ fontSize: 14 }}>条</Text>}
                      />
                      <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 4 }}>
                        这是实际要删除的新闻数量
                      </Text>
                    </Card>
                  </Col>
                  <Col span={12}>
                    <Card size="small">
                      <Statistic
                        title={<Text>关联数据总计</Text>}
                        value={previewStats.total_count}
                        valueStyle={{ color: '#ff9800', fontSize: 20 }}
                        suffix={<Text type="secondary" style={{ fontSize: 14 }}>条</Text>}
                      />
                      <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 4 }}>
                        包括所有关联表的记录
                      </Text>
                    </Card>
                  </Col>
                  <Col span={12}>
                    <Card size="small">
                      <Statistic
                        title="股票关联"
                        value={previewStats.news_symbols_count}
                        valueStyle={{ fontSize: 16 }}
                      />
                    </Card>
                  </Col>
                  <Col span={12}>
                    <Card size="small">
                      <Statistic
                        title="标签关联"
                        value={previewStats.news_tags_count}
                        valueStyle={{ fontSize: 16 }}
                      />
                    </Card>
                  </Col>
                  <Col span={12}>
                    <Card size="small">
                      <Statistic
                        title="主题匹配"
                        value={previewStats.news_theme_matches_count}
                        valueStyle={{ fontSize: 16 }}
                      />
                    </Card>
                  </Col>
                  <Col span={12}>
                    <Card size="small">
                      <Statistic
                        title="统一主题"
                        value={previewStats.news_themes_unified_count}
                        valueStyle={{ fontSize: 16 }}
                      />
                      <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 4 }}>
                        每条新闻平均 {previewStats.news_count > 0 ? (previewStats.news_themes_unified_count / previewStats.news_count).toFixed(1) : '0'} 条
                      </Text>
                    </Card>
                  </Col>
                </Row>
                
                {/* 详细说明 */}
                <Alert
                  message="统计说明"
                  description={
                    <div style={{ fontSize: 12 }}>
                      <p style={{ marginBottom: 4 }}>
                        • <Text strong>新闻主记录</Text>：实际要删除的新闻数量（{previewStats.news_count.toLocaleString()} 条）
                      </p>
                      <p style={{ marginBottom: 4 }}>
                        • <Text strong>关联数据</Text>：每条新闻在关联表中可能有多条记录，例如：
                      </p>
                      <ul style={{ marginLeft: 20, marginBottom: 0, paddingLeft: 0 }}>
                        <li>统一主题：每条新闻通常有 6-10 条记录（news_value, primary_theme, event_type 等维度）</li>
                        <li>股票关联：每条新闻可能关联多个股票</li>
                        <li>标签关联：每条新闻可能有多个标签</li>
                      </ul>
                      <p style={{ marginTop: 8, marginBottom: 0 }}>
                        • <Text strong>总计记录数</Text>：所有表中将被删除的记录总数（{previewStats.total_count.toLocaleString()} 条）
                      </p>
                    </div>
                  }
                  type="info"
                  style={{ marginTop: 16 }}
                  showIcon
                />
              </div>
            )}
          </Space>
        </Card>

        {/* 确认删除 */}
        {previewStats && previewStats.news_count > 0 && (
          <Card size="small" title="步骤 3: 确认删除">
            <Space direction="vertical" style={{ width: '100%' }}>
              <Checkbox
                checked={confirmChecked}
                onChange={(e) => setConfirmChecked(e.target.checked)}
              >
                <Text type="danger">
                  我已了解此操作的严重性，确认删除以上 {previewStats.news_count.toLocaleString()} 条新闻及其关联数据
                </Text>
              </Checkbox>

              <Button
                type="primary"
                danger
                icon={<DeleteOutlined />}
                onClick={handleDelete}
                disabled={!confirmChecked}
                loading={deleting}
                block
                size="large"
              >
                确认删除
              </Button>
            </Space>
          </Card>
        )}

        {/* 操作按钮 */}
        <div style={{ textAlign: 'right' }}>
          <Button onClick={handleClose}>
            取消
          </Button>
        </div>
      </Space>
    </Modal>
  );
};

export default DeleteNewsByDateModal;


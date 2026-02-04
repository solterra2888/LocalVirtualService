import React, { useState, useCallback } from 'react';
import { Typography, Space, Button, Tag, Row, Col, Skeleton, message } from 'antd';
import { ReloadOutlined, DeleteOutlined, PlusOutlined, EyeOutlined, EyeInvisibleOutlined, BarChartOutlined } from '@ant-design/icons';
import { type StockPriceData } from '../services/dashboard';
import CandlestickChart from './CandlestickChart';
import EventEditModal from './EventEditModal';
import { useChartEvents } from '../hooks/useChartEvents';
import { EventFormData, ChartEvent } from '../types/events';

const { Title } = Typography;

export type StockChartThemeMode = 'light' | 'dark';

interface StockChartProps {
  symbol: string;
  companyName?: string;
  primaryTicker?: string;
  realTimePriceData?: StockPriceData;
  onRefreshPrice?: () => void;
  priceLoading?: boolean;
  onRemoveFromWatchlist?: () => void;
  sector?: string;
  industry?: string;
  exchange?: string;
  themeMode?: StockChartThemeMode;
}

// 走势图区域 UI 主题（与 FinvizDashboard dark 蓝灰一致）
const STOCK_CHART_UI_THEME = {
  light: {
    headerBg: '#fff',
    headerBorder: '#f0f0f0',
    chartWrapperBg: '#fff',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: '#8c8c8c',
    positive: '#52c41a',
    negative: '#f5222d',
  },
  dark: {
    headerBg: '#1a2234',
    headerBorder: '#4c5263',
    chartWrapperBg: '#1a2234',
    text: '#e0e6f1',
    textSecondary: 'rgba(255, 255, 255, 0.5)',
    positive: '#38a169',
    negative: '#e53e3e',
  },
} as const;

const getPriceChangeColor = (changePercent?: number, themeMode: StockChartThemeMode = 'light') => {
  if (changePercent === undefined || changePercent === null) return STOCK_CHART_UI_THEME[themeMode].textSecondary;
  return changePercent >= 0 ? STOCK_CHART_UI_THEME[themeMode].positive : STOCK_CHART_UI_THEME[themeMode].negative;
};

const StockChart: React.FC<StockChartProps> = ({ 
  symbol, 
  companyName,
  primaryTicker,
  realTimePriceData,
  onRefreshPrice,
  priceLoading = false,
  onRemoveFromWatchlist,
  sector,
  industry,
  exchange,
  themeMode = 'light',
}) => {
  const ui = STOCK_CHART_UI_THEME[themeMode];
  // 事件管理状态
  const [showEvents, setShowEvents] = useState<boolean>(true);
  const [showVolume, setShowVolume] = useState<boolean>(false); // 默认不显示交易量
  const [eventModalVisible, setEventModalVisible] = useState<boolean>(false);
  const [editingEvent, setEditingEvent] = useState<ChartEvent | null>(null);
  const [clickedDate, setClickedDate] = useState<string | null>(null);
  const [availableTradingDates, setAvailableTradingDates] = useState<string[]>([]);

  // 使用事件管理hook - 优先使用primaryTicker作为事件symbol
  const eventSymbol = primaryTicker || symbol;
  const {
    events,
    loading: eventsLoading,
    addEvent,
    updateEvent,
    deleteEvent
  } = useChartEvents(eventSymbol);

  // 事件处理函数
  const handleAddEvent = useCallback(() => {
    setEditingEvent(null);
    setClickedDate(null);
    setEventModalVisible(true);
  }, []);

  const handleDoubleClickCandlestick = useCallback((date: string) => {
    setClickedDate(date);
    setEditingEvent(null);
    setEventModalVisible(true);
  }, []);

  const handleToggleEvents = useCallback(() => {
    setShowEvents(prev => !prev);
  }, []);

  const handleToggleVolume = useCallback(() => {
    setShowVolume(prev => !prev);
  }, []);

  const handleSaveEvent = useCallback(async (eventData: EventFormData) => {
    try {
      if (editingEvent) {
        await updateEvent(editingEvent.id, eventData);
        message.success('Event updated successfully');
      } else {
        console.log('[StockChart] Creating new event:', eventData);
        await addEvent(eventData);
        console.log('[StockChart] Event created successfully, setting showEvents to true');
        message.success('Event added successfully');
        // 创建新事件后，自动显示事件图标
        setShowEvents(true);
      }
      setEventModalVisible(false);
      setEditingEvent(null);
      setClickedDate(null);
    } catch (error) {
      console.error('[StockChart] Failed to save event:', error);
      message.error('Failed to save event');
    }
  }, [editingEvent, updateEvent, addEvent]);

  const handleDeleteEvent = useCallback(async (eventId: string) => {
    try {
      await deleteEvent(eventId);
      message.success('Event deleted successfully');
      setEventModalVisible(false);
      setEditingEvent(null);
    } catch (error) {
      message.error('Failed to delete event');
    }
  }, [deleteEvent]);

  return (
    <div style={{ marginBottom: '16px', position: 'relative' }}>
      {/* Header with Stock Info and Controls */}
      <div 
        style={{ 
          backgroundColor: ui.headerBg, 
          padding: '16px 24px', 
          borderRadius: '8px 8px 0 0',
          borderBottom: `1px solid ${ui.headerBorder}`,
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <div style={{ flex: 1 }}>
            {/* Company Name and Symbol */}
            <div style={{ marginBottom: '12px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
                <Title level={4} style={{ margin: 0, color: ui.text }}>
                  {companyName || symbol}
                  {' '}({symbol})
                </Title>
                <Space size="small">
                  {exchange && <Tag color="default" style={{ fontSize: '11px', fontWeight: 'normal' }}>{exchange}</Tag>}
                  {sector && <Tag color="default" style={{ fontSize: '11px', fontWeight: 'normal' }}>{sector}</Tag>}
                  {industry && <Tag color="default" style={{ fontSize: '11px', fontWeight: 'normal' }}>{industry}</Tag>}
                </Space>
              </div>
            </div>

            {/* Real-time Price Information */}
            {realTimePriceData && (
              <div>
                <Row gutter={16}>
                  <Col span={8}>
                    <div>
                      <Typography.Text strong style={{ fontSize: '12px', color: ui.textSecondary }}>Current Price</Typography.Text>
                      <br />
                      {priceLoading ? (
                        <Skeleton.Input style={{ width: 80, height: 20 }} size="small" active />
                      ) : (
                        <Typography.Text style={{ fontSize: '16px', fontWeight: 'bold', color: ui.text }}>
                          {realTimePriceData.close ? `$${realTimePriceData.close.toFixed(2)}` : 'N/A'}
                        </Typography.Text>
                      )}
                    </div>
                  </Col>
                  <Col span={8}>
                    <div>
                      <Typography.Text strong style={{ fontSize: '12px', color: ui.textSecondary }}>Price Change %</Typography.Text>
                      <br />
                      {priceLoading ? (
                        <Skeleton.Input style={{ width: 60, height: 20 }} size="small" active />
                      ) : (
                        <Typography.Text 
                          style={{ 
                            fontSize: '16px', 
                            fontWeight: 'bold',
                            color: realTimePriceData.change_percent !== undefined 
                              ? getPriceChangeColor(realTimePriceData.change_percent, themeMode) 
                              : ui.textSecondary,
                          }}
                        >
                          {realTimePriceData.change_percent !== undefined 
                            ? `${realTimePriceData.change_percent >= 0 ? '+' : ''}${realTimePriceData.change_percent.toFixed(2)}%` 
                            : 'N/A'
                          }
                        </Typography.Text>
                      )}
                    </div>
                  </Col>
                  <Col span={8}>
                    <div>
                      <Typography.Text strong style={{ fontSize: '12px', color: ui.textSecondary }}>Volume</Typography.Text>
                      <br />
                      {priceLoading ? (
                        <Skeleton.Input style={{ width: 100, height: 20 }} size="small" active />
                      ) : (
                        <Typography.Text style={{ fontSize: '14px', color: ui.text }}>
                          {realTimePriceData.volume ? realTimePriceData.volume.toLocaleString() : 'N/A'}
                        </Typography.Text>
                      )}
                    </div>
                  </Col>
                </Row>
                
                {realTimePriceData.updated_at && (
                  <div style={{ marginTop: '8px', fontSize: '10px', color: ui.textSecondary, fontStyle: 'italic' }}>
                    Updated at {new Date(realTimePriceData.updated_at).toLocaleString()}
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Action Buttons */}
          <Space>
            {/* 事件管理按钮 */}
            <Button
              type="text"
              size="small"
              icon={<PlusOutlined />}
              onClick={handleAddEvent}
              title="Add Event"
            />
            <Button
              type="text"
              size="small"
              icon={<BarChartOutlined />}
              onClick={handleToggleVolume}
              title={showVolume ? 'Hide Volume' : 'Show Volume'}
              style={{ color: showVolume ? '#1890ff' : undefined }}
            />
            <Button
              type="text"
              size="small"
              icon={showEvents ? <EyeInvisibleOutlined /> : <EyeOutlined />}
              onClick={handleToggleEvents}
              title={showEvents ? 'Hide Events' : 'Show Events'}
            />
            
            {onRefreshPrice && (
              <Button
                type="text"
                size="small"
                icon={<ReloadOutlined />}
                onClick={onRefreshPrice}
                loading={priceLoading}
                title="Refresh Price"
              />
            )}
            {onRemoveFromWatchlist && (
              <Button
                type="text"
                danger
                size="small"
                icon={<DeleteOutlined />}
                onClick={onRemoveFromWatchlist}
                title="Remove from watchlist"
              />
            )}
          </Space>
        </div>
      </div>

      {/* Candlestick Chart */}
      <div style={{ backgroundColor: ui.chartWrapperBg, borderRadius: '0 0 8px 8px', overflow: 'hidden', padding: '16px' }}>
        <CandlestickChart
          symbol={symbol}
          companyName={companyName}
          primaryTicker={primaryTicker}
          width={900}
          height={260}
          showHeader={false}
          themeMode={themeMode}
          onAddEventClick={handleAddEvent}
          onToggleEvents={handleToggleEvents}
          externalShowEvents={showEvents}
          showVolume={showVolume}
          onDataLoaded={setAvailableTradingDates}
          onDoubleClickCandlestick={handleDoubleClickCandlestick}
          externalEvents={events}
          externalEventsLoading={eventsLoading}
          onEventCreate={addEvent}
          onEventUpdate={updateEvent}
          onEventDelete={deleteEvent}
        />
      </div>

      {/* 事件编辑模态框 */}
      <EventEditModal
        visible={eventModalVisible}
        onCancel={() => {
          setEventModalVisible(false);
          setEditingEvent(null);
          setClickedDate(null);
        }}
        onSave={handleSaveEvent}
        onDelete={editingEvent ? handleDeleteEvent : undefined}
        event={editingEvent}
        defaultDate={clickedDate || undefined}
        loading={eventsLoading}
        availableTradingDates={availableTradingDates}
      />
    </div>
  );
};

export default StockChart;
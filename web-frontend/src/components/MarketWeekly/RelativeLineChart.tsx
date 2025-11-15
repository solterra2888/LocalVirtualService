import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine
} from 'recharts';
import { Card, Typography, Empty } from 'antd';
import type { RelativePerformancePoint } from '../../types/marketWeekly';

const { Text } = Typography;

interface RelativeLineChartProps {
  data: RelativePerformancePoint[];
  title: string;
  height?: number;
  showTitle?: boolean;
}

// 预定义颜色方案
const COLORS = [
  '#1890ff', // 蓝色
  '#52c41a', // 绿色
  '#fa8c16', // 橙色
  '#eb2f96', // 粉色
  '#722ed1', // 紫色
  '#13c2c2', // 青色
  '#faad14', // 金色
  '#f5222d', // 红色
  '#2f54eb', // 靛蓝
  '#a0d911', // 黄绿
];

const RelativeLineChart: React.FC<RelativeLineChartProps> = ({
  data,
  title,
  height = 400,
  showTitle = true
}) => {
  if (!data || data.length === 0) {
    return (
      <Card>
        {showTitle && <Text strong style={{ fontSize: '14px' }}>{title}</Text>}
        <Empty
          description="No data available"
          style={{ padding: '40px 0' }}
        />
      </Card>
    );
  }

  // 过滤数据，只显示最近3个月
  const threeMonthsAgo = new Date();
  threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);
  const filteredData = data.filter(point => {
    const pointDate = new Date(point.date);
    return pointDate >= threeMonthsAgo;
  });

  if (filteredData.length === 0) {
    return (
      <Card>
        {showTitle && <Text strong style={{ fontSize: '14px' }}>{title}</Text>}
        <Empty
          description="No data available for the last 3 months"
          style={{ padding: '40px 0' }}
        />
      </Card>
    );
  }

  // 提取所有资产名称（除了date字段）
  const assetNames = Object.keys(filteredData[0]).filter(key => key !== 'date');

  // 自定义Tooltip
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div
          style={{
            background: '#fff',
            border: '1px solid #d9d9d9',
            borderRadius: '4px',
            padding: '12px',
            boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
            maxWidth: '250px'
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: '8px', color: '#2C3E50' }}>
            {new Date(label).toLocaleDateString('en-US', {
              month: 'short',
              day: 'numeric',
              year: 'numeric'
            })}
          </div>
          {payload.map((entry: any, index: number) => (
            <div
              key={index}
              style={{
                color: entry.color,
                fontSize: '12px',
                marginBottom: '4px',
                display: 'flex',
                justifyContent: 'space-between',
                gap: '16px'
              }}
            >
              <span>{entry.name}:</span>
              <span style={{ fontWeight: 600 }}>{entry.value.toFixed(2)}</span>
            </div>
          ))}
        </div>
      );
    }
    return null;
  };

  // 自定义Legend
  const CustomLegend = (props: any) => {
    const { payload } = props;
    return (
      <div style={{
        display: 'flex',
        flexWrap: 'wrap',
        justifyContent: 'center',
        gap: '12px 24px',
        marginTop: '16px',
        padding: '0 20px'
      }}>
        {payload.map((entry: any, index: number) => (
          <div
            key={index}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              fontSize: '11px'
            }}
          >
            <div
              style={{
                width: '20px',
                height: '3px',
                background: entry.color,
                borderRadius: '2px'
              }}
            />
            <span style={{ color: '#666' }}>{entry.value}</span>
          </div>
        ))}
      </div>
    );
  };

  // 格式化X轴日期
  const formatXAxis = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  // 计算Y轴的合理范围（基于数据的实际最小值和最大值）
  const calculateYDomain = () => {
    let minValue = Infinity;
    let maxValue = -Infinity;

    filteredData.forEach(point => {
      assetNames.forEach(assetName => {
        const value = point[assetName];
        if (typeof value === 'number' && !isNaN(value)) {
          minValue = Math.min(minValue, value);
          maxValue = Math.max(maxValue, value);
        }
      });
    });

    // 如果没有有效数据，使用默认范围
    if (minValue === Infinity || maxValue === -Infinity) {
      return [95, 105];
    }

    // 添加5%的padding使图表更美观
    const range = maxValue - minValue;
    const padding = range * 0.05;
    
    return [
      Math.floor(minValue - padding),
      Math.ceil(maxValue + padding)
    ];
  };

  const yDomain = calculateYDomain();

  return (
    <div style={{ width: '100%' }}>
      {showTitle && (
        <Text strong style={{ fontSize: '14px', display: 'block', marginBottom: '16px' }}>
          {title}
        </Text>
      )}
      <ResponsiveContainer width="100%" height={height}>
        <LineChart
          data={filteredData}
          margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 10, fill: '#666' }}
            tickFormatter={formatXAxis}
            stroke="#d9d9d9"
            interval="preserveStartEnd"
          />
          <YAxis
            domain={yDomain}
            tick={{ fontSize: 11, fill: '#666' }}
            stroke="#d9d9d9"
            label={{
              value: 'Indexed (Start = 100)',
              angle: -90,
              position: 'insideLeft',
              style: { fontSize: 11, fill: '#666' }
            }}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend content={<CustomLegend />} />
          <ReferenceLine y={100} stroke="#95A5A6" strokeWidth={1.5} strokeDasharray="5 5" />
          
          {assetNames.map((assetName, index) => (
            <Line
              key={assetName}
              type="linear"
              dataKey={assetName}
              stroke={COLORS[index % COLORS.length]}
              strokeWidth={2}
              strokeOpacity={0.5}
              dot={false}
              activeDot={{ r: 5 }}
              connectNulls={true}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
      <div style={{
        textAlign: 'center',
        marginTop: '8px',
        fontSize: '11px',
        color: '#999',
        fontStyle: 'italic'
      }}>
        Cumulative performance indexed to 100 at period start
      </div>
    </div>
  );
};

export default RelativeLineChart;


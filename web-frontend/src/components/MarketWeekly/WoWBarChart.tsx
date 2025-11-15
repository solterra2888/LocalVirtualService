import React from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, ReferenceLine } from 'recharts';
import { Card, Typography, Empty } from 'antd';
import type { WoWDataPoint } from '../../types/marketWeekly';

const { Text } = Typography;

interface WoWBarChartProps {
  data: WoWDataPoint[];
  title: string;
  height?: number;
  showTitle?: boolean;
}

const WoWBarChart: React.FC<WoWBarChartProps> = ({
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

  // 自定义Tooltip
  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      const isPositive = data.change_pct >= 0;
      
      return (
        <div
          style={{
            background: '#fff',
            border: '1px solid #d9d9d9',
            borderRadius: '4px',
            padding: '12px',
            boxShadow: '0 2px 8px rgba(0,0,0,0.1)'
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: '4px', color: '#2C3E50' }}>
            {data.name}
          </div>
          <div style={{
            color: isPositive ? '#52c41a' : '#f5222d',
            fontSize: '16px',
            fontWeight: 'bold'
          }}>
            {isPositive ? '+' : ''}{data.change_pct.toFixed(2)}%
          </div>
        </div>
      );
    }
    return null;
  };

  // 自定义Y轴标签（带百分号）
  const formatYAxis = (value: number) => {
    return `${value}%`;
  };

  // 自定义柱状图标签（与 CombinedWoWChart 样式一致）
  const CustomLabel = (props: any) => {
    const { x, y, width, height, value } = props;
    const isPositive = value >= 0;
    const labelOffset = 10;
    const barStartX = isPositive ? x : x + width;
    const barEndX = isPositive ? x + width : x;
    const labelX = isPositive ? barEndX + labelOffset : barStartX - labelOffset;

    return (
      <text
        x={labelX}
        y={y + height / 2}
        fill="#2C3E50"
        textAnchor={isPositive ? 'start' : 'end'}
        dominantBaseline="middle"
        fontSize="9"
        fontWeight="bold"
      >
        {value >= 0 ? '+' : ''}{value.toFixed(1)}%
      </text>
    );
  };

  return (
    <div style={{ width: '100%' }}>
      {showTitle && (
        <Text strong style={{ fontSize: '14px', display: 'block', marginBottom: '16px' }}>
          {title}
        </Text>
      )}
      <ResponsiveContainer width="100%" height={height}>
        <BarChart
          data={data}
          layout="vertical"

        >
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" horizontal={false} />
          <XAxis
            type="number"
            tick={{ fontSize: 11, fill: '#666' }}
            tickFormatter={formatYAxis}
            stroke="#d9d9d9"
          />
          <YAxis
            type="category"
            dataKey="name"
            tick={{ fontSize: 11, fill: '#333' }}
            width={110}
            stroke="#d9d9d9"
          />
          <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(0,0,0,0.05)' }} />
          <ReferenceLine x={0} stroke="#999" strokeWidth={1.5} />
          <Bar
            dataKey="change_pct"
            radius={[0, 4, 4, 0]}
            label={<CustomLabel />}
          >
            {data.map((entry, index) => (
              <Cell
                key={`cell-${index}`}
                fill={entry.change_pct >= 0 ? '#2E6C8F' : '#C62828'}
                opacity={0.85}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <div style={{
        textAlign: 'center',
        marginTop: '8px',
        fontSize: '11px',
        color: '#999',
        fontStyle: 'italic'
      }}>
        Week-over-week performance comparison
      </div>
    </div>
  );
};

export default WoWBarChart;


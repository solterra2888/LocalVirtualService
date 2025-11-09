import React from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Cell, ResponsiveContainer, ReferenceLine } from 'recharts';
import type { WoWChange } from '../../types/marketWeekly';
import './CombinedWoWChart.css';

interface CombinedWoWChartProps {
  indicesData: WoWChange[];
  forexData: WoWChange[];
  commoditiesData: WoWChange[];
  yieldsData: WoWChange[];
  weekStart: string;
  weekEnd: string;
}

interface SubChartProps {
  data: WoWChange[];
  title: string;
}

const SubChart: React.FC<SubChartProps> = ({ data, title }) => {
  // 如果没有数据，显示"No data available"
  if (!data || data.length === 0) {
    return (
      <div className="subchart-container">
        <h4 className="subchart-title">{title}</h4>
        <div className="no-data-message">
          <p>No data available</p>
        </div>
      </div>
    );
  }

  // 按照变化百分比降序排序
  const sortedData = [...data].sort((a, b) => b.change_pct - a.change_pct);

  // 计算最大绝对值，用于设置x轴范围
  const maxAbsValue = Math.max(...sortedData.map(d => Math.abs(d.change_pct)));
  const xAxisDomain = maxAbsValue > 0 ? [-maxAbsValue * 1.3, maxAbsValue * 1.3] : [-10, 10];

  // 自定义Tooltip
  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="custom-tooltip">
          <p className="tooltip-label">{data.name}</p>
          <p className={`tooltip-value ${data.change_pct >= 0 ? 'positive' : 'negative'}`}>
            {data.change_pct >= 0 ? '+' : ''}{data.change_pct.toFixed(1)}%
          </p>
        </div>
      );
    }
    return null;
  };

  // 自定义标签：在柱状图旁边显示百分比
  const CustomLabel = (props: any) => {
    const { x, y, width, height, value } = props;
    const isPositive = value >= 0;
    const labelOffset = 12;
    const labelX = isPositive ? x + width + labelOffset : x - labelOffset;
    
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

  // 计算动态高度（根据数据项数量）
  const chartHeight = Math.max(300, sortedData.length * 32);

  return (
    <div className="subchart-container">
      <h4 className="subchart-title">{title}</h4>
      <div className="subchart-content" style={{ height: `${chartHeight}px` }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={sortedData}
            layout="vertical"
            margin={{ top: 5, right: 60, left: 5, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e8e8e8" />
            <XAxis
              type="number"
              domain={xAxisDomain}
              tickFormatter={(value) => `${value.toFixed(0)}%`}
              tick={{ fontSize: 9, fill: '#666' }}
              stroke="#ccc"
              axisLine={{ stroke: '#ccc' }}
            />
            <YAxis
              type="category"
              dataKey="name"
              width={85}
              tick={{ fontSize: 9, fill: '#666' }}
              stroke="transparent"
              axisLine={false}
              tickLine={false}
            />
            <Tooltip content={<CustomTooltip />} />
            <ReferenceLine x={0} stroke="#ccc" strokeWidth={1.2} />
            <Bar 
              dataKey="change_pct" 
              radius={[0, 3, 3, 0]}
              label={<CustomLabel />}
            >
              {sortedData.map((entry, index) => (
                <Cell
                  key={`cell-${index}`}
                  fill={entry.change_pct >= 0 ? '#2E6C8F' : '#C62828'}
                  opacity={0.85}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

const CombinedWoWChart: React.FC<CombinedWoWChartProps> = ({
  indicesData,
  forexData,
  commoditiesData,
  yieldsData,
  weekStart,
  weekEnd,
}) => {
  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric'
    });
  };

  return (
    <div className="combined-wow-chart">
      <div className="chart-header">
        <h3 className="chart-main-title">
          Weekly Performance Across All Asset Classes
        </h3>
        <p className="chart-subtitle">
          {formatDate(weekStart)} to {formatDate(weekEnd)}
        </p>
      </div>

      <div className="subcharts-grid">
        <SubChart data={indicesData} title="Equity Indices" />
        <SubChart data={forexData} title="Foreign Exchange" />
        <SubChart data={commoditiesData} title="Commodities" />
        <SubChart data={yieldsData} title="Treasury Yields" />
      </div>
    </div>
  );
};

export default CombinedWoWChart;


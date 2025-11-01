import React from 'react';
import { ChartEvent, EVENT_TYPE_CONFIGS } from '../types/events';

interface EventDotProps {
  cx?: number;
  cy?: number;
  event: ChartEvent;
  onClick?: (event: ChartEvent) => void;
}

const EventDot: React.FC<EventDotProps> = ({ cx = 0, cy = 0, event, onClick }) => {
  const config = EVENT_TYPE_CONFIGS[event.type] || EVENT_TYPE_CONFIGS['custom'];
  
  // DEBUG: 检查坐标和配置
  console.log(`EventDot render - ID: ${event.id}, cx: ${cx}, cy: ${cy}, type: ${event.type}, icon: ${config.icon}, color: ${config.color}`);
  
  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    onClick?.(event);
  };

  return (
    <g onClick={handleClick} style={{ cursor: 'pointer' }}>
      {/* 阴影效果 */}
      <circle
        cx={cx + 1}
        cy={cy + 1}
        r={10}
        fill="rgba(0,0,0,0.2)"
        opacity={0.5}
      />
      
      {/* 外圈光晕效果 */}
      <circle
        cx={cx}
        cy={cy}
        r={14}
        fill={config.color}
        fillOpacity={0.15}
        stroke={config.color}
        strokeWidth={1}
        strokeOpacity={0.4}
      />
      
      {/* 主圆圈 */}
      <circle
        cx={cx}
        cy={cy}
        r={10}
        fill={config.color}
        stroke="#fff"
        strokeWidth={3}
        filter="url(#event-drop-shadow)"
      />
      
      {/* 内圈强调 */}
      <circle
        cx={cx}
        cy={cy}
        r={8}
        fill="none"
        stroke="rgba(255,255,255,0.3)"
        strokeWidth={1}
      />
      
      {/* Emoji图标 */}
      <text
        x={cx}
        y={cy + 1}
        textAnchor="middle"
        dominantBaseline="middle"
        fontSize="14"
        fill="#fff"
        style={{ 
          userSelect: 'none',
          pointerEvents: 'none',
          fontWeight: 'bold',
          textShadow: '1px 1px 2px rgba(0,0,0,0.5)'
        }}
      >
        {config.icon}
      </text>
      
      {/* 鼠标悬停显示标题 */}
      <title>{event.title}</title>
    </g>
  );
};

export default EventDot;

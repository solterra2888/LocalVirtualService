/**
 * Project Card Component
 * 项目卡片组件 - 用于左侧面板的事件列表
 */

import React from 'react';
import { Typography, Tag } from 'antd';
import { InvestmentProject, PROJECT_STATUS_OPTIONS } from '../../types/project';

const { Text } = Typography;

interface ProjectCardThemeColors {
  border?: string;
  cardBg?: string;
  cardSelectedBorder?: string;
  cardSelectedBg?: string;
  cardShadow?: string;
  cardHoverShadow?: string;
}

interface ProjectCardProps {
  project: InvestmentProject;
  isSelected: boolean;
  onClick: (project: InvestmentProject) => void;
  themeColors?: ProjectCardThemeColors;
}

const ProjectCard: React.FC<ProjectCardProps> = ({
  project,
  isSelected,
  onClick,
  themeColors
}) => {
  const border = isSelected ? (themeColors?.cardSelectedBorder ?? '#91d5ff') : (themeColors?.border ?? '#f0f0f0');
  const background = isSelected ? (themeColors?.cardSelectedBg ?? '#e6f7ff') : (themeColors?.cardBg ?? '#ffffff');
  const shadow = isSelected ? '0 2px 8px rgba(24, 144, 255, 0.15)' : (themeColors?.cardShadow ?? '0 1px 2px rgba(0, 0, 0, 0.02)');
  const hoverShadow = themeColors?.cardHoverShadow ?? '0 2px 8px rgba(0, 0, 0, 0.08)';

  const statusOption = PROJECT_STATUS_OPTIONS.find(opt => opt.value === project.status);
  
  return (
    <div
      onClick={() => onClick(project)}
      style={{
        display: 'flex',
        flexDirection: 'column',
        gap: '8px',
        padding: '12px 16px',
        borderRadius: '10px',
        border: `1px solid ${border}`,
        background,
        boxShadow: shadow,
        transition: 'all 0.2s ease',
        cursor: 'pointer',
      }}
      onMouseEnter={(e) => {
        if (!isSelected) {
          e.currentTarget.style.boxShadow = hoverShadow;
        }
      }}
      onMouseLeave={(e) => {
        if (!isSelected) {
          e.currentTarget.style.boxShadow = shadow;
        }
      }}
    >
      {/* Event Name */}
      <Text
        style={{
          fontSize: '14px',
          fontWeight: 500,
          margin: 0,
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}
        title={project.project_name}
      >
        {project.project_name}
      </Text>

      {/* Status Tag */}
      <div>
        <Tag 
          color={statusOption?.color} 
          style={{ 
            margin: 0,
            fontSize: '12px'
          }}
        >
          {statusOption?.label || project.status}
        </Tag>
      </div>
    </div>
  );
};

export default ProjectCard;


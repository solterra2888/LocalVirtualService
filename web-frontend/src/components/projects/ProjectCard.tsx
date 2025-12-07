/**
 * Project Card Component
 * 项目卡片组件 - 用于左侧面板的事件列表
 */

import React from 'react';
import { Typography, Tag } from 'antd';
import { InvestmentProject, PROJECT_STATUS_OPTIONS } from '../../types/project';

const { Text } = Typography;

interface ProjectCardProps {
  project: InvestmentProject;
  isSelected: boolean;
  onClick: (project: InvestmentProject) => void;
}

const ProjectCard: React.FC<ProjectCardProps> = ({
  project,
  isSelected,
  onClick
}) => {
  // 获取状态显示信息
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
        border: isSelected ? '1px solid #91d5ff' : '1px solid #f0f0f0',
        background: isSelected ? '#e6f7ff' : '#ffffff',
        boxShadow: isSelected
          ? '0 2px 8px rgba(24, 144, 255, 0.15)'
          : '0 1px 2px rgba(0, 0, 0, 0.02)',
        transition: 'all 0.2s ease',
        cursor: 'pointer',
      }}
      onMouseEnter={(e) => {
        if (!isSelected) {
          e.currentTarget.style.boxShadow = '0 2px 8px rgba(0, 0, 0, 0.08)';
        }
      }}
      onMouseLeave={(e) => {
        if (!isSelected) {
          e.currentTarget.style.boxShadow = '0 1px 2px rgba(0, 0, 0, 0.02)';
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


import React, { useState } from 'react';
import { Card, Tabs, Space } from 'antd';
import { ScheduleOutlined, BarChartOutlined } from '@ant-design/icons';
import DatabaseTableViewer from '../components/DatabaseTableViewer';
import TaskExecutionHistory from '../components/TaskExecutionHistory';

type SchedulerLogsTabKey = 'task_management' | 'execution_history';

const SchedulerLogsPage: React.FC = () => {
  const [activeTab, setActiveTab] = useState<SchedulerLogsTabKey>('execution_history');

  const handleTabChange = (key: string) => {
    const tabKey = key as SchedulerLogsTabKey;
    setActiveTab(tabKey);
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card>
        <Tabs
          activeKey={activeTab}
          onChange={handleTabChange}
          items={[
            {
              key: 'execution_history',
              label: (
                <Space>
                  <BarChartOutlined />
                  Execution History
                </Space>
              ),
              children: (
                <TaskExecutionHistory
                  height={600}
                />
              )
            },
            {
              key: 'task_management',
              label: (
                <Space>
                  <ScheduleOutlined />
                  Task Management
                </Space>
              ),
              children: (
                <DatabaseTableViewer />
              )
            }
          ]}
        />
      </Card>
    </Space>
  );
};

export default SchedulerLogsPage;



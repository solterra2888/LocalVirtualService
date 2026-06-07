import React, { useEffect, useState } from 'react';
import {
  Button, Card, Form, Input, Modal, Popconfirm, Space, Table, Tag, Typography, App,
  Divider, Switch, Row, Col, Alert,
} from 'antd';
import {
  CheckCircleOutlined, CloseCircleOutlined, QuestionCircleOutlined,
  PlusOutlined, EditOutlined, DeleteOutlined, ReloadOutlined, SafetyCertificateOutlined,
} from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import api from '../services/api';

const { Title, Text } = Typography;
const { TextArea } = Input;

interface Credential {
  id: number;
  label: string;
  status: 'valid' | 'expired' | 'unknown';
  last_validated_at: string | null;
  last_error: string | null;
  created_at: string;
  updated_at: string;
}

interface ZsxqGroup {
  id: number;
  group_id: string;
  group_name: string;
  enabled: boolean;
  backfill_complete: boolean;
  last_sync_at: string | null;
  last_sync_mode: string | null;
  credential_label: string | null;
  credential_status: string | null;
}

const statusColor = (s: string) => {
  if (s === 'valid') return 'success';
  if (s === 'expired') return 'error';
  return 'warning';
};

const statusIcon = (s: string) => {
  if (s === 'valid') return <CheckCircleOutlined />;
  if (s === 'expired') return <CloseCircleOutlined />;
  return <QuestionCircleOutlined />;
};

const ZsxqAdminPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const { message: msg, modal } = App.useApp();
  const isDark = themeMode === 'dark';

  const [credentials, setCredentials] = useState<Credential[]>([]);
  const [groups, setGroups] = useState<ZsxqGroup[]>([]);
  const [credsLoading, setCredsLoading] = useState(false);
  const [groupsLoading, setGroupsLoading] = useState(false);
  const [credModalOpen, setCredModalOpen] = useState(false);
  const [groupModalOpen, setGroupModalOpen] = useState(false);
  const [editingCred, setEditingCred] = useState<Credential | null>(null);
  const [editingGroup, setEditingGroup] = useState<ZsxqGroup | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [credForm] = Form.useForm();
  const [groupForm] = Form.useForm();

  const cardStyle = isDark
    ? { background: '#1a2234', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 12 }
    : { borderRadius: 12 };

  const fetchCredentials = async () => {
    setCredsLoading(true);
    try {
      const data = await api.get('/admin/zsxq/credentials');
      setCredentials(data.data ?? data);
    } catch {
      msg.error('获取凭证列表失败');
    } finally {
      setCredsLoading(false);
    }
  };

  const fetchGroups = async () => {
    setGroupsLoading(true);
    try {
      const data = await api.get('/admin/zsxq/groups');
      setGroups(data.data ?? data);
    } catch {
      msg.error('获取星球列表失败');
    } finally {
      setGroupsLoading(false);
    }
  };

  useEffect(() => {
    fetchCredentials();
    fetchGroups();
  }, []);

  // ---- 凭证操作 ----

  const openCreateCred = () => {
    setEditingCred(null);
    credForm.resetFields();
    setCredModalOpen(true);
  };

  const openEditCred = (cred: Credential) => {
    setEditingCred(cred);
    credForm.setFieldsValue({ label: cred.label, cookie: '' });
    setCredModalOpen(true);
  };

  const handleSaveCred = async () => {
    const values = await credForm.validateFields();
    setSubmitting(true);
    try {
      if (editingCred) {
        await api.put(`/admin/zsxq/credentials/${editingCred.id}`, {
          label: values.label || undefined,
          cookie: values.cookie || undefined,
        });
        msg.success('凭证已更新');
      } else {
        await api.post('/admin/zsxq/credentials', { label: values.label, cookie: values.cookie });
        msg.success('凭证已录入');
      }
      setCredModalOpen(false);
      fetchCredentials();
    } catch (e: any) {
      msg.error(e?.response?.data?.detail || '操作失败');
    } finally {
      setSubmitting(false);
    }
  };

  const handleDeleteCred = async (id: number) => {
    try {
      await api.delete(`/admin/zsxq/credentials/${id}`);
      msg.success('凭证已删除');
      fetchCredentials();
    } catch {
      msg.error('删除失败');
    }
  };

  const handleValidateCred = async (id: number) => {
    const hide = msg.loading('校验中…', 0);
    try {
      const data = await api.post(`/admin/zsxq/credentials/${id}/validate`);
      const result = data.data ?? data;
      hide();
      if (result.success) {
        msg.success(`校验通过，可见星球 ${result.groups_count ?? 0} 个`);
      } else {
        msg.error(`校验失败: ${result.error}`);
      }
      fetchCredentials();
    } catch {
      hide();
      msg.error('校验请求失败');
    }
  };

  const handleRefreshGroups = async (id: number) => {
    const hide = msg.loading('同步星球中…', 0);
    try {
      const data = await api.post(`/admin/zsxq/credentials/${id}/refresh-groups`);
      const result = data.data ?? data;
      hide();
      if (result.success) {
        msg.success(`同步完成，共 ${result.synced} 个星球`);
        fetchGroups();
      } else {
        msg.error(`同步失败: ${result.error}`);
      }
    } catch {
      hide();
      msg.error('同步请求失败');
    }
  };

  // ---- 星球操作 ----

  const openCreateGroup = () => {
    setEditingGroup(null);
    groupForm.resetFields();
    setGroupModalOpen(true);
  };

  const openEditGroup = (g: ZsxqGroup) => {
    setEditingGroup(g);
    groupForm.setFieldsValue({ group_id: g.group_id, group_name: g.group_name, enabled: g.enabled });
    setGroupModalOpen(true);
  };

  const handleSaveGroup = async () => {
    const values = await groupForm.validateFields();
    setSubmitting(true);
    try {
      await api.put('/admin/zsxq/groups', values);
      msg.success('星球已保存');
      setGroupModalOpen(false);
      fetchGroups();
    } catch (e: any) {
      msg.error(e?.response?.data?.detail || '保存失败');
    } finally {
      setSubmitting(false);
    }
  };

  const credColumns = [
    { title: 'ID', dataIndex: 'id', width: 60 },
    { title: '标识', dataIndex: 'label' },
    {
      title: '状态',
      dataIndex: 'status',
      render: (s: string) => (
        <Tag icon={statusIcon(s)} color={statusColor(s)}>
          {s === 'valid' ? '有效' : s === 'expired' ? '已过期' : '未校验'}
        </Tag>
      ),
    },
    {
      title: '最近校验时间',
      dataIndex: 'last_validated_at',
      render: (t: string | null) => t ? new Date(t).toLocaleString() : '-',
    },
    {
      title: '最近错误',
      dataIndex: 'last_error',
      render: (e: string | null) => e ? <Text type="danger" style={{ fontSize: 12 }}>{e}</Text> : '-',
    },
    {
      title: '操作',
      render: (_: any, row: Credential) => (
        <Space>
          <Button size="small" icon={<SafetyCertificateOutlined />} onClick={() => handleValidateCred(row.id)}>校验</Button>
          <Button size="small" icon={<ReloadOutlined />} onClick={() => handleRefreshGroups(row.id)}>同步星球</Button>
          <Button size="small" icon={<EditOutlined />} onClick={() => openEditCred(row)}>编辑</Button>
          <Popconfirm title="确认删除该凭证？" onConfirm={() => handleDeleteCred(row.id)}>
            <Button size="small" danger icon={<DeleteOutlined />}>删除</Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  const groupColumns = [
    { title: 'Group ID', dataIndex: 'group_id', width: 160 },
    { title: '星球名称', dataIndex: 'group_name' },
    {
      title: '开放订阅',
      dataIndex: 'enabled',
      render: (v: boolean) => <Tag color={v ? 'green' : 'default'}>{v ? '是' : '否'}</Tag>,
    },
    {
      title: '补全状态',
      dataIndex: 'backfill_complete',
      render: (v: boolean) => <Tag color={v ? 'blue' : 'orange'}>{v ? '已补全' : '待补全'}</Tag>,
    },
    {
      title: '凭证',
      dataIndex: 'credential_label',
      render: (label: string | null, row: ZsxqGroup) => label
        ? <Tag color={row.credential_status === 'valid' ? 'green' : row.credential_status === 'expired' ? 'red' : 'default'}>{label}</Tag>
        : <Text type="secondary">手动</Text>,
    },
    {
      title: '最近同步',
      dataIndex: 'last_sync_at',
      render: (t: string | null) => t ? new Date(t).toLocaleString() : '-',
    },
    {
      title: '操作',
      render: (_: any, row: ZsxqGroup) => (
        <Button size="small" icon={<EditOutlined />} onClick={() => openEditGroup(row)}>编辑</Button>
      ),
    },
  ];

  return (
    <div style={{ padding: '24px', maxWidth: 1200, margin: '0 auto' }}>
      <Title level={3} style={{ color: isDark ? '#fff' : undefined }}>知识星球管理</Title>

      <Alert
        style={{ marginBottom: 16 }}
        message="Cookie 维护说明"
        description={
          <span>
            在 <b>wx.zsxq.com</b> 登录后按 F12 → Network，找到 api.zsxq.com 的请求，复制完整 Cookie 头粘贴到下方。
            Cookie 通常有效数周至数月，过期后状态变红，点击「校验」或「编辑」重新粘贴即可恢复。
          </span>
        }
        type="info"
        showIcon
      />

      {/* ---- 凭证卡片 ---- */}
      <Card
        title="管理员凭证（Cookie）"
        style={cardStyle}
        extra={
          <Button type="primary" icon={<PlusOutlined />} onClick={openCreateCred}>录入 Cookie</Button>
        }
      >
        <Table
          rowKey="id"
          size="small"
          columns={credColumns}
          dataSource={credentials}
          loading={credsLoading}
          pagination={false}
        />
      </Card>

      <Divider />

      {/* ---- 星球列表卡片 ---- */}
      <Card
        title="知识星球列表"
        style={cardStyle}
        extra={
          <Space>
            <Button icon={<ReloadOutlined />} onClick={fetchGroups}>刷新</Button>
            <Button type="primary" icon={<PlusOutlined />} onClick={openCreateGroup}>手动添加星球</Button>
          </Space>
        }
      >
        <Table
          rowKey="id"
          size="small"
          columns={groupColumns}
          dataSource={groups}
          loading={groupsLoading}
          pagination={false}
        />
      </Card>

      {/* ---- 凭证 Modal ---- */}
      <Modal
        title={editingCred ? '编辑凭证' : '录入 Cookie'}
        open={credModalOpen}
        onOk={handleSaveCred}
        onCancel={() => setCredModalOpen(false)}
        confirmLoading={submitting}
        okText="保存"
        cancelText="取消"
        width={600}
      >
        <Form form={credForm} layout="vertical">
          <Form.Item name="label" label="标识名称" rules={[{ required: !editingCred, message: '请填写标识名称' }]}>
            <Input placeholder="如：guanhe-admin-1" />
          </Form.Item>
          <Form.Item
            name="cookie"
            label="Cookie 字符串"
            rules={[{ required: !editingCred, message: '请粘贴 Cookie' }]}
            extra={editingCred ? '留空则不修改 Cookie，只更新标识名' : '从 wx.zsxq.com 浏览器 F12 复制完整 Cookie 字符串'}
          >
            <TextArea rows={6} placeholder="zsxq_access_token=xxx; sensorsdata2015jssdkcross=..." />
          </Form.Item>
        </Form>
      </Modal>

      {/* ---- 星球 Modal ---- */}
      <Modal
        title={editingGroup ? '编辑星球' : '手动添加星球'}
        open={groupModalOpen}
        onOk={handleSaveGroup}
        onCancel={() => setGroupModalOpen(false)}
        confirmLoading={submitting}
        okText="保存"
        cancelText="取消"
      >
        <Form form={groupForm} layout="vertical">
          <Form.Item name="group_id" label="Group ID" rules={[{ required: true, message: '请填写 group_id' }]}>
            <Input placeholder="如 15555452148452" disabled={!!editingGroup} />
          </Form.Item>
          <Form.Item name="group_name" label="星球名称" rules={[{ required: true, message: '请填写名称' }]}>
            <Input placeholder="如 社长说" />
          </Form.Item>
          <Form.Item name="enabled" label="是否开放订阅" valuePropName="checked" initialValue>
            <Switch checkedChildren="开放" unCheckedChildren="关闭" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

const ZsxqAdminPageWrapper: React.FC = () => (
  <App>
    <ZsxqAdminPage />
  </App>
);

export default ZsxqAdminPageWrapper;

import React, { useEffect, useState } from 'react';
import {
  Button, Card, Form, Input, Modal, Popconfirm, Space, Table, Tag, Typography, App,
  Divider, Switch, Alert, Image,
} from 'antd';
import {
  CheckCircleOutlined, CloseCircleOutlined, QuestionCircleOutlined,
  PlusOutlined, EditOutlined, DeleteOutlined, ReloadOutlined, SafetyCertificateOutlined,
} from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useDeviceDetection } from '../hooks/useDeviceDetection';
import api from '../services/api';

const { Title, Text } = Typography;
const { TextArea } = Input;

interface Credential {
  id: number;
  label: string;
  masked_cookie: string;
  status: 'valid' | 'expired' | 'unknown';
  last_validated_at: string | null;
  last_error: string | null;
  created_at: string;
  updated_at: string;
}

interface Publication {
  id: number;
  publication_url: string;
  publication_id: number | null;
  publication_name: string;
  is_subscribed: boolean;
  is_followed: boolean;
  enabled: boolean;
  backfill_complete: boolean;
  last_posts_sync_at: string | null;
  last_posts_sync_mode: string | null;
  credential_label: string | null;
  credential_status: string | null;
}

interface FollowedAuthor {
  id: number;
  substack_user_id: number;
  author_handle: string;
  author_name: string;
  publication_url: string;
  enabled: boolean;
  backfill_complete: boolean;
  last_notes_sync_at: string | null;
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

const SubstackAdminPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const { message: msg } = App.useApp();
  const isDark = themeMode === 'dark';
  const { isMobile } = useDeviceDetection();
  const [guidePreviewOpen, setGuidePreviewOpen] = useState(false);

  const [credentials, setCredentials] = useState<Credential[]>([]);
  const [publications, setPublications] = useState<Publication[]>([]);
  const [authors, setAuthors] = useState<FollowedAuthor[]>([]);
  const [credsLoading, setCredsLoading] = useState(false);
  const [pubsLoading, setPubsLoading] = useState(false);
  const [authorsLoading, setAuthorsLoading] = useState(false);
  const [credModalOpen, setCredModalOpen] = useState(false);
  const [pubModalOpen, setPubModalOpen] = useState(false);
  const [editingCred, setEditingCred] = useState<Credential | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [credForm] = Form.useForm();
  const [pubForm] = Form.useForm();

  const cardStyle = isDark
    ? { background: '#1a2234', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 12 }
    : { borderRadius: 12 };

  const fetchCredentials = async () => {
    setCredsLoading(true);
    try {
      const data = await api.get('/admin/substack/credentials');
      setCredentials(data.data ?? data);
    } catch {
      msg.error('获取凭证列表失败');
    } finally {
      setCredsLoading(false);
    }
  };

  const fetchPublications = async () => {
    setPubsLoading(true);
    try {
      const data = await api.get('/admin/substack/publications');
      setPublications(data.data ?? data);
    } catch {
      msg.error('获取订阅源失败');
    } finally {
      setPubsLoading(false);
    }
  };

  const fetchAuthors = async () => {
    setAuthorsLoading(true);
    try {
      const data = await api.get('/admin/substack/followed-authors');
      setAuthors(data.data ?? data);
    } catch {
      msg.error('获取关注作者失败');
    } finally {
      setAuthorsLoading(false);
    }
  };

  useEffect(() => {
    fetchCredentials();
    fetchPublications();
    fetchAuthors();
  }, []);

  // ---- 凭证操作 ----

  const openCreateCred = () => {
    setEditingCred(null);
    credForm.resetFields();
    setCredModalOpen(true);
  };

  const openEditCred = (cred: Credential) => {
    setEditingCred(cred);
    credForm.setFieldsValue({ label: cred.label, sid: '', lli: '' });
    setCredModalOpen(true);
  };

  const handleSaveCred = async () => {
    const values = await credForm.validateFields();
    setSubmitting(true);
    try {
      if (editingCred) {
        await api.put(`/admin/substack/credentials/${editingCred.id}`, {
          label: values.label || undefined,
          sid: values.sid || undefined,
          lli: values.lli || undefined,
        });
        msg.success('凭证已更新');
      } else {
        await api.post('/admin/substack/credentials', {
          label: values.label, sid: values.sid, lli: values.lli || '',
        });
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
      await api.delete(`/admin/substack/credentials/${id}`);
      msg.success('凭证已删除');
      fetchCredentials();
    } catch {
      msg.error('删除失败');
    }
  };

  const handleValidateCred = async (id: number) => {
    const hide = msg.loading('校验中…', 0);
    try {
      const data = await api.post(`/admin/substack/credentials/${id}/validate`);
      const result = data.data ?? data;
      hide();
      if (result.success) {
        msg.success(`校验通过，可见订阅 ${result.subscriptions_count ?? 0} 个`);
      } else {
        msg.error(`校验失败: ${result.error}`);
      }
      fetchCredentials();
    } catch {
      hide();
      msg.error('校验请求失败');
    }
  };

  const handleRefreshPublications = async (id: number) => {
    const hide = msg.loading('同步订阅源（源A订阅 + 源B关注）中…', 0);
    try {
      const data = await api.post(`/admin/substack/credentials/${id}/refresh-publications`);
      const result = data.data ?? data;
      hide();
      if (result.success) {
        msg.success(`同步完成：订阅 ${result.synced_subscriptions ?? 0} 个，关注作者 ${result.synced_authors ?? 0} 个`);
        fetchPublications();
        fetchAuthors();
      } else {
        msg.error(`同步失败: ${result.error}`);
      }
    } catch {
      hide();
      msg.error('同步请求失败');
    }
  };

  // ---- publication 手动维护 ----

  const openCreatePub = () => {
    pubForm.resetFields();
    setPubModalOpen(true);
  };

  const handleSavePub = async () => {
    const values = await pubForm.validateFields();
    setSubmitting(true);
    try {
      await api.put('/admin/substack/publications', values);
      msg.success('publication 已保存');
      setPubModalOpen(false);
      fetchPublications();
    } catch (e: any) {
      msg.error(e?.response?.data?.detail || '保存失败');
    } finally {
      setSubmitting(false);
    }
  };

  const credColumns = [
    { title: 'ID', dataIndex: 'id', width: 60 },
    { title: '标识', dataIndex: 'label' },
    { title: 'Cookie', dataIndex: 'masked_cookie', render: (v: string) => <Text code>{v}</Text> },
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
      title: '最近校验',
      dataIndex: 'last_validated_at',
      render: (t: string | null) => t ? new Date(t).toLocaleString() : '-',
    },
    {
      title: '操作',
      width: 360,
      render: (_: any, row: Credential) => (
        <Space wrap>
          <Button size="small" icon={<SafetyCertificateOutlined />} onClick={() => handleValidateCred(row.id)}>校验</Button>
          <Button size="small" icon={<ReloadOutlined />} onClick={() => handleRefreshPublications(row.id)}>同步订阅源</Button>
          <Button size="small" icon={<EditOutlined />} onClick={() => openEditCred(row)}>编辑</Button>
          <Popconfirm title="确认删除该凭证？" onConfirm={() => handleDeleteCred(row.id)}>
            <Button size="small" danger icon={<DeleteOutlined />}>删除</Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  const pubColumns = [
    { title: 'Publication', dataIndex: 'publication_name' },
    {
      title: 'URL',
      dataIndex: 'publication_url',
      render: (u: string) => <a href={u} target="_blank" rel="noreferrer">{u}</a>,
    },
    {
      title: '来源',
      render: (_: any, row: Publication) => (
        <Space>
          {row.is_subscribed && <Tag color="blue">订阅</Tag>}
          {row.is_followed && <Tag color="purple">关注</Tag>}
          {!row.is_subscribed && !row.is_followed && <Tag>手动</Tag>}
        </Space>
      ),
    },
    {
      title: '开放订阅',
      dataIndex: 'enabled',
      render: (v: boolean) => <Tag color={v ? 'green' : 'default'}>{v ? '是' : '否'}</Tag>,
    },
    {
      title: '文章补全',
      dataIndex: 'backfill_complete',
      render: (v: boolean) => <Tag color={v ? 'blue' : 'orange'}>{v ? '已补全' : '待补全'}</Tag>,
    },
    {
      title: '最近同步',
      dataIndex: 'last_posts_sync_at',
      render: (t: string | null) => t ? new Date(t).toLocaleString() : '-',
    },
  ];

  const authorColumns = [
    { title: '作者', dataIndex: 'author_name', render: (n: string, row: FollowedAuthor) => `${n || row.author_handle}` },
    { title: 'Handle', dataIndex: 'author_handle', render: (h: string) => h ? <Text code>@{h}</Text> : '-' },
    {
      title: '主刊',
      dataIndex: 'publication_url',
      render: (u: string) => <a href={u} target="_blank" rel="noreferrer">{u}</a>,
    },
    {
      title: '启用',
      dataIndex: 'enabled',
      render: (v: boolean) => <Tag color={v ? 'green' : 'default'}>{v ? '是' : '否'}</Tag>,
    },
    {
      title: '动态补全',
      dataIndex: 'backfill_complete',
      render: (v: boolean) => <Tag color={v ? 'blue' : 'orange'}>{v ? '已补全' : '待补全'}</Tag>,
    },
    {
      title: '最近同步',
      dataIndex: 'last_notes_sync_at',
      render: (t: string | null) => t ? new Date(t).toLocaleString() : '-',
    },
  ];

  return (
    <div style={{ padding: '24px', maxWidth: 1200, margin: '0 auto' }}>
      <Title level={3} style={{ color: isDark ? '#fff' : undefined }}>Substack 管理</Title>

      <Alert
        style={{ marginBottom: 16 }}
        message="Cookie 维护说明（双值 sid + lli）"
        description={
          <span>
            在 <b>substack.com</b> 登录后按 F12 → Application → Cookies，复制 <code>substack.sid</code>（必需，长期有效）与
            <code> substack.lli</code>（建议，刷新会变属正常）。「同步订阅源」会拉取 <b>源A 订阅 newsletter</b> 与
            <b> 源B 关注作者主刊</b>的并集。
            <br />
            {isMobile ? (
              <Typography.Link href="/substack_cookie_guide.png" target="_blank" rel="noopener noreferrer">查看图文操作指引</Typography.Link>
            ) : (
              <Typography.Link onClick={() => setGuidePreviewOpen(true)}>查看图文操作指引</Typography.Link>
            )}
            <Image
              src="/substack_cookie_guide.png"
              preview={{ visible: guidePreviewOpen, onVisibleChange: setGuidePreviewOpen }}
              style={{ display: 'none' }}
            />
          </span>
        }
        type="info"
        showIcon
      />

      {/* ---- 凭证卡片 ---- */}
      <Card
        title="管理员凭证（substack.sid + substack.lli）"
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

      {/* ---- 订阅源并集卡片 ---- */}
      <Card
        title="订阅源（源A 订阅 + 源B 关注作者主刊 并集）"
        style={cardStyle}
        extra={
          <Space>
            <Button icon={<ReloadOutlined />} onClick={fetchPublications}>刷新</Button>
            <Button type="primary" icon={<PlusOutlined />} onClick={openCreatePub}>手动添加 publication</Button>
          </Space>
        }
      >
        <Table
          rowKey="id"
          size="small"
          columns={pubColumns}
          dataSource={publications}
          loading={pubsLoading}
          pagination={false}
        />
      </Card>

      <Divider />

      {/* ---- 关注作者卡片 ---- */}
      <Card
        title="关注作者（源B · 动态 Notes）"
        style={cardStyle}
        extra={<Button icon={<ReloadOutlined />} onClick={fetchAuthors}>刷新</Button>}
      >
        <Table
          rowKey="id"
          size="small"
          columns={authorColumns}
          dataSource={authors}
          loading={authorsLoading}
          pagination={false}
        />
      </Card>

      {/* ---- 凭证 Modal（sid + lli 双 TextArea）---- */}
      <Modal
        title={editingCred ? '编辑凭证' : '录入 Cookie（sid + lli）'}
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
            name="sid"
            label="substack.sid（必需）"
            rules={[{ required: !editingCred, message: '请粘贴 substack.sid' }]}
            extra={editingCred ? '留空则不改 sid' : '从浏览器 Cookie 复制 substack.sid 的值'}
          >
            <TextArea rows={3} placeholder="s%3Axxxxxxxx.yyyyyyyyy" />
          </Form.Item>
          <Form.Item
            name="lli"
            label="substack.lli（建议）"
            extra={editingCred ? '留空则不改 lli' : '从浏览器 Cookie 复制 substack.lli 的值；刷新会变属正常'}
          >
            <TextArea rows={3} placeholder="eyJhbGciOiJIUzI1NiIs..." />
          </Form.Item>
        </Form>
      </Modal>

      {/* ---- publication Modal ---- */}
      <Modal
        title="手动添加 publication"
        open={pubModalOpen}
        onOk={handleSavePub}
        onCancel={() => setPubModalOpen(false)}
        confirmLoading={submitting}
        okText="保存"
        cancelText="取消"
      >
        <Form form={pubForm} layout="vertical">
          <Form.Item name="publication_url" label="Publication URL" rules={[{ required: true, message: '请填写 URL' }]}>
            <Input placeholder="如 https://citrini.substack.com" />
          </Form.Item>
          <Form.Item name="publication_name" label="展示名">
            <Input placeholder="如 Citrini Research" />
          </Form.Item>
          <Form.Item name="enabled" label="是否开放订阅" valuePropName="checked" initialValue>
            <Switch checkedChildren="开放" unCheckedChildren="关闭" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

const SubstackAdminPageWrapper: React.FC = () => (
  <App>
    <SubstackAdminPage />
  </App>
);

export default SubstackAdminPageWrapper;

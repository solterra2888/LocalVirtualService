import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Checkbox,
  Form,
  Input,
  Modal,
  Select,
  Space,
  Table,
  Tabs,
  Tag,
  Typography,
  message,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { KeyOutlined, CopyOutlined, LinkOutlined, PlusOutlined } from '@ant-design/icons';
import { useAuthStore } from '../store/authStore';

const { Title, Text, Paragraph } = Typography;

type TokenRow = {
  id: number;
  name: string;
  prefix: string;
  scopes: string[];
  expires_at: string | null;
  last_used_at: string | null;
  revoked_at: string | null;
  created_at: string | null;
};

const SCOPE_OPTIONS = [
  { label: 'Daily feed & search (daily:read)', value: 'daily:read' },
  { label: 'Twitter / YouTube reports (reports:read)', value: 'reports:read' },
];

const ApiKeysPage: React.FC = () => {
  const token = useAuthStore((s) => s.token);
  const [rows, setRows] = useState<TokenRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [createOpen, setCreateOpen] = useState(false);
  const [createdPlaintext, setCreatedPlaintext] = useState<string | null>(null);
  const [form] = Form.useForm();

  const apiBase = useMemo(() => process.env.REACT_APP_API_BASE_URL || '/api', []);

  const authHeaders = useCallback(() => {
    const t = token || localStorage.getItem('access_token');
    return {
      Authorization: `Bearer ${t}`,
      'Content-Type': 'application/json',
    } as Record<string, string>;
  }, [token]);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${apiBase}/users/me/tokens`, { headers: authHeaders() });
      if (!res.ok) throw new Error(await res.text());
      const json = await res.json();
      setRows(json.data || []);
    } catch (e) {
      message.error('Failed to load API tokens');
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, [apiBase, authHeaders]);

  useEffect(() => {
    void load();
  }, [load]);

  const origin = typeof window !== 'undefined' ? window.location.origin : '';

  const skillUrl = `${origin}/agent/skill/SKILL.md`;

  const quickInstall = {
    claude: `帮我安装这个 skill：${skillUrl}`,
    cursor: `Install this skill: ${skillUrl}`,
    codex: `Install this skill: ${skillUrl}`,
  };

  const openCreate = () => {
    form.resetFields();
    form.setFieldsValue({
      name: 'Agent',
      scopes: ['daily:read', 'reports:read'],
      expires_days: 'never',
    });
    setCreatedPlaintext(null);
    setCreateOpen(true);
  };

  const submitCreate = async () => {
    try {
      const v = await form.validateFields();
      let expires_days: number | null = null;
      if (v.expires_days === '30') expires_days = 30;
      else if (v.expires_days === '90') expires_days = 90;
      const body = {
        name: v.name,
        scopes: v.scopes as string[],
        expires_days,
      };
      const res = await fetch(`${apiBase}/users/me/tokens`, {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const t = await res.text();
        throw new Error(t);
      }
      const json = await res.json();
      const plain = json?.data?.token as string;
      setCreatedPlaintext(plain || null);
      message.success('Token created — copy it now; it will not be shown again.');
      await load();
    } catch (e) {
      message.error('Create failed');
      console.error(e);
    }
  };

  const revoke = async (id: number) => {
    Modal.confirm({
      title: 'Revoke this API token?',
      okType: 'danger',
      onOk: async () => {
        const res = await fetch(`${apiBase}/users/me/tokens/${id}`, {
          method: 'DELETE',
          headers: authHeaders(),
        });
        if (!res.ok) {
          message.error('Revoke failed');
          return;
        }
        message.success('Revoked');
        await load();
      },
    });
  };

  const columns: ColumnsType<TokenRow> = [
    { title: 'Name', dataIndex: 'name', key: 'name' },
    { title: 'Prefix', dataIndex: 'prefix', key: 'prefix', width: 160 },
    {
      title: 'Scopes',
      dataIndex: 'scopes',
      key: 'scopes',
      render: (scopes: string[]) =>
        (scopes || []).map((s) => (
          <Tag key={s} color="blue">
            {s}
          </Tag>
        )),
    },
    {
      title: 'Expires',
      dataIndex: 'expires_at',
      key: 'expires_at',
      width: 120,
      render: (v: string | null) => v || '—',
    },
    {
      title: 'Last used',
      dataIndex: 'last_used_at',
      key: 'last_used_at',
      width: 160,
      render: (v: string | null) => v || '—',
    },
    {
      title: '',
      key: 'actions',
      width: 100,
      render: (_, r) =>
        r.revoked_at ? (
          <Tag>Revoked</Tag>
        ) : (
          <Button type="link" danger size="small" onClick={() => revoke(r.id)}>
            Revoke
          </Button>
        ),
    },
  ];

  return (
    <div style={{ padding: 24, maxWidth: 960 }}>
      <Title level={4} style={{ marginBottom: 8 }}>
        <KeyOutlined /> API Keys (Agent / MCP)
      </Title>
      <Paragraph type="secondary">
        Personal tokens start with <Text code>ght_</Text>. Use them as{' '}
        <Text code>Authorization: Bearer …</Text> for{' '}
        <Text code>/api/v1/agent</Text> (readonly Daily + reports). Manage tokens here; never commit
        tokens to git.
      </Paragraph>

      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 16 }}
        message={
          <span>
            Skill doc:{' '}
            <a href="/agent/skill/SKILL.md" target="_blank" rel="noreferrer">
              /agent/skill/SKILL.md <LinkOutlined />
            </a>
          </span>
        }
      />

      <Card
        title="Quick connect"
        style={{ marginBottom: 16 }}
        extra={
          <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
            Generate token
          </Button>
        }
      >
        <Tabs
          items={[
            {
              key: 'claude',
              label: 'Claude',
              children: (
                <Space direction="vertical">
                  <Text type="secondary">Paste into Claude Code / Claude Desktop:</Text>
                  <Input.TextArea readOnly rows={2} value={quickInstall.claude} />
                  <Button icon={<CopyOutlined />} onClick={() => void navigator.clipboard.writeText(quickInstall.claude)}>
                    Copy
                  </Button>
                </Space>
              ),
            },
            {
              key: 'cursor',
              label: 'Cursor',
              children: (
                <Space direction="vertical">
                  <Text type="secondary">Ask Cursor agent:</Text>
                  <Input.TextArea readOnly rows={2} value={quickInstall.cursor} />
                  <Button icon={<CopyOutlined />} onClick={() => void navigator.clipboard.writeText(quickInstall.cursor)}>
                    Copy
                  </Button>
                </Space>
              ),
            },
            {
              key: 'codex',
              label: 'Codex',
              children: (
                <Space direction="vertical">
                  <Text type="secondary">Ask Codex CLI / compatible client:</Text>
                  <Input.TextArea readOnly rows={2} value={quickInstall.codex} />
                  <Button icon={<CopyOutlined />} onClick={() => void navigator.clipboard.writeText(quickInstall.codex)}>
                    Copy
                  </Button>
                </Space>
              ),
            },
          ]}
        />
      </Card>

      <Card title="Your tokens">
        <Table rowKey="id" loading={loading} columns={columns} dataSource={rows} pagination={false} />
      </Card>

      <Modal
        title={createdPlaintext ? 'Save your token' : 'Generate API token'}
        open={createOpen}
        onCancel={() => setCreateOpen(false)}
        footer={
          createdPlaintext ? (
            <Button type="primary" onClick={() => setCreateOpen(false)}>
              Done
            </Button>
          ) : (
            <>
              <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
              <Button type="primary" onClick={() => void submitCreate()}>
                Create
              </Button>
            </>
          )
        }
        destroyOnClose
      >
        {createdPlaintext ? (
          <Space direction="vertical" style={{ width: '100%' }}>
            <Alert type="warning" message="This is the only time the full token is shown. Store it securely." />
            <Input.TextArea readOnly rows={4} value={createdPlaintext} />
            <Button
              icon={<CopyOutlined />}
              onClick={() => {
                void navigator.clipboard.writeText(createdPlaintext);
                message.success('Copied');
              }}
            >
              Copy token
            </Button>
          </Space>
        ) : (
          <Form form={form} layout="vertical">
            <Form.Item name="name" label="Name" rules={[{ required: true, message: 'Name required' }]}>
              <Input placeholder="e.g. MacBook Claude" maxLength={80} />
            </Form.Item>
            <Form.Item name="scopes" label="Scopes" rules={[{ required: true }]}>
              <Checkbox.Group options={SCOPE_OPTIONS} />
            </Form.Item>
            <Form.Item name="expires_days" label="Expiry">
              <Select
                options={[
                  { value: 'never', label: 'Never' },
                  { value: '30', label: '30 days' },
                  { value: '90', label: '90 days' },
                ]}
              />
            </Form.Item>
          </Form>
        )}
      </Modal>
    </div>
  );
};

export default ApiKeysPage;

import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import MobileSourceFileDetail from '../components/MobileSourceFileDetail';
import {
  App,
  Avatar,
  Button,
  Checkbox,
  ConfigProvider,
  Dropdown,
  Empty,
  Input,
  Modal,
  Segmented,
  Select,
  AutoComplete,
  Space,
  Spin,
  Tooltip,
  Tag,
  Typography,
  Drawer,
  theme as antdTheme,
} from 'antd';
import type { MenuProps } from 'antd';
import {
  SearchOutlined,
  LinkOutlined,
  PlusOutlined,
  FilePdfOutlined,
  FileWordOutlined,
  FileExcelOutlined,
  FileImageOutlined,
  FileTextOutlined,
  SoundOutlined,
  VideoCameraOutlined,
  CalendarOutlined,
  DeleteOutlined,
  EditOutlined,
  EllipsisOutlined,
  FolderAddOutlined,
  ShareAltOutlined,
  MenuOutlined,
  FolderOutlined,
  InboxOutlined,
  AppstoreOutlined,
  GlobalOutlined,
} from '@ant-design/icons';
import { filesAPI, FolderResponse } from '../services/files';
import { fileTagsService } from '../services/fileTags';
import { tasksAPI, TaskExecution } from '../services/tasks';
import { getErrorMessage } from '../utils/errorHandler';
import { getModalStyles } from '../utils/modalStyles';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import { getExecutionStatusDisplay } from '../utils/rcAgentStatus';
import UploadFilesModal from '../components/UploadFilesModal';
import UploadAudioModal from '../components/UploadAudioModal';
import UploadYouTubeLinkModal from '../components/UploadYouTubeLinkModal';
import UploadWebLinkModal from '../components/UploadWebLinkModal';
import ReportTickerTags, { parseReportTickers } from '../components/ReportTickerTags';
import './MobileSourcesPage.css';

const { Text } = Typography;

const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    textMuted: 'rgba(0, 0, 0, 0.45)',
    codeBlockBg: '#f5f5f5',
    selectedBorder: '#1890ff',
    background: '#f5f5f5',
  },
  dark: {
    pageBg: '#1E202B',
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
    textMuted: '#A0A3AD',
    codeBlockBg: '#1E202B',
    selectedBorder: '#2196F3',
    background: '#151b26',
    inputBg: '#1E202B',
  },
} as const;

const EXECUTIONS_PAGE_SIZE = 20;

/** 与 PC Sources `getFileTypeTagConfig` 一致，彩色矢量图标 */
function getFileTypeIconDisplay(firstFileType: string | undefined): {
  tooltip: string;
  icon: React.ReactElement;
  color: string;
} {
  const t = (firstFileType || '').toLowerCase();
  if (['pdf'].includes(t)) {
    return { tooltip: 'PDF', icon: <FilePdfOutlined />, color: '#f5222d' };
  }
  if (['doc', 'docx', 'word'].includes(t)) {
    return { tooltip: 'Word', icon: <FileWordOutlined />, color: '#1890ff' };
  }
  if (['excel', 'xlsx', 'xls'].includes(t)) {
    return { tooltip: 'Excel', icon: <FileExcelOutlined />, color: '#52c41a' };
  }
  if (['audio'].includes(t)) {
    return { tooltip: 'Audio', icon: <SoundOutlined />, color: '#722ed1' };
  }
  if (['youtube'].includes(t)) {
    return { tooltip: 'YouTube', icon: <VideoCameraOutlined />, color: '#ff4d4f' };
  }
  if (['image', 'jpg', 'jpeg', 'png', 'gif', 'bmp'].includes(t)) {
    return { tooltip: 'Image', icon: <FileImageOutlined />, color: '#fa8c16' };
  }
  if (['web'].includes(t)) {
    return { tooltip: 'Web Clip', icon: <GlobalOutlined />, color: '#13c2c2' };
  }
  const label = (firstFileType || 'File').toUpperCase();
  return { tooltip: label, icon: <FileTextOutlined />, color: '#8c8c8c' };
}

const MobileSourcesInner: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const { modal, message } = App.useApp();
  const { registerSourceListRefresh } = useFileProcessing();
  const isDark = themeMode === 'dark';
  const themeColors = THEME_COLORS[themeMode];

  const [taskExecutions, setTaskExecutions] = useState<TaskExecution[]>([]);
  const [listLoading, setListLoading] = useState(true);
  const executionsPageRef = useRef(1);
  const [hasMoreExecutions, setHasMoreExecutions] = useState(true);
  const [loadingMoreExecutions, setLoadingMoreExecutions] = useState(false);

  const [searchType, setSearchType] = useState<'file_name' | 'content' | 'ticker'>('file_name');
  const [searchQuery, setSearchQuery] = useState('');
  const [tickerFilter, setTickerFilter] = useState<string | undefined>(undefined);
  const [tickerSearchOptions, setTickerSearchOptions] = useState<Array<{ value: string; label: string }>>([]);
  const tickerSearchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const searchTypeRef = useRef(searchType);
  const searchQueryRef = useRef(searchQuery);
  const tickerFilterRef = useRef<string | undefined>(undefined);
  const prevSearchTypeForReloadRef = useRef<'file_name' | 'content' | 'ticker'>(searchType);
  const selectedFolderIdRef = useRef<string | undefined>(undefined);

  const [folders, setFolders] = useState<FolderResponse[]>([]);
  const [folderDrawerOpen, setFolderDrawerOpen] = useState(false);
  const [creatingFolder, setCreatingFolder] = useState(false);
  const [newFolderName, setNewFolderName] = useState('');
  const [editingFolderId, setEditingFolderId] = useState<string | null>(null);
  const [editingFolderName, setEditingFolderName] = useState('');
  const [selectedFolderId, setSelectedFolderId] = useState<string | undefined>(undefined);
  const [batchMoveLoading, setBatchMoveLoading] = useState(false);

  const [batchMode, setBatchMode] = useState(false);
  const [selectedExecutionIds, setSelectedExecutionIds] = useState<string[]>([]);
  const [deletingExecutions, setDeletingExecutions] = useState(false);

  const [uploadSheetOpen, setUploadSheetOpen] = useState(false);
  const [showUploadDocModal, setShowUploadDocModal] = useState(false);
  const [showUploadAudioModal, setShowUploadAudioModal] = useState(false);
  const [showYouTubeModal, setShowYouTubeModal] = useState(false);
  const [showWebLinkModal, setShowWebLinkModal] = useState(false);

  const [shareModalVisible, setShareModalVisible] = useState(false);
  const [shareTargetExecution, setShareTargetExecution] = useState<TaskExecution | null>(null);
  const [shareableUsers, setShareableUsers] = useState<Array<{ id: number; username: string }>>([]);
  const [selectedShareUserIds, setSelectedShareUserIds] = useState<number[]>([]);
  const [sharingInProgress, setSharingInProgress] = useState(false);
  const [currentShares, setCurrentShares] = useState<
    Array<{ shared_to_user_id: number; shared_to_username: string }>
  >([]);

  const [renameModalVisible, setRenameModalVisible] = useState(false);
  const [executionToRename, setExecutionToRename] = useState<TaskExecution | null>(null);
  const [renameStem, setRenameStem] = useState('');
  const [renameExtension, setRenameExtension] = useState('');
  const [renameLoading, setRenameLoading] = useState(false);

  const [deleteExecutionVisible, setDeleteExecutionVisible] = useState(false);
  const [executionToDelete, setExecutionToDelete] = useState<TaskExecution | null>(null);

  const [detailExecution, setDetailExecution] = useState<TaskExecution | null>(null);
  const detailExecutionRef = useRef<TaskExecution | null>(null);

  const filesListScrollRef = useRef<HTMLDivElement>(null);
  const hasMoreExecutionsRef = useRef(hasMoreExecutions);
  const loadingMoreExecutionsRef = useRef(loadingMoreExecutions);
  const loadTaskExecutionsFuncRef = useRef<(showSpinner: boolean, append: boolean) => Promise<void>>(async () => {});

  useEffect(() => {
    hasMoreExecutionsRef.current = hasMoreExecutions;
    loadingMoreExecutionsRef.current = loadingMoreExecutions;
  }, [hasMoreExecutions, loadingMoreExecutions]);

  useEffect(() => {
    searchTypeRef.current = searchType;
    searchQueryRef.current = searchQuery;
    tickerFilterRef.current = tickerFilter;
  }, [searchType, searchQuery, tickerFilter]);

  useEffect(() => {
    selectedFolderIdRef.current = selectedFolderId;
  }, [selectedFolderId]);

  useEffect(() => {
    detailExecutionRef.current = detailExecution;
  }, [detailExecution]);

  const loadTaskExecutionsFunc = useCallback(async (showSpinner: boolean, append: boolean) => {
    if (append && (!hasMoreExecutionsRef.current || loadingMoreExecutionsRef.current)) return;
    try {
      if (append) setLoadingMoreExecutions(true);
      else if (showSpinner) setListLoading(true);

      const pageToLoad = append ? executionsPageRef.current + 1 : 1;
      const st = searchTypeRef.current;
      const sq = searchQueryRef.current.trim();
      const tf = tickerFilterRef.current;
      const useContentApi = st === 'content' && sq.length > 0;
      const useTickerApi = st === 'ticker' && !!tf;
      const fid = selectedFolderIdRef.current;

      const executions = await tasksAPI.getTaskExecutions({
        page: pageToLoad,
        page_size: EXECUTIONS_PAGE_SIZE,
        ...(useContentApi ? { search_type: 'content', q: sq } : {}),
        ...(useTickerApi && tf ? { ticker: tf } : {}),
        ...(fid ? { folder_id: fid } : {}),
      });

      if (append) {
        setTaskExecutions(prev => [...prev, ...executions]);
        executionsPageRef.current = pageToLoad;
      } else {
        setTaskExecutions(executions);
        executionsPageRef.current = 1;
      }
      setHasMoreExecutions(executions.length === EXECUTIONS_PAGE_SIZE);
    } catch (e) {
      console.error(e);
      message.error('Failed to load sources');
    } finally {
      setLoadingMoreExecutions(false);
      setListLoading(false);
    }
  }, [message]);

  const loadFolders = useCallback(async () => {
    try {
      const data = await filesAPI.getFolders();
      setFolders(data);
    } catch (e) {
      console.error('Failed to load folders:', e);
    }
  }, []);

  const handleCreateFolder = useCallback(async () => {
    const name = newFolderName.trim();
    if (!name) return;
    try {
      await filesAPI.createFolder(name);
      setNewFolderName('');
      setCreatingFolder(false);
      await loadFolders();
    } catch (e: unknown) {
      message.error(getErrorMessage(e, 'Failed to create folder'));
    }
  }, [newFolderName, loadFolders, message]);

  const handleRenameFolder = useCallback(async (folderId: number) => {
    const name = editingFolderName.trim();
    if (!name) return;
    try {
      await filesAPI.updateFolder(folderId, { name });
      setEditingFolderId(null);
      setEditingFolderName('');
      await loadFolders();
    } catch (e: unknown) {
      message.error(getErrorMessage(e, 'Failed to rename folder'));
    }
  }, [editingFolderName, loadFolders, message]);

  const handleDeleteFolder = useCallback(
    async (folderId: number) => {
      try {
        await filesAPI.deleteFolder(folderId);
        if (selectedFolderId === String(folderId)) {
          setSelectedFolderId(undefined);
        }
        await loadFolders();
        loadTaskExecutionsFuncRef.current(false, false).catch(() => {});
      } catch (e: unknown) {
        message.error(getErrorMessage(e, 'Failed to delete folder'));
      }
    },
    [selectedFolderId, loadFolders, message],
  );

  const handleMoveToFolder = useCallback(
    async (executionId: string, folderId: number | null) => {
      try {
        await filesAPI.moveExecutionToFolder(Number(executionId), folderId);
        await loadFolders();
        loadTaskExecutionsFuncRef.current(false, false).catch(() => {});
        message.success('Moved successfully');
      } catch (e: unknown) {
        const err = e as { response?: unknown };
        if (!err?.response) {
          message.error(getErrorMessage(e, 'Failed to move file'));
        }
      }
    },
    [loadFolders, message],
  );

  const handleBatchMoveToFolder = useCallback(
    async (folderId: number | null) => {
      if (selectedExecutionIds.length === 0) {
        message.warning('Select items first');
        return;
      }
      try {
        setBatchMoveLoading(true);
        await filesAPI.batchMoveToFolder(selectedExecutionIds.map(Number), folderId);
        await loadFolders();
        loadTaskExecutionsFuncRef.current(false, false).catch(() => {});
        message.success('Moved successfully');
        setSelectedExecutionIds([]);
        setBatchMode(false);
      } catch (e: unknown) {
        message.error(getErrorMessage(e, 'Move failed'));
      } finally {
        setBatchMoveLoading(false);
      }
    },
    [selectedExecutionIds, loadFolders, message],
  );

  const folderFilterSummary = useMemo(() => {
    if (!selectedFolderId) return 'All';
    if (selectedFolderId === 'uncategorized') return 'Uncategorized';
    const f = folders.find(x => String(x.id) === selectedFolderId);
    return f ? f.name : selectedFolderId;
  }, [selectedFolderId, folders]);

  const handleDetailRefreshList = useCallback(async () => {
    await loadTaskExecutionsFunc(false, false);
    const id = detailExecutionRef.current?.id;
    if (!id) return;
    try {
      const fresh = await tasksAPI.getTaskExecution(id);
      setDetailExecution(fresh);
    } catch {
      setDetailExecution(null);
    }
  }, [loadTaskExecutionsFunc]);

  useEffect(() => {
    loadTaskExecutionsFuncRef.current = loadTaskExecutionsFunc;
  }, [loadTaskExecutionsFunc]);

  useEffect(() => {
    loadFolders();
  }, [loadFolders]);

  useEffect(() => {
    loadTaskExecutionsFunc(true, false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const isFolderMountRef = useRef(true);
  useEffect(() => {
    if (isFolderMountRef.current) {
      isFolderMountRef.current = false;
      return;
    }
    const t = setTimeout(() => {
      loadTaskExecutionsFuncRef.current(false, false).catch(() => {});
    }, 50);
    return () => clearTimeout(t);
  }, [selectedFolderId]);

  /** 内容搜索：切换类型或关键词时重新拉取（防抖与 PC Sources 一致） */
  useEffect(() => {
    if (searchType !== 'content') return;
    const delay = searchQuery.trim() ? 400 : 0;
    const t = setTimeout(() => {
      loadTaskExecutionsFuncRef.current(false, false);
    }, delay);
    return () => clearTimeout(t);
  }, [searchType, searchQuery]);

  /** 内容/ticker 与标题本地过滤切换时：恢复完整列表供标题模式或清空服务端筛选 */
  useEffect(() => {
    const prev = prevSearchTypeForReloadRef.current;
    prevSearchTypeForReloadRef.current = searchType;
    if (
      (prev === 'content' && (searchType === 'file_name' || searchType === 'ticker')) ||
      (prev === 'ticker' && (searchType === 'file_name' || searchType === 'content'))
    ) {
      loadTaskExecutionsFuncRef.current(true, false);
    }
  }, [searchType]);

  /** ticker 选中/清除后刷新（与 PC Sources 一致，跳过一次初始 effect） */
  const isTickerMountRef = useRef(true);
  useEffect(() => {
    if (isTickerMountRef.current) {
      isTickerMountRef.current = false;
      return;
    }
    const t = setTimeout(() => {
      loadTaskExecutionsFuncRef.current(false, false);
    }, 50);
    return () => clearTimeout(t);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tickerFilter]);

  useEffect(() => {
    const unregister = registerSourceListRefresh(() => {
      loadTaskExecutionsFuncRef.current(false, false).catch(() => {});
    });
    return unregister;
  }, [registerSourceListRefresh]);

  const handleFilesListScroll = useCallback(() => {
    const el = filesListScrollRef.current;
    if (!el || !hasMoreExecutionsRef.current || loadingMoreExecutionsRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = el;
    if (scrollTop + clientHeight >= scrollHeight - 80) {
      loadTaskExecutionsFuncRef.current(false, true);
    }
  }, []);

  useEffect(() => {
    const el = filesListScrollRef.current;
    if (!el) return;
    el.addEventListener('scroll', handleFilesListScroll);
    return () => el.removeEventListener('scroll', handleFilesListScroll);
  }, [handleFilesListScroll]);

  const q = searchQuery.trim().toLowerCase();
  const filteredExecutions =
    searchType === 'file_name' && q
      ? taskExecutions.filter(exec => (exec.first_filename || '').toLowerCase().includes(q))
      : taskExecutions;

  const handleOpenShareModal = async (execution: TaskExecution) => {
    setShareTargetExecution(execution);
    setSelectedShareUserIds([]);
    setShareModalVisible(true);
    try {
      const [users, sharesData] = await Promise.all([
        filesAPI.getShareableUsers(),
        (async () => {
          const detailResp = await tasksAPI.getTaskExecutionDetails(execution.id);
          const fileId = detailResp?.files?.[0]?.id;
          if (fileId) return filesAPI.getFileShares(fileId);
          return { file_id: 0, shares: [] };
        })(),
      ]);
      setShareableUsers(users);
      setCurrentShares(sharesData.shares as any);
    } catch {
      setShareableUsers([]);
      setCurrentShares([]);
    }
  };

  const handleConfirmShare = async () => {
    if (!shareTargetExecution) return;
    if (selectedShareUserIds.length === 0) {
      setShareModalVisible(false);
      handleDetailRefreshList().catch(() => {});
      return;
    }
    setSharingInProgress(true);
    try {
      const detailResp = await tasksAPI.getTaskExecutionDetails(shareTargetExecution.id);
      const fileId = detailResp?.files?.[0]?.id;
      if (!fileId) {
        message.error('No file found for this source');
        return;
      }
      const result = await filesAPI.shareFile(fileId, selectedShareUserIds);
      message.success(`Shared to ${result.shared_count} user(s)`);
      setShareModalVisible(false);
      await handleDetailRefreshList();
    } catch {
      message.error('Failed to share');
    } finally {
      setSharingInProgress(false);
    }
  };

  const handleRemoveShare = async (fileId: number, userId: number) => {
    try {
      await filesAPI.unshareFile(fileId, [userId]);
      setCurrentShares(prev => prev.filter(s => s.shared_to_user_id !== userId));
      message.success('Share removed');
      handleDetailRefreshList().catch(() => {});
    } catch {
      message.error('Failed to remove share');
    }
  };

  const handleOpenRename = (execution: TaskExecution) => {
    setExecutionToRename(execution);
    const raw = execution.first_filename || 'Untitled';
    const { stem, extension } = filesAPI.splitFilenameStemAndExtension(raw);
    setRenameStem(stem);
    setRenameExtension(extension);
    setRenameModalVisible(true);
  };

  const handleConfirmRename = async () => {
    if (!executionToRename || !renameStem.trim()) return;
    const newFilename = `${renameStem.trim()}${renameExtension}`;
    try {
      setRenameLoading(true);
      await tasksAPI.renameExecutionFile(executionToRename.id, newFilename);
      setTaskExecutions(prev =>
        prev.map(exec =>
          exec.id === executionToRename.id ? { ...exec, first_filename: newFilename } : exec
        )
      );
      message.success('Renamed');
      setRenameModalVisible(false);
      setExecutionToRename(null);
      setRenameStem('');
      setRenameExtension('');
    } catch (error: unknown) {
      message.error(getErrorMessage(error, 'Rename failed'));
    } finally {
      setRenameLoading(false);
    }
  };

  const handleDeleteExecution = (execution: TaskExecution) => {
    setExecutionToDelete(execution);
    setDeleteExecutionVisible(true);
  };

  const confirmDeleteExecution = async () => {
    if (!executionToDelete) return;
    try {
      setDeletingExecutions(true);
      await tasksAPI.deleteTaskExecution(executionToDelete.id);
      setTaskExecutions(prev => prev.filter(exec => exec.id !== executionToDelete.id));
      setSelectedExecutionIds(prev => prev.filter(id => id !== executionToDelete.id));
      setDetailExecution(prev => (prev?.id === executionToDelete.id ? null : prev));
      message.success('Deleted');
      setDeleteExecutionVisible(false);
      setExecutionToDelete(null);
    } catch (error: unknown) {
      message.error(getErrorMessage(error, 'Delete failed'));
    } finally {
      setDeletingExecutions(false);
    }
  };

  const handleBatchDeleteExecutions = () => {
    if (selectedExecutionIds.length === 0) {
      message.warning('Select items first');
      return;
    }
    modal.confirm({
      title: 'Delete selected?',
      content: `Permanently delete ${selectedExecutionIds.length} item(s)? This cannot be undone.`,
      okText: 'Delete',
      cancelText: 'Cancel',
      okType: 'danger',
      centered: true,
      styles: getModalStyles(themeMode),
      onOk: async () => {
        try {
          setDeletingExecutions(true);
          const BATCH_SIZE = 10;
          const ids = [...selectedExecutionIds];
          const succeeded: string[] = [];
          for (let i = 0; i < ids.length; i += BATCH_SIZE) {
            const batch = ids.slice(i, i + BATCH_SIZE);
            const results = await Promise.allSettled(batch.map(id => tasksAPI.deleteTaskExecution(id)));
            results.forEach((result, idx) => {
              if (result.status === 'fulfilled') succeeded.push(batch[idx]);
            });
          }
          setTaskExecutions(prev => prev.filter(exec => !succeeded.includes(exec.id)));
          setSelectedExecutionIds([]);
          setBatchMode(false);
          setDetailExecution(prev => (prev && succeeded.includes(prev.id) ? null : prev));
          message.success(`Deleted ${succeeded.length} item(s)`);
        } catch {
          message.error('Batch delete failed');
        } finally {
          setDeletingExecutions(false);
        }
      },
    });
  };

  /** 与 PC `Layout.tsx` 上传选项 Modal 的遮罩一致 */
  const uploadOptionsMaskStyle: React.CSSProperties = {
    background: isDark ? 'rgba(0, 0, 0, 0.6)' : 'rgba(0, 0, 0, 0.25)',
    backdropFilter: 'blur(6px)',
    WebkitBackdropFilter: 'blur(6px)',
  };

  const uploadOptionButtonStyle: React.CSSProperties = {
    height: '52px',
    fontSize: '15px',
    fontWeight: 500,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-start',
    paddingLeft: '20px',
    background: isDark ? '#212936' : '#fafafa',
    color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
    border: 'none',
    borderRadius: '26px',
    transition: 'all 0.2s',
  };

  const pageSurfaceStyle = {
    background: themeColors.pageBg,
    // 与工具栏、卡片、默认 Button 描边一致（暗色下避免沿用 --ant-color-border 过亮）
    '--mobile-sources-control-border': themeColors.cardBorder,
    '--mobile-sources-control-bg': themeColors.cardBg,
  } as React.CSSProperties;

  return (
    <div
      className={`mobile-sources-page${isDark ? ' mobile-sources-page--dark' : ''}`}
      style={pageSurfaceStyle}
    >
      <div
        className="mobile-sources-toolbar"
        style={{
          background: themeColors.cardBg,
          borderBottom: `1px solid ${themeColors.cardBorder}`,
        }}
      >
        <div className="mobile-sources-toolbar-top">
          <Button
            type="text"
            icon={<MenuOutlined style={{ fontSize: 18 }} />}
            onClick={() => setFolderDrawerOpen(true)}
            aria-label="Folders"
            style={{ color: themeColors.textSecondary }}
          />
          <div className="mobile-sources-toolbar-title-block">
            <div className="mobile-sources-toolbar-title" style={{ color: themeColors.text }}>
              Sources
            </div>
            <div className="mobile-sources-toolbar-sub" style={{ color: themeColors.textSecondary }}>
              {folderFilterSummary}
            </div>
          </div>
          <Button
            type={batchMode ? 'primary' : 'default'}
            shape="circle"
            className="mobile-sources-batch-btn"
            icon={<FolderAddOutlined />}
            onClick={() => {
              setBatchMode(v => !v);
              if (batchMode) setSelectedExecutionIds([]);
            }}
            aria-label={batchMode ? 'Exit batch mode' : 'Batch operations'}
            title={batchMode ? 'Exit batch mode' : 'Batch operations'}
          />
        </div>
        <div className="mobile-sources-toolbar-input-row">
          <Space.Compact block className="mobile-sources-search-compact">
            <Segmented
              size="middle"
              className="mobile-sources-segmented-in-search mobile-sources-segmented-three"
              value={searchType}
              onChange={v => {
                const next = v as 'file_name' | 'content' | 'ticker';
                setSearchType(next);
                if (next === 'ticker') {
                  setSearchQuery('');
                } else {
                  setTickerFilter(undefined);
                  setTickerSearchOptions([]);
                }
              }}
              options={[
                { label: 'Title', value: 'file_name' },
                { label: 'Content', value: 'content' },
                { label: 'Ticker', value: 'ticker' },
              ]}
            />
            {searchType === 'ticker' ? (
              <AutoComplete
                className="mobile-sources-toolbar-search mobile-sources-toolbar-ticker"
                allowClear
                size="middle"
                placeholder="Search ticker..."
                options={tickerSearchOptions}
                value={tickerFilter}
                onSearch={text => {
                  if (tickerSearchTimerRef.current) clearTimeout(tickerSearchTimerRef.current);
                  if (!text.trim()) {
                    setTickerSearchOptions([]);
                    return;
                  }
                  tickerSearchTimerRef.current = setTimeout(() => {
                    fileTagsService
                      .getDistinctTickers(text.trim())
                      .then(results => {
                        setTickerSearchOptions(
                          results.map(r => ({
                            value: r.ticker,
                            label: `${r.ticker} (${r.file_count})`,
                          })),
                        );
                      })
                      .catch(() => {});
                  }, 300);
                }}
                onSelect={(val: string) => setTickerFilter(val)}
                onChange={(val: string) => {
                  if (!val) {
                    setTickerFilter(undefined);
                    setTickerSearchOptions([]);
                  }
                }}
                classNames={{ popup: { root: 'mobile-sources-ticker-autocomplete-popup' } }}
              />
            ) : (
              <Input
                className="mobile-sources-toolbar-search"
                allowClear
                placeholder={searchType === 'content' ? 'Search in content' : 'Search by title'}
                value={searchQuery}
                onChange={e => setSearchQuery(e.target.value)}
                prefix={<SearchOutlined style={{ color: themeColors.textSecondary }} />}
                size="middle"
              />
            )}
          </Space.Compact>
        </div>
      </div>

      {batchMode && selectedExecutionIds.length > 0 && (
        <div className="mobile-sources-batch-bar">
          <span>{selectedExecutionIds.length} selected</span>
          <Space size={8}>
            <Dropdown
              menu={{
                items: [
                  {
                    key: 'uncat',
                    icon: <InboxOutlined />,
                    label: 'Uncategorized',
                    onClick: () => {
                      void handleBatchMoveToFolder(null);
                    },
                  },
                  ...folders.map(f => ({
                    key: `f-${f.id}`,
                    icon: <FolderOutlined />,
                    label: f.name,
                    onClick: () => {
                      void handleBatchMoveToFolder(f.id);
                    },
                  })),
                ],
              }}
              trigger={['click']}
            >
              <Button size="small" loading={batchMoveLoading} icon={<FolderOutlined />}>
                Move
              </Button>
            </Dropdown>
            <Button
              type="primary"
              danger
              size="small"
              loading={deletingExecutions}
              icon={<DeleteOutlined />}
              onClick={handleBatchDeleteExecutions}
            >
              Delete
            </Button>
          </Space>
        </div>
      )}

      <Spin spinning={listLoading}>
        <div
          ref={filesListScrollRef}
          className="mobile-sources-list"
          style={{ maxHeight: 'calc(100vh - 268px)', overflowY: 'auto' }}
        >
          {filteredExecutions.length === 0 && !listLoading ? (
            <Empty
              description={
                taskExecutions.length === 0
                  ? 'No sources yet'
                  : searchType === 'content'
                    ? 'No matching content'
                    : searchType === 'ticker'
                      ? 'No matching ticker'
                      : 'No matching titles'
              }
              style={{ marginTop: 48 }}
            />
          ) : (
            filteredExecutions.map(execution => {
              const statusDisplay = getExecutionStatusDisplay(execution);
              const fileTypeDisplay = getFileTypeIconDisplay(execution.first_file_type);
              const isChecked = selectedExecutionIds.includes(execution.id);
              const isSharedFile = execution.access_type === 'shared';

              const menuItems: MenuProps['items'] = [
                {
                  key: 'share',
                  icon: <ShareAltOutlined />,
                  label: 'Share',
                  onClick: e => {
                    e.domEvent.stopPropagation();
                    handleOpenShareModal(execution);
                  },
                },
                {
                  key: 'rename',
                  icon: <EditOutlined />,
                  label: 'Rename',
                  onClick: e => {
                    e.domEvent.stopPropagation();
                    handleOpenRename(execution);
                  },
                },
                ...(!isSharedFile
                  ? [
                      {
                        key: 'move-to-folder',
                        icon: <FolderOutlined />,
                        label: 'Move to Folder',
                        children: [
                          {
                            key: 'move-to-root',
                            icon: <InboxOutlined />,
                            label: 'Uncategorized',
                            onClick: (e: { domEvent: { stopPropagation: () => void } }) => {
                              e.domEvent.stopPropagation();
                              void handleMoveToFolder(execution.id, null);
                            },
                          },
                          ...folders.map(f => ({
                            key: `move-to-${f.id}`,
                            icon: <FolderOutlined />,
                            label: f.name,
                            onClick: (e: { domEvent: { stopPropagation: () => void } }) => {
                              e.domEvent.stopPropagation();
                              void handleMoveToFolder(execution.id, f.id);
                            },
                          })),
                        ],
                      },
                      {
                        key: 'delete',
                        icon: <DeleteOutlined />,
                        label: 'Delete',
                        danger: true,
                        onClick: (e: { domEvent: { stopPropagation: () => void } }) => {
                          e.domEvent.stopPropagation();
                          handleDeleteExecution(execution);
                        },
                      },
                    ]
                  : []),
              ];

              return (
                <div
                  key={execution.id}
                  role={batchMode ? undefined : 'button'}
                  tabIndex={batchMode ? undefined : 0}
                  className={`mobile-sources-card${!batchMode ? ' mobile-sources-card--openable' : ''}`}
                  style={{
                    background: themeColors.cardBg,
                    borderColor: themeColors.cardBorder,
                  }}
                  onClick={e => {
                    if (batchMode) return;
                    // Portal 菜单事件会沿 React 树冒泡到卡片，需忽略下拉内点击（如 Move to Folder 子菜单）
                    const el = e.target as HTMLElement;
                    if (el.closest?.('.ant-dropdown')) return;
                    setDetailExecution(execution);
                  }}
                  onKeyDown={e => {
                    if (batchMode) return;
                    if (e.key === 'Enter' || e.key === ' ') {
                      e.preventDefault();
                      setDetailExecution(execution);
                    }
                  }}
                >
                  <div className="mobile-sources-card-header">
                    {batchMode && (
                      <Checkbox
                        checked={isChecked}
                        onChange={e => {
                          if (e.target.checked) {
                            setSelectedExecutionIds(prev => [...prev, execution.id]);
                          } else {
                            setSelectedExecutionIds(prev => prev.filter(id => id !== execution.id));
                          }
                        }}
                        onClick={e => e.stopPropagation()}
                      />
                    )}
                    <div className="mobile-sources-card-title" style={{ color: themeColors.text }}>
                      {execution.first_filename || 'Untitled'}
                    </div>
                    <Dropdown menu={{ items: menuItems }} trigger={['click']} placement="bottomRight">
                      <Button type="text" size="small" icon={<EllipsisOutlined />} onClick={e => e.stopPropagation()} />
                    </Dropdown>
                  </div>
                  <div className="mobile-sources-card-meta" style={{ color: themeColors.textSecondary }}>
                    <div className="mobile-sources-meta-row">
                      <CalendarOutlined />
                      <span>{new Date(execution.created_at).toLocaleString()}</span>
                    </div>
                  </div>
                  <div className="mobile-sources-card-footer">
                    <div className="mobile-sources-card-footer-left">
                      <Tooltip title={fileTypeDisplay.tooltip}>
                        <span
                          className="mobile-sources-file-type-icon"
                          style={{ color: fileTypeDisplay.color }}
                          aria-label={fileTypeDisplay.tooltip}
                        >
                          {fileTypeDisplay.icon}
                        </span>
                      </Tooltip>
                      <div className="mobile-sources-status-wrap">
                        <Tooltip title={statusDisplay.tooltip}>
                          <span
                            className="mobile-sources-status-text"
                            style={{ color: themeColors.textSecondary }}
                            aria-label={statusDisplay.label}
                            onClick={e => e.stopPropagation()}
                          >
                            {statusDisplay.label}
                          </span>
                        </Tooltip>
                      </div>
                    </div>
                    {execution.share_info && (
                      <div className="mobile-sources-card-footer-share">
                        <Tooltip title={`Shared by ${execution.share_info.shared_by.username}`}>
                          {execution.share_info.shared_by.avatar_url ? (
                            <Avatar size={18} src={execution.share_info.shared_by.avatar_url} />
                          ) : (
                            <Avatar size={18} style={{ backgroundColor: '#722ed1', fontSize: 10 }}>
                              {execution.share_info.shared_by.username.charAt(0).toUpperCase()}
                            </Avatar>
                          )}
                        </Tooltip>
                        <span
                          className="mobile-sources-share-divider"
                          style={{ color: themeColors.textSecondary }}
                          aria-hidden
                        >
                          |
                        </span>
                        <Avatar.Group
                          size={18}
                          max={{
                            count: 2,
                            style: { backgroundColor: '#722ed1', fontSize: 9 },
                          }}
                        >
                          {execution.share_info.shared_to.map(u => (
                            <Tooltip key={u.user_id} title={u.username}>
                              {u.avatar_url ? (
                                <Avatar size={18} src={u.avatar_url} />
                              ) : (
                                <Avatar size={18} style={{ backgroundColor: '#1890ff', fontSize: 10 }}>
                                  {u.username.charAt(0).toUpperCase()}
                                </Avatar>
                              )}
                            </Tooltip>
                          ))}
                        </Avatar.Group>
                      </div>
                    )}
                  </div>
                  {parseReportTickers(execution.ticker_tags).length > 0 && (
                    <div className="mobile-sources-card-tickers">
                      <ReportTickerTags tickers={execution.ticker_tags} maxVisible={3} />
                    </div>
                  )}
                </div>
              );
            })
          )}
          {loadingMoreExecutions && (
            <div className="mobile-sources-loading-more">Loading more…</div>
          )}
        </div>
      </Spin>

      <Drawer
        title={<span style={{ color: themeColors.text }}>Folders</span>}
        placement="left"
        width="min(100%, 360px)"
        onClose={() => setFolderDrawerOpen(false)}
        open={folderDrawerOpen}
        rootClassName={isDark ? 'mobile-sources-folder-drawer--dark' : 'mobile-sources-folder-drawer--light'}
        styles={{
          content: { background: themeColors.cardBg },
          header: {
            background: themeColors.cardBg,
            borderBottom: `1px solid ${themeColors.cardBorder}`,
          },
          body: {
            background: themeColors.cardBg,
            paddingTop: 8,
            paddingLeft: 16,
            paddingRight: 16,
            paddingBottom: 24,
            overflowY: 'auto',
          },
          ...(isDark ? { mask: { background: 'rgba(0, 0, 0, 0.65)' } } : {}),
        }}
      >
        <div className="mobile-sources-folder-drawer-hint" style={{ color: themeColors.textSecondary, fontSize: 12, marginBottom: 10 }}>
          Filter by folder
        </div>
        <button
          type="button"
          className={`mobile-sources-folder-row${!selectedFolderId ? ' is-active' : ''}`}
          style={{ color: themeColors.text }}
          onClick={() => {
            setSelectedFolderId(undefined);
            setFolderDrawerOpen(false);
          }}
        >
          <AppstoreOutlined />
          <span className="mobile-sources-folder-row-label">All</span>
        </button>
        <button
          type="button"
          className={`mobile-sources-folder-row${selectedFolderId === 'uncategorized' ? ' is-active' : ''}`}
          style={{ color: themeColors.text }}
          onClick={() => {
            setSelectedFolderId('uncategorized');
            setFolderDrawerOpen(false);
          }}
        >
          <InboxOutlined />
          <span className="mobile-sources-folder-row-label">Uncategorized</span>
        </button>
        {folders.map(folder => (
          <div key={folder.id} className="mobile-sources-folder-row-wrap">
            {editingFolderId === String(folder.id) ? (
              <Space.Compact style={{ width: '100%' }}>
                <Input
                  value={editingFolderName}
                  onChange={e => setEditingFolderName(e.target.value)}
                  onPressEnter={() => void handleRenameFolder(folder.id)}
                />
                <Button type="primary" size="small" onClick={() => void handleRenameFolder(folder.id)}>
                  Save
                </Button>
                <Button
                  size="small"
                  onClick={() => {
                    setEditingFolderId(null);
                    setEditingFolderName('');
                  }}
                >
                  Cancel
                </Button>
              </Space.Compact>
            ) : (
              <div className="mobile-sources-folder-row-inner">
                <button
                  type="button"
                  className={`mobile-sources-folder-row${selectedFolderId === String(folder.id) ? ' is-active' : ''}`}
                  style={{ color: themeColors.text }}
                  onClick={() => {
                    setSelectedFolderId(String(folder.id));
                    setFolderDrawerOpen(false);
                  }}
                >
                  <FolderOutlined />
                  <span className="mobile-sources-folder-row-name">{folder.name}</span>
                  <span className="mobile-sources-folder-row-count" style={{ color: themeColors.textSecondary }}>
                    {folder.file_count}
                  </span>
                </button>
                <Dropdown
                  menu={{
                    items: [
                      {
                        key: 'ren',
                        label: 'Rename',
                        icon: <EditOutlined />,
                        onClick: () => {
                          setEditingFolderId(String(folder.id));
                          setEditingFolderName(folder.name);
                        },
                      },
                      {
                        key: 'del',
                        label: 'Delete',
                        danger: true,
                        icon: <DeleteOutlined />,
                        onClick: () => {
                          modal.confirm({
                            title: 'Delete folder?',
                            content: 'Files remain in your library as uncategorized.',
                            okText: 'Delete',
                            okType: 'danger',
                            cancelText: 'Cancel',
                            centered: true,
                            styles: getModalStyles(themeMode),
                            onOk: () => handleDeleteFolder(folder.id),
                          });
                        },
                      },
                    ],
                  }}
                  trigger={['click']}
                >
                  <Button type="text" size="small" icon={<EllipsisOutlined />} aria-label="Folder actions" />
                </Dropdown>
              </div>
            )}
          </div>
        ))}
        {creatingFolder ? (
          <Space.Compact style={{ width: '100%', marginTop: 16 }}>
            <Input
              placeholder="Folder name"
              value={newFolderName}
              onChange={e => setNewFolderName(e.target.value)}
              onPressEnter={() => void handleCreateFolder()}
            />
            <Button type="primary" onClick={() => void handleCreateFolder()}>
              Add
            </Button>
            <Button
              onClick={() => {
                setCreatingFolder(false);
                setNewFolderName('');
              }}
            >
              Cancel
            </Button>
          </Space.Compact>
        ) : (
          <Button
            type="dashed"
            block
            icon={<PlusOutlined />}
            onClick={() => setCreatingFolder(true)}
            style={{ marginTop: 16 }}
          >
            New folder
          </Button>
        )}
      </Drawer>

      {!detailExecution && (
        <button
          type="button"
          className="mobile-sources-fab"
          aria-label="Upload"
          onClick={() => setUploadSheetOpen(true)}
          style={{
            background: isDark
              ? 'linear-gradient(135deg, #1890ff 0%, #096dd9 100%)'
              : 'linear-gradient(135deg, #40a9ff 0%, #1890ff 100%)',
            boxShadow: '2px 2px 12px rgba(24, 144, 255, 0.35)',
          }}
        >
          <PlusOutlined style={{ fontSize: 22 }} />
        </button>
      )}

      {detailExecution && (
        <MobileSourceFileDetail
          execution={detailExecution}
          themeColors={themeColors}
          isDark={isDark}
          onClose={() => setDetailExecution(null)}
          onRefreshList={handleDetailRefreshList}
        />
      )}

      <Modal
        open={uploadSheetOpen}
        onCancel={() => setUploadSheetOpen(false)}
        footer={null}
        closable={false}
        width={360}
        centered
        className="mobile-sources-upload-sheet"
        maskStyle={uploadOptionsMaskStyle}
        styles={{
          ...getModalStyles(themeMode),
          body: { ...getModalStyles(themeMode).body, padding: '24px 16px' },
        }}
      >
        {/* 与 PC `Layout.tsx` 上传选项一致：Upload Files / Upload Audio / YouTube Link / Web Link */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
          <Button
            type="text"
            size="large"
            icon={<FileTextOutlined style={{ fontSize: '18px' }} />}
            onClick={() => {
              setUploadSheetOpen(false);
              setShowUploadDocModal(true);
            }}
            style={uploadOptionButtonStyle}
            onMouseEnter={e => {
              e.currentTarget.style.background = isDark ? '#2a3544' : '#e6f7ff';
            }}
            onMouseLeave={e => {
              e.currentTarget.style.background = isDark ? '#212936' : '#fafafa';
            }}
          >
            <span style={{ marginLeft: '12px' }}>Upload Files</span>
          </Button>
          <Button
            type="text"
            size="large"
            icon={<SoundOutlined style={{ fontSize: '18px' }} />}
            onClick={() => {
              setUploadSheetOpen(false);
              setShowUploadAudioModal(true);
            }}
            style={uploadOptionButtonStyle}
            onMouseEnter={e => {
              e.currentTarget.style.background = isDark ? '#2a3544' : '#e6f7ff';
            }}
            onMouseLeave={e => {
              e.currentTarget.style.background = isDark ? '#212936' : '#fafafa';
            }}
          >
            <span style={{ marginLeft: '12px' }}>Upload Audio</span>
          </Button>
          <Button
            type="text"
            size="large"
            icon={<LinkOutlined style={{ fontSize: '18px' }} />}
            onClick={() => {
              setUploadSheetOpen(false);
              setShowYouTubeModal(true);
            }}
            style={uploadOptionButtonStyle}
            onMouseEnter={e => {
              e.currentTarget.style.background = isDark ? '#2a3544' : '#e6f7ff';
            }}
            onMouseLeave={e => {
              e.currentTarget.style.background = isDark ? '#212936' : '#fafafa';
            }}
          >
            <span style={{ marginLeft: '12px' }}>YouTube Link</span>
          </Button>
          <Button
            type="text"
            size="large"
            icon={<GlobalOutlined style={{ fontSize: '18px' }} />}
            onClick={() => {
              setUploadSheetOpen(false);
              setShowWebLinkModal(true);
            }}
            style={uploadOptionButtonStyle}
            onMouseEnter={e => {
              e.currentTarget.style.background = isDark ? '#2a3544' : '#e6f7ff';
            }}
            onMouseLeave={e => {
              e.currentTarget.style.background = isDark ? '#212936' : '#fafafa';
            }}
          >
            <span style={{ marginLeft: '12px' }}>Web Link</span>
          </Button>
        </div>
      </Modal>

      <UploadFilesModal
        visible={showUploadDocModal}
        onCancel={() => setShowUploadDocModal(false)}
        themeMode={themeMode}
        compactDocumentProcessing
      />
      <UploadAudioModal
        visible={showUploadAudioModal}
        onCancel={() => setShowUploadAudioModal(false)}
        themeMode={themeMode}
      />
      <UploadYouTubeLinkModal
        visible={showYouTubeModal}
        onCancel={() => setShowYouTubeModal(false)}
        themeMode={themeMode}
      />
      <UploadWebLinkModal
        visible={showWebLinkModal}
        onCancel={() => setShowWebLinkModal(false)}
        themeMode={themeMode}
      />

      <Modal
        title={<><ShareAltOutlined /> Share</>}
        open={shareModalVisible}
        onCancel={() => setShareModalVisible(false)}
        onOk={handleConfirmShare}
        okText={selectedShareUserIds.length > 0 ? 'Share' : 'Done'}
        cancelText="cancel"
        confirmLoading={sharingInProgress}
        width={400}
        centered
        styles={getModalStyles(themeMode)}
      >
        <div style={{ marginBottom: 12 }}>
          <Text type="secondary">File: </Text>
          <Text>{shareTargetExecution?.first_filename || '—'}</Text>
        </div>
        <Select
          mode="multiple"
          placeholder="Users to share with"
          style={{ width: '100%' }}
          value={selectedShareUserIds}
          onChange={setSelectedShareUserIds}
          optionFilterProp="label"
          options={shareableUsers
            .filter(u => !currentShares.some(s => s.shared_to_user_id === u.id))
            .map(u => ({ value: u.id, label: u.username }))}
        />
        {currentShares.length > 0 && (
          <div style={{ marginTop: 12 }}>
            <Text strong style={{ display: 'block', marginBottom: 8 }}>
              Shared with:
            </Text>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
              {currentShares.map(share => (
                <Tag
                  key={share.shared_to_user_id}
                  closable={shareTargetExecution?.access_type !== 'shared'}
                  onClose={e => {
                    e.preventDefault();
                    const run = async () => {
                      if (!shareTargetExecution) return;
                      const detail = await tasksAPI.getTaskExecutionDetails(shareTargetExecution.id);
                      const fid = detail?.files?.[0]?.id;
                      if (fid) await handleRemoveShare(fid, share.shared_to_user_id);
                    };
                    void run();
                  }}
                  color="purple"
                >
                  {share.shared_to_username}
                </Tag>
              ))}
            </div>
          </div>
        )}
      </Modal>

      <Modal
        title="Rename"
        open={renameModalVisible}
        onCancel={() => {
          setRenameModalVisible(false);
          setExecutionToRename(null);
          setRenameStem('');
          setRenameExtension('');
        }}
        onOk={handleConfirmRename}
        okButtonProps={{ disabled: !renameStem.trim() }}
        confirmLoading={renameLoading}
        okText="Save"
        cancelText="cancel"
        centered
        destroyOnClose
        styles={getModalStyles(themeMode)}
      >
        <Input
          value={renameStem}
          onChange={e => setRenameStem(e.target.value)}
          addonAfter={renameExtension || undefined}
          placeholder="File name"
        />
      </Modal>

      <Modal
        title="Delete"
        open={deleteExecutionVisible}
        onOk={confirmDeleteExecution}
        onCancel={() => {
          setDeleteExecutionVisible(false);
          setExecutionToDelete(null);
        }}
        okText="Delete"
        okType="danger"
        confirmLoading={deletingExecutions}
        centered
        styles={getModalStyles(themeMode)}
      >
        Delete this source permanently?
      </Modal>
    </div>
  );
};

const MobileSourcesPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = THEME_COLORS[themeMode];

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: themeColors.cardBg,
            colorBgElevated: themeColors.cardBg,
            colorBorderSecondary: themeColors.cardBorder,
            colorPrimary: '#2196F3',
            colorText: themeColors.text,
            colorTextSecondary: themeColors.textSecondary,
            colorBgLayout: themeColors.pageBg,
            borderRadius: 8,
            borderRadiusLG: 12,
          },
        }),
      }}
    >
      <App>
        <MobileSourcesInner />
      </App>
    </ConfigProvider>
  );
};

export default MobileSourcesPage;

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { App, Button, Segmented, Typography } from 'antd';
import { ArrowLeftOutlined } from '@ant-design/icons';
import {
  ExecutionDetailsPanel,
  OriginalContentView,
  AIAnalysisView,
} from '../pages/FilesPage';
import TradeReviewView from './TradeReviewView';
import { isIBReportContent, type IBReportData } from './IBTradingReportView';
import { filesAPI } from '../services/files';
import { tasksAPI, TaskExecution, TaskExecutionDetail } from '../services/tasks';
import { getErrorMessage } from '../utils/errorHandler';
import { buildAiAnalysisPdfFilename, downloadElementAsPDF } from '../utils/pdfDownload';
import promptTemplateService, { PromptTemplate } from '../services/promptTemplateService';
import { useFileProcessing } from '../contexts/FileProcessingContext';

const { Text } = Typography;

export type MobileSourceFileDetailThemeColors = {
  pageBg: string;
  cardBg: string;
  cardBorder: string;
  text: string;
  textSecondary: string;
  textMuted: string;
  codeBlockBg: string;
  selectedBorder: string;
  background: string;
  inputBg?: string;
};

type DetailTab = 'info' | 'original' | 'analysis' | 'review';

export type MobileSourceFileDetailProps = {
  execution: TaskExecution;
  themeColors: MobileSourceFileDetailThemeColors;
  isDark: boolean;
  onClose: () => void;
  onRefreshList: () => Promise<void>;
};

const MobileSourceFileDetail: React.FC<MobileSourceFileDetailProps> = ({
  execution,
  themeColors,
  isDark,
  onClose,
  onRefreshList,
}) => {
  const { message } = App.useApp();
  const { addTask, updateTaskStatus: updateGlobalTaskStatus } = useFileProcessing();

  const [detailTab, setDetailTab] = useState<DetailTab>('original');
  const [selectedFileForView, setSelectedFileForView] = useState<unknown>(null);
  const [aiAnalysisData, setAiAnalysisData] = useState<TaskExecutionDetail | null>(null);
  const [aiAnalysisLoading, setAiAnalysisLoading] = useState(false);
  const [pdfDownloading, setPdfDownloading] = useState(false);
  const [audioAnalyzingIds, setAudioAnalyzingIds] = useState<Set<string>>(new Set());
  const [docAnalyzingIds, setDocAnalyzingIds] = useState<Set<string>>(new Set());
  const [allTemplates, setAllTemplates] = useState<PromptTemplate[]>([]);
  /** 活动报告（IB）：与桌面端一致，用于 Trade Review Tab，并隐藏 AI Analysis */
  const [ibReportDataForReview, setIbReportDataForReview] = useState<IBReportData | null>(null);

  useEffect(() => {
    setDetailTab('original');
    setSelectedFileForView(null);
    setAiAnalysisData(null);
    setIbReportDataForReview(null);
  }, [execution.id]);

  const handleIBReportDetected = useCallback((data: IBReportData | null) => {
    setIbReportDataForReview(data);
    // 与桌面一致：不自动跳到 Trade Review。Original 应展示活动报表的 Overview / MTM / Trades（IBTradingReportView）。
    if (!data) {
      setDetailTab(t => (t === 'review' ? 'original' : t));
    }
  }, []);

  /**
   * 预取 execution details，用于尽早识别活动报告、显示「Trade Review」分段入口；不切换当前 Tab，避免挡掉 Original 内的报表 Tabs。
   */
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const details = await tasksAPI.getTaskExecutionDetails(execution.id);
        const file = details.files?.[0];
        if (!file?.content) {
          if (!cancelled) handleIBReportDetected(null);
          return;
        }
        const parsed = isIBReportContent(file.content);
        if (cancelled) return;
        handleIBReportDetected(parsed);
      } catch {
        if (!cancelled) handleIBReportDetected(null);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [execution.id, handleIBReportDetected]);

  useEffect(() => {
    promptTemplateService
      .listTemplates({ active_only: true })
      .then(setAllTemplates)
      .catch(() => {});
  }, []);

  const getDisplayAgentName = useCallback(
    (taskName: string): string => {
      if (!taskName) return '';
      let templateId: number | null = null;
      const idMatch = taskName.match(/^ID:(\d+)$/);
      if (idMatch) {
        templateId = parseInt(idMatch[1], 10);
      } else {
        const parsed = parseInt(taskName, 10);
        if (!isNaN(parsed) && parsed.toString() === taskName) {
          templateId = parsed;
        }
      }
      if (templateId !== null && allTemplates.length > 0) {
        const template = allTemplates.find(t => t.id === templateId);
        if (template) return template.template_name;
      }
      return taskName;
    },
    [allTemplates]
  );

  const onExecutionRefresh = useCallback(() => {
    void onRefreshList();
  }, [onRefreshList]);

  const segmentedOptions = useMemo(
    () =>
      ibReportDataForReview
        ? [
            { label: 'Original', value: 'original' as DetailTab },
            { label: 'Trade Review', value: 'review' as DetailTab },
            { label: 'Info', value: 'info' as DetailTab },
          ]
        : [
            { label: 'Original', value: 'original' as DetailTab },
            { label: 'AI Analysis', value: 'analysis' as DetailTab },
            { label: 'Info', value: 'info' as DetailTab },
          ],
    [ibReportDataForReview]
  );

  const handleDownloadPdf = async () => {
    if (!aiAnalysisData?.execution?.final_report) {
      message.error('No report content available for download');
      return;
    }
    try {
      setPdfDownloading(true);
      await new Promise(resolve => setTimeout(resolve, 500));
      const documentName =
        execution.first_display_name ||
        execution.first_filename ||
        aiAnalysisData.files?.[0]?.filename ||
        'Document';
      const filename = buildAiAnalysisPdfFilename({
        documentName,
        analyzedAt: aiAnalysisData.execution.created_at,
        modelName: execution.llm_model,
      });
      await downloadElementAsPDF('ai-analysis-content', {
        filename,
        title: documentName,
        author: process.env.REACT_APP_APP_NAME || 'Broadview Hub',
        subject: 'AI Analysis Report',
        scale: 1.2,
        imageQuality: 0.80,
      });
      message.success('PDF report downloaded successfully');
    } catch {
      message.error('PDF download failed, please try again');
    } finally {
      setPdfDownloading(false);
    }
  };

  const handleAnalyzeAudio = async (
    record: TaskExecution,
    templateVariables?: Record<string, unknown>,
    templateId?: number,
  ) => {
    const executionId = Number(record.id);
    const fileName = record.first_display_name || record.first_filename || record.task_name || 'Audio file';
    try {
      setAudioAnalyzingIds(prev => new Set(prev).add(record.id));
      message.loading({ content: 'Submitting audio analysis task...', key: 'audioAnalyze', duration: 0 });
      const filteredVars = templateVariables
        ? Object.fromEntries(Object.entries(templateVariables).filter(([, v]) => v !== undefined && v !== ''))
        : undefined;
      const result = await filesAPI.analyzeAudio({
        execution_id: executionId,
        ...(templateId != null ? { template_id: templateId } : {}),
        ...(filteredVars && Object.keys(filteredVars).length > 0 ? { template_variables: filteredVars } : {}),
      });
      if (result.success && result.task_id) {
        message.loading({ content: 'AI analyzing audio...', key: 'audioAnalyze', duration: 0 });
        addTask(result.task_id, fileName, 'audio');
        const pollInterval = setInterval(async () => {
          try {
            const statusResult = await filesAPI.getAudioAnalysisStatus(result.task_id);
            updateGlobalTaskStatus(result.task_id, {
              status: statusResult.status,
              progress: statusResult.progress || 0,
              current_file: fileName,
              files_processed: 0,
              files_failed: 0,
              total_files: 1,
              logs: [],
            });
            if (statusResult.status === 'completed') {
              clearInterval(pollInterval);
              message.destroy('audioAnalyze');
              message.success('Audio AI analysis completed!');
              setAudioAnalyzingIds(prev => {
                const next = new Set(prev);
                next.delete(record.id);
                return next;
              });
              await onRefreshList();
              try {
                const details = await tasksAPI.getTaskExecutionDetails(record.id);
                setAiAnalysisData(details);
              } catch {
                /* ignore */
              }
            } else if (statusResult.status === 'failed') {
              clearInterval(pollInterval);
              message.destroy('audioAnalyze');
              message.error(`Audio analysis failed: ${statusResult.message}`);
              setAudioAnalyzingIds(prev => {
                const next = new Set(prev);
                next.delete(record.id);
                return next;
              });
              await onRefreshList();
            }
          } catch (err) {
            console.error('Failed to poll audio analysis status:', err);
          }
        }, 8000);
        setTimeout(() => {
          clearInterval(pollInterval);
          setAudioAnalyzingIds(prev => {
            const next = new Set(prev);
            next.delete(record.id);
            return next;
          });
        }, 300000);
      }
    } catch (error: unknown) {
      message.destroy('audioAnalyze');
      message.error(getErrorMessage(error, 'Failed to start audio analysis'));
      setAudioAnalyzingIds(prev => {
        const next = new Set(prev);
        next.delete(record.id);
        return next;
      });
    }
  };

  const handleAnalyzeDocument = async (
    record: TaskExecution,
    templateId: number,
    templateVariables?: Record<string, unknown>
  ) => {
    const executionId = Number(record.id);
    const fileName = record.first_display_name || record.first_filename || record.task_name || 'Document';
    try {
      setDocAnalyzingIds(prev => new Set(prev).add(record.id));
      message.loading({ content: 'Submitting document analysis task...', key: 'docAnalyze', duration: 0 });
      const result = await filesAPI.analyzeDocument({
        execution_id: executionId,
        template_id: templateId,
        template_variables: templateVariables ?? {},
      });
      if (result.success && result.task_id) {
        message.destroy('docAnalyze');
        addTask(result.task_id, fileName, 'document');
        const pollInterval = setInterval(async () => {
          try {
            const statusResult = await filesAPI.getDocumentAnalysisStatus(result.task_id);
            updateGlobalTaskStatus(result.task_id, {
              status: statusResult.status,
              progress: statusResult.progress || 0,
              current_file: fileName,
              files_processed: 0,
              files_failed: 0,
              total_files: 1,
              logs: [],
            });
            if (statusResult.status === 'completed') {
              clearInterval(pollInterval);
              message.destroy('docAnalyze');
              message.success('Document AI analysis completed!');
              setDocAnalyzingIds(prev => {
                const next = new Set(prev);
                next.delete(record.id);
                return next;
              });
              await onRefreshList();
              try {
                const details = await tasksAPI.getTaskExecutionDetails(record.id);
                setAiAnalysisData(details);
              } catch {
                /* ignore */
              }
            } else if (statusResult.status === 'failed') {
              clearInterval(pollInterval);
              message.destroy('docAnalyze');
              message.error(`Document analysis failed: ${statusResult.message}`);
              setDocAnalyzingIds(prev => {
                const next = new Set(prev);
                next.delete(record.id);
                return next;
              });
              await onRefreshList();
            }
          } catch (err) {
            console.error('Failed to poll document analysis status:', err);
          }
        }, 8000);
        setTimeout(() => {
          clearInterval(pollInterval);
          setDocAnalyzingIds(prev => {
            const next = new Set(prev);
            next.delete(record.id);
            return next;
          });
        }, 600000);
      }
    } catch (error: unknown) {
      message.destroy('docAnalyze');
      message.error(getErrorMessage(error, 'Failed to start document analysis'));
      setDocAnalyzingIds(prev => {
        const next = new Set(prev);
        next.delete(record.id);
        return next;
      });
    }
  };

  const title = execution.first_display_name || execution.first_filename || 'Source';

  return (
    <div
      className="mobile-source-file-detail"
      style={{
        background: themeColors.pageBg,
        color: themeColors.text,
      }}
    >
      <header className="mobile-source-file-detail-header" style={{ borderBottom: `1px solid ${themeColors.cardBorder}` }}>
        <Button
          type="text"
          icon={<ArrowLeftOutlined />}
          onClick={onClose}
          aria-label="Back to list"
          className="mobile-source-file-detail-back"
        />
        <Text ellipsis className="mobile-source-file-detail-title" style={{ color: themeColors.text }}>
          {title}
        </Text>
        <span className="mobile-source-file-detail-header-spacer" />
      </header>

      <div className="mobile-source-file-detail-tabs" style={{ borderBottom: `1px solid ${themeColors.cardBorder}` }}>
        <Segmented
          block
          size="small"
          value={detailTab}
          onChange={v => setDetailTab(v as DetailTab)}
          options={segmentedOptions}
          className="mobile-source-file-detail-segmented"
        />
      </div>

      <div className="mobile-source-file-detail-body">
        {detailTab === 'info' && (
          <div className="mobile-source-file-detail-pane mobile-source-file-detail-pane--info">
            <ExecutionDetailsPanel
              execution={execution}
              themeColors={themeColors}
              isDark={isDark}
              getDisplayAgentName={getDisplayAgentName}
              onExecutionRefresh={onExecutionRefresh}
            />
          </div>
        )}
        {detailTab === 'original' && (
          <div className="mobile-source-file-detail-pane mobile-source-file-detail-pane--original">
            <OriginalContentView
              execution={execution}
              themeColors={themeColors}
              isDark={isDark}
              onFileSelect={setSelectedFileForView}
              selectedFile={selectedFileForView}
              onTranscriptPersisted={async () => {
                await onRefreshList();
              }}
              onTextNoteContentSaved={async () => {
                await onRefreshList();
              }}
              onIBReportDetected={handleIBReportDetected}
            />
          </div>
        )}
        {detailTab === 'review' && ibReportDataForReview && (
          <div className="mobile-source-file-detail-pane mobile-source-file-detail-pane--review">
            <TradeReviewView data={ibReportDataForReview} isDark={isDark} themeColors={themeColors} />
          </div>
        )}
        {detailTab === 'analysis' && !ibReportDataForReview && (
          <div className="mobile-source-file-detail-pane mobile-source-file-detail-pane--analysis">
            <AIAnalysisView
              execution={execution}
              themeColors={themeColors}
              isDark={isDark}
              aiAnalysisData={aiAnalysisData}
              aiAnalysisLoading={aiAnalysisLoading}
              onDownloadPdf={handleDownloadPdf}
              pdfDownloading={pdfDownloading}
              onLoadAnalysis={async () => {
                try {
                  setAiAnalysisLoading(true);
                  const details = await tasksAPI.getTaskExecutionDetails(execution.id);
                  setAiAnalysisData(details);
                } catch {
                  message.error('Failed to load AI analysis report');
                } finally {
                  setAiAnalysisLoading(false);
                }
              }}
              onAnalyzeAudio={handleAnalyzeAudio}
              onAnalyzeDocument={handleAnalyzeDocument}
              onAnalysisSaved={newContent => {
                setAiAnalysisData(prev =>
                  prev
                    ? {
                        ...prev,
                        execution: { ...prev.execution, final_report: newContent },
                      }
                    : null
                );
              }}
              audioAnalyzing={audioAnalyzingIds.has(execution.id)}
              docAnalyzing={docAnalyzingIds.has(execution.id)}
              mobileCompactActions
            />
          </div>
        )}
      </div>
    </div>
  );
};

export default MobileSourceFileDetail;

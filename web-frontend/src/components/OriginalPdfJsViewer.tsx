import React, { useEffect, useRef, useState } from 'react';
import * as pdfjsLib from 'pdfjs-dist';

let workerConfigured = false;

function ensurePdfWorker(): void {
  if (typeof window === 'undefined' || workerConfigured) return;
  const base = `${window.location.origin}${process.env.PUBLIC_URL || ''}`.replace(/\/$/, '');
  pdfjsLib.GlobalWorkerOptions.workerSrc = `${base}/pdf.worker.min.js`;
  workerConfigured = true;
}

export type OriginalPdfJsViewerProps = {
  fileUrl: string;
  /** 外层与 PDF.js 容器（通常需 flex:1 + min-height） */
  className?: string;
  style?: React.CSSProperties;
  loadingText?: string;
  textMuted?: string;
  text?: string;
};

/**
 * 使用 PDF.js 在 Canvas 上渲染整份 PDF。
 * 解决多数 Android WebView / 自带浏览器无法在 iframe/embed 内嵌预览 PDF、仅 Chrome 出现「Open」外链的问题。
 */
export const OriginalPdfJsViewer: React.FC<OriginalPdfJsViewerProps> = ({
  fileUrl,
  className,
  style,
  loadingText = 'Loading PDF…',
  textMuted = 'rgba(0, 0, 0, 0.45)',
  text = 'rgba(0, 0, 0, 0.88)',
}) => {
  const hostRef = useRef<HTMLDivElement>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    ensurePdfWorker();
    let cancelled = false;

    const run = async () => {
      setLoading(true);
      setError(null);
      try {
        await new Promise<void>((r) => requestAnimationFrame(() => requestAnimationFrame(() => r())));
        const host = hostRef.current;
        if (!host || cancelled) return;

        const loadingTask = pdfjsLib.getDocument({
          url: fileUrl,
          withCredentials: false,
          // 后端 serve 多为整包流式返回，未必支持 Range；关闭分片避免部分 WebView 拉取失败
          disableRange: true,
          disableStream: true,
        });
        const pdf = await loadingTask.promise;
        if (cancelled) return;

        host.innerHTML = '';

        const measureWidth = () => {
          const w = host.clientWidth || host.parentElement?.clientWidth;
          return Math.max(280, (w || 360) - 8);
        };

        const dpr = Math.min(window.devicePixelRatio || 1, 2);

        for (let p = 1; p <= pdf.numPages; p++) {
          if (cancelled) return;
          const page = await pdf.getPage(p);
          const containerWidth = measureWidth();
          const baseViewport = page.getViewport({ scale: 1 });
          const scale = Math.min(containerWidth / baseViewport.width, 2.5);
          const viewport = page.getViewport({ scale: scale * dpr });

          const canvas = document.createElement('canvas');
          const ctx = canvas.getContext('2d');
          if (!ctx) {
            throw new Error('Canvas not available');
          }
          canvas.width = viewport.width;
          canvas.height = viewport.height;
          canvas.style.width = `${viewport.width / dpr}px`;
          canvas.style.height = `${viewport.height / dpr}px`;
          canvas.style.display = 'block';
          canvas.style.margin = '0 auto 12px';
          canvas.style.maxWidth = '100%';
          host.appendChild(canvas);

          const renderTask = page.render({
            canvasContext: ctx,
            viewport,
          });
          await renderTask.promise;
          await new Promise<void>((r) => setTimeout(r, 0));
        }
      } catch (e) {
        if (!cancelled) {
          console.error('[OriginalPdfJsViewer]', e);
          setError(e instanceof Error ? e.message : 'Failed to render PDF');
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    void run();
    return () => {
      cancelled = true;
    };
  }, [fileUrl]);

  return (
    <div
      className={className}
      style={{
        position: 'relative',
        width: '100%',
        height: '100%',
        minHeight: 0,
        overflow: 'auto',
        WebkitOverflowScrolling: 'touch',
        ...style,
      }}
    >
      {loading && (
        <div
          style={{
            position: 'absolute',
            inset: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: textMuted,
            fontSize: 13,
            pointerEvents: 'none',
          }}
        >
          {loadingText}
        </div>
      )}
      {error && (
        <div
          style={{
            padding: 24,
            textAlign: 'center',
            color: text,
            fontSize: 13,
          }}
        >
          {error}
        </div>
      )}
      <div ref={hostRef} style={{ width: '100%', minHeight: loading ? 120 : 0 }} />
    </div>
  );
};

export default OriginalPdfJsViewer;

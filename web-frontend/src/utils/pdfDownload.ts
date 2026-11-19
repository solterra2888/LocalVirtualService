import html2canvas from 'html2canvas';
import jsPDF from 'jspdf';

interface PDFDownloadOptions {
  filename?: string;
  title?: string;
  author?: string;
  subject?: string;
  /** JPEG quality 0–1, defaults to 0.82 */
  imageQuality?: number;
  /**
   * html2canvas render scale relative to CSS pixels.
   * Lower = smaller file. 1.5 gives good readability; defaults to 1.5.
   * On high-DPI (retina) screens the browser default would be 2–3×,
   * which is the primary cause of 100 MB+ PDFs.
   */
  scale?: number;
}

/** Strip characters that are invalid in file names on common OSes. */
const sanitizeFilenamePart = (name: string): string =>
  name.replace(/[/\\:*?"<>|]/g, '_').replace(/\s+/g, ' ').trim();

/**
 * Build export filename: `{Document Name} {YYYY-MM-DD} (Analyzed by {model}).pdf`
 */
export const buildAiAnalysisPdfFilename = (options: {
  documentName?: string | null;
  analyzedAt?: string | Date | null;
  modelName?: string | null;
}): string => {
  const doc = sanitizeFilenamePart(options.documentName?.trim() || '') || 'Document';
  const dateSource = options.analyzedAt ? new Date(options.analyzedAt) : new Date();
  const date = Number.isNaN(dateSource.getTime())
    ? new Date().toISOString().split('T')[0]
    : dateSource.toISOString().split('T')[0];
  const model = options.modelName?.trim();
  const analyzedBy = model ? ` (Analyzed by ${model})` : '';
  return `${doc} ${date}${analyzedBy}.pdf`;
};

/**
 * 将HTML元素转换为PDF并下载
 * @param elementId HTML元素的ID
 * @param options PDF选项
 */
export const downloadElementAsPDF = async (
  elementId: string,
  options: PDFDownloadOptions = {}
): Promise<void> => {
  const {
    filename = 'ai-analysis-report.pdf',
    title = 'AI Analysis Report',
    author = process.env.REACT_APP_APP_NAME || 'GuanHe Terminal',
    subject = 'AI Analysis Report',
    imageQuality = 0.82,
    scale = 1.5,
  } = options;

  const RENDER_WIDTH = 800; // CSS px — fixed width for consistent layout
  const A4_MM_WIDTH = 210;
  const A4_MM_HEIGHT = 297;

  try {
    const element = document.getElementById(elementId);
    if (!element) {
      throw new Error(`Element with id "${elementId}" not found`);
    }

    // Off-screen clone so we don't mutate the live DOM
    const tempContainer = document.createElement('div');
    tempContainer.style.cssText = [
      'position:absolute',
      'left:-9999px',
      'top:0',
      `width:${RENDER_WIDTH}px`,
      'background-color:#ffffff',
      'padding:20px',
      'font-family:Arial,sans-serif',
      'font-size:14px',
      'line-height:1.6',
    ].join(';');

    const clonedElement = element.cloneNode(true) as HTMLElement;
    clonedElement.style.cssText = 'width:100%;height:auto;background-color:#ffffff;color:#000000;';

    // Drop any scoped <style> tags that carry dark-theme overrides
    clonedElement.querySelectorAll('style').forEach(s => s.remove());

    // Minimal PDF-friendly stylesheet — keep only what's needed
    const styleEl = document.createElement('style');
    styleEl.textContent = `
      * { box-sizing: border-box; }
      h1,h2,h3,h4,h5,h6 { color:#000 !important; margin:20px 0 10px; page-break-after:avoid; }
      h1 { font-size:24px !important; }
      h2 { font-size:20px !important; }
      h3 { font-size:18px !important; }
      h4 { font-size:16px !important; }
      p  { color:#000 !important; margin-bottom:10px !important; }
      ul,ol { color:#000 !important; margin-bottom:10px !important; padding-left:20px !important; }
      li { color:#000 !important; margin-bottom:5px !important; }
      table { width:100% !important; border-collapse:collapse !important; margin:10px 0 !important; }
      th,td { border:1px solid #000 !important; padding:8px !important; color:#000 !important; }
      th { background-color:#f0f0f0 !important; font-weight:bold !important; }
      blockquote { border-left:4px solid #000 !important; padding-left:16px !important; margin:16px 0 !important; background:#f9f9f9 !important; color:#000 !important; }
      code { background:#f0f0f0 !important; padding:2px 4px !important; border-radius:3px !important; font-family:'Courier New',monospace !important; color:#000 !important; }
      pre  { background:#f0f0f0 !important; padding:10px !important; border-radius:5px !important; margin:10px 0 !important; }
      pre code { background:transparent !important; padding:0 !important; }
      a    { color:#00f !important; text-decoration:underline !important; }
      strong { font-weight:bold !important; color:#000 !important; }
      em { font-style:italic !important; color:#000 !important; }
      hr { border:none !important; border-top:1px solid #000 !important; margin:20px 0 !important; }
    `;

    tempContainer.appendChild(styleEl);
    tempContainer.appendChild(clonedElement);
    document.body.appendChild(tempContainer);

    // Allow the browser to paint the cloned DOM
    await new Promise(resolve => setTimeout(resolve, 800));

    // Capture at controlled scale — the key lever for file size.
    // Default window.devicePixelRatio (2–3 on retina) was the primary
    // cause of 100 MB+ PDFs; 1.5 keeps text crisp at ~1/4 the area.
    const canvas = await html2canvas(tempContainer, {
      useCORS: true,
      allowTaint: true,
      background: '#ffffff',
      // html2canvas@1.4.1 types are incomplete; these options exist at runtime
      ...({ scale, windowWidth: RENDER_WIDTH, logging: false } as object),
    } as Parameters<typeof html2canvas>[1]);

    document.body.removeChild(tempContainer);

    // Convert to JPEG — dramatically smaller than PNG for text-on-white
    const imgData = canvas.toDataURL('image/jpeg', imageQuality);

    const A4_PX_HEIGHT_AT_SCALE = (A4_MM_HEIGHT / A4_MM_WIDTH) * canvas.width;
    const totalHeightMm = (canvas.height * A4_MM_WIDTH) / canvas.width;

    const pdf = new jsPDF('p', 'mm', 'a4');
    pdf.setProperties({
      title,
      author,
      subject,
      creator: `${process.env.REACT_APP_APP_NAME || 'GuanHe Terminal'} AI Analysis Tool`,
    });

    // Slice the single canvas into A4 pages to avoid embedding one
    // giant image repeated N times (which inflates the file).
    const totalPages = Math.ceil(canvas.height / A4_PX_HEIGHT_AT_SCALE);
    for (let page = 0; page < totalPages; page++) {
      if (page > 0) pdf.addPage();

      // Y offset in mm so that each page shows the correct band
      const yOffsetMm = -(page * A4_MM_HEIGHT);
      pdf.addImage(imgData, 'JPEG', 0, yOffsetMm, A4_MM_WIDTH, totalHeightMm);
    }

    pdf.save(filename);

  } catch (error) {
    console.error('Error generating PDF:', error);
    throw new Error('Failed to generate PDF. Please try again.');
  }
};

/**
 * 将markdown内容转换为PDF并下载
 * @param markdownContent markdown内容
 * @param options PDF选项
 */
export const downloadMarkdownAsPDF = async (
  markdownContent: string,
  options: PDFDownloadOptions = {}
): Promise<void> => {
  const {
    filename = 'ai-analysis-report.pdf',
    title = 'AI Analysis Report',
    author = process.env.REACT_APP_APP_NAME || 'GuanHe Terminal',
    subject = 'AI Analysis Report'
  } = options;

  // 使用这些变量以避免ESLint警告
  console.log('PDF options:', { filename, title, author, subject });

  try {
    // 创建临时容器
    const tempContainer = document.createElement('div');
    tempContainer.id = 'temp-markdown-pdf';
    tempContainer.style.position = 'absolute';
    tempContainer.style.left = '-9999px';
    tempContainer.style.top = '0';
    tempContainer.style.width = '800px';
    tempContainer.style.backgroundColor = '#ffffff';
    tempContainer.style.padding = '20px';
    tempContainer.style.fontFamily = 'Arial, sans-serif';
    tempContainer.style.fontSize = '14px';
    tempContainer.style.lineHeight = '1.6';
    tempContainer.style.color = '#000000';
    
    // 简单的markdown到HTML转换（基础版本）
    let htmlContent = markdownContent
      .replace(/^# (.*$)/gim, '<h1>$1</h1>')
      .replace(/^## (.*$)/gim, '<h2>$1</h2>')
      .replace(/^### (.*$)/gim, '<h3>$1</h3>')
      .replace(/^#### (.*$)/gim, '<h4>$1</h4>')
      .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
      .replace(/\*(.*?)\*/g, '<em>$1</em>')
      .replace(/`(.*?)`/g, '<code>$1</code>')
      .replace(/^\* (.*$)/gim, '<li>$1</li>')
      .replace(/^\d+\. (.*$)/gim, '<li>$1</li>')
      .replace(/\n\n/g, '</p><p>')
      .replace(/\n/g, '<br>');
    
    // 包装在段落中
    htmlContent = `<p>${htmlContent}</p>`;
    
    // 处理列表
    htmlContent = htmlContent.replace(/(<li>.*<\/li>)/gs, '<ul>$1</ul>');
    
    tempContainer.innerHTML = htmlContent;
    document.body.appendChild(tempContainer);

    // 等待内容渲染
    await new Promise(resolve => setTimeout(resolve, 500));

    // 使用downloadElementAsPDF函数
    await downloadElementAsPDF('temp-markdown-pdf', options);

    // 清理
    document.body.removeChild(tempContainer);
    
  } catch (error) {
    console.error('Error generating PDF from markdown:', error);
    throw new Error('Failed to generate PDF from markdown. Please try again.');
  }
};

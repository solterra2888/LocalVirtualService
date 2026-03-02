import html2canvas from 'html2canvas';
import jsPDF from 'jspdf';

interface PDFDownloadOptions {
  filename?: string;
  title?: string;
  author?: string;
  subject?: string;
}

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
    author = process.env.REACT_APP_APP_NAME || 'Reverse Capital',
    subject = 'AI Analysis Report'
  } = options;

  try {
    const element = document.getElementById(elementId);
    if (!element) {
      throw new Error(`Element with id "${elementId}" not found`);
    }

    // 创建临时容器来确保完整渲染
    const tempContainer = document.createElement('div');
    tempContainer.style.position = 'absolute';
    tempContainer.style.left = '-9999px';
    tempContainer.style.top = '0';
    tempContainer.style.width = '800px'; // 固定宽度确保一致性
    tempContainer.style.backgroundColor = '#ffffff';
    tempContainer.style.padding = '20px';
    tempContainer.style.fontFamily = 'Arial, sans-serif';
    tempContainer.style.fontSize = '14px';
    tempContainer.style.lineHeight = '1.6';
    
    // 克隆元素内容
    const clonedElement = element.cloneNode(true) as HTMLElement;
    clonedElement.style.width = '100%';
    clonedElement.style.height = 'auto';
    clonedElement.style.backgroundColor = '#ffffff';
    clonedElement.style.color = '#000000';
    
    // 移除可能影响PDF生成的样式
    const styleElements = clonedElement.querySelectorAll('style');
    styleElements.forEach(style => style.remove());
    
    // 添加PDF友好的样式
    const style = document.createElement('style');
    style.textContent = `
      * {
        box-sizing: border-box;
      }
      h1, h2, h3, h4, h5, h6 {
        color: #000000 !important;
        margin-top: 20px !important;
        margin-bottom: 10px !important;
        page-break-after: avoid;
      }
      h1 { font-size: 24px !important; }
      h2 { font-size: 20px !important; }
      h3 { font-size: 18px !important; }
      h4 { font-size: 16px !important; }
      p {
        color: #000000 !important;
        margin-bottom: 10px !important;
        page-break-inside: avoid;
      }
      ul, ol {
        color: #000000 !important;
        margin-bottom: 10px !important;
        padding-left: 20px !important;
      }
      li {
        color: #000000 !important;
        margin-bottom: 5px !important;
      }
      table {
        width: 100% !important;
        border-collapse: collapse !important;
        margin: 10px 0 !important;
        page-break-inside: avoid;
      }
      th, td {
        border: 1px solid #000000 !important;
        padding: 8px !important;
        text-align: left !important;
        color: #000000 !important;
      }
      th {
        background-color: #f0f0f0 !important;
        font-weight: bold !important;
      }
      blockquote {
        border-left: 4px solid #000000 !important;
        padding-left: 16px !important;
        margin: 16px 0 !important;
        background-color: #f9f9f9 !important;
        color: #000000 !important;
      }
      code {
        background-color: #f0f0f0 !important;
        padding: 2px 4px !important;
        border-radius: 3px !important;
        font-family: 'Courier New', monospace !important;
        color: #000000 !important;
      }
      pre {
        background-color: #f0f0f0 !important;
        padding: 10px !important;
        border-radius: 5px !important;
        overflow-x: auto !important;
        margin: 10px 0 !important;
        page-break-inside: avoid;
      }
      pre code {
        background-color: transparent !important;
        padding: 0 !important;
      }
      a {
        color: #0000ff !important;
        text-decoration: underline !important;
      }
      strong {
        font-weight: bold !important;
        color: #000000 !important;
      }
      em {
        font-style: italic !important;
        color: #000000 !important;
      }
      hr {
        border: none !important;
        border-top: 1px solid #000000 !important;
        margin: 20px 0 !important;
      }
    `;
    
    tempContainer.appendChild(style);
    tempContainer.appendChild(clonedElement);
    document.body.appendChild(tempContainer);

    // 等待内容渲染完成
    await new Promise(resolve => setTimeout(resolve, 1000));

    // 使用html2canvas生成canvas
    const canvas = await html2canvas(tempContainer, {
      useCORS: true,
      allowTaint: true,
      background: '#ffffff'
    });

    // 清理临时元素
    document.body.removeChild(tempContainer);

    // 计算PDF尺寸
    const imgWidth = 210; // A4宽度 (mm)
    const pageHeight = 297; // A4高度 (mm)
    const imgHeight = (canvas.height * imgWidth) / canvas.width;
    
    // 创建PDF
    const pdf = new jsPDF('p', 'mm', 'a4');
    
    // 设置PDF元数据
    pdf.setProperties({
      title,
      author,
      subject,
      creator: `${process.env.REACT_APP_APP_NAME || 'Reverse Capital'} AI Analysis Tool`
    });

    let heightLeft = imgHeight;
    let position = 0;

    // 添加第一页
    pdf.addImage(canvas, 'PNG', 0, position, imgWidth, imgHeight);
    heightLeft -= pageHeight;

    // 如果内容超过一页，添加更多页面
    while (heightLeft >= 0) {
      position = heightLeft - imgHeight;
      pdf.addPage();
      pdf.addImage(canvas, 'PNG', 0, position, imgWidth, imgHeight);
      heightLeft -= pageHeight;
    }

    // 下载PDF
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
    author = process.env.REACT_APP_APP_NAME || 'Reverse Capital',
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

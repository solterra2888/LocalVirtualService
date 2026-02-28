import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeHighlight from 'rehype-highlight';
import 'highlight.js/styles/github.css'; // 使用GitHub风格的代码高亮样式

interface MarkdownRendererProps {
  content: string;
  className?: string;
  style?: React.CSSProperties;
  /** 'dark' 时使用浅色文字与深色背景，适合深色主题弹窗 */
  variant?: 'light' | 'dark';
}

const LIGHT = {
  h1: '#1f2937', h2: '#374151', h3: '#4b5563', h4: '#6b7280',
  text: '#374151', border: '#e5e7eb', blockquoteBg: '#f8fafc', blockquote: '#475569',
  codeBg: '#f1f5f9', code: '#e11d48', preBg: '#f8fafc', preBorder: '#e2e8f0',
  tableBorder: '#e2e8f0', theadBg: '#f8fafc', tbodyBg: '#fff',
  th: '#374151', td: '#4b5563',
  link: '#3b82f6', strong: '#1f2937', em: '#6b7280', hr: '#e5e7eb',
};
const DARK = {
  h1: '#e0e6f1', h2: '#c9d1d9', h3: '#b1bac4', h4: '#8b949e',
  text: 'rgba(255,255,255,0.9)', border: 'rgba(255,255,255,0.2)', blockquoteBg: 'rgba(255,255,255,0.06)', blockquote: 'rgba(255,255,255,0.7)',
  codeBg: 'rgba(255,255,255,0.1)', code: '#79c0ff', preBg: 'rgba(255,255,255,0.06)', preBorder: 'rgba(255,255,255,0.15)',
  tableBorder: 'rgba(255,255,255,0.2)', theadBg: 'rgba(255,255,255,0.08)', tbodyBg: 'transparent',
  th: '#e0e6f1', td: 'rgba(255,255,255,0.85)',
  link: '#58a6ff', strong: '#e0e6f1', em: 'rgba(255,255,255,0.7)', hr: 'rgba(255,255,255,0.2)',
};

const MarkdownRenderer: React.FC<MarkdownRendererProps> = ({ 
  content, 
  className, 
  style,
  variant = 'light'
}) => {
  const c = variant === 'dark' ? DARK : LIGHT;
  // 预处理内容：为紧跟在有序列表项后的无序列表添加缩进
  // 这样可以确保有序列表的序号连续（1. 2. 3.）而不是重复（1. 1. 1.）
  const preprocessContent = (text: string): string => {
    const lines = text.split('\n');
    const processedLines: string[] = [];
    
    for (let i = 0; i < lines.length; i++) {
      const currentLine = lines[i];
      const prevLine = i > 0 ? lines[i - 1] : '';
      
      // 检查当前行是否是无序列表项（以 "- " 开头，但不是已经有缩进的）
      const isUnorderedListItem = /^- /.test(currentLine) && !/^\s+- /.test(currentLine);
      
      // 检查上一行是否是有序列表项或者是空行（但再上一行是有序列表项）
      const prevIsOrderedList = /^\d+\.\s/.test(prevLine);
      const prevIsEmpty = prevLine.trim() === '';
      const beforePrevLine = i > 1 ? lines[i - 2] : '';
      const beforePrevIsOrderedList = /^\d+\.\s/.test(beforePrevLine);
      
      // 如果当前行是无序列表且紧跟在有序列表项后（可能中间有空行），添加缩进
      if (isUnorderedListItem && (prevIsOrderedList || (prevIsEmpty && beforePrevIsOrderedList))) {
        processedLines.push('   ' + currentLine); // 添加3个空格缩进
      } else {
        processedLines.push(currentLine);
      }
    }
    // 用 "  \n" 连接，使单个换行在 Markdown 中渲染为换行（GFM 硬换行）
    return processedLines.join('  \n');
  };
  
  const processedContent = preprocessContent(content);
  
  return (
    <div className={[className, variant === 'dark' ? 'markdown-renderer-dark' : ''].filter(Boolean).join(' ')} style={style}>
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        rehypePlugins={[rehypeHighlight]}
        components={{
          // 自定义组件样式
          h1: ({ children }) => (
            <h1 style={{ 
              fontSize: '24px', 
              fontWeight: 'bold', 
              marginBottom: '16px',
              marginTop: '24px',
              color: c.h1,
              borderBottom: `2px solid ${c.border}`,
              paddingBottom: '8px'
            }}>
              {children}
            </h1>
          ),
          h2: ({ children }) => (
            <h2 style={{ 
              fontSize: '20px', 
              fontWeight: 'bold', 
              marginBottom: '12px',
              marginTop: '20px',
              color: c.h2
            }}>
              {children}
            </h2>
          ),
          h3: ({ children }) => (
            <h3 style={{ 
              fontSize: '18px', 
              fontWeight: '600', 
              marginBottom: '10px',
              marginTop: '16px',
              color: c.h3
            }}>
              {children}
            </h3>
          ),
          h4: ({ children }) => (
            <h4 style={{ 
              fontSize: '16px', 
              fontWeight: '600', 
              marginBottom: '8px',
              marginTop: '14px',
              color: c.h4
            }}>
              {children}
            </h4>
          ),
          p: ({ children }) => (
            <p style={{ 
              marginBottom: '12px', 
              lineHeight: '1.6',
              color: c.text
            }}>
              {children}
            </p>
          ),
          ul: ({ children }) => (
            <ul style={{ 
              marginBottom: '12px', 
              paddingLeft: '20px',
              color: c.text
            }}>
              {children}
            </ul>
          ),
          ol: ({ children }) => (
            <ol style={{ 
              marginBottom: '12px', 
              paddingLeft: '20px',
              color: c.text
            }}>
              {children}
            </ol>
          ),
          li: ({ children }) => (
            <li style={{ 
              marginBottom: '4px',
              lineHeight: '1.5'
            }}>
              {children}
            </li>
          ),
          blockquote: ({ children }) => (
            <blockquote style={{ 
              borderLeft: `4px solid ${c.link}`,
              paddingLeft: '16px',
              margin: '16px 0',
              backgroundColor: c.blockquoteBg,
              padding: '12px 16px',
              borderRadius: '0 4px 4px 0',
              fontStyle: 'italic',
              color: c.blockquote
            }}>
              {children}
            </blockquote>
          ),
          code: ({ children, className }) => {
            const isInline = !className;
            return isInline ? (
              <code style={{ 
                backgroundColor: c.codeBg,
                padding: '2px 6px',
                borderRadius: '4px',
                fontSize: '0.9em',
                fontFamily: 'Monaco, Consolas, "Courier New", monospace',
                color: c.code
              }}>
                {children}
              </code>
            ) : (
              <code className={className}>
                {children}
              </code>
            );
          },
          pre: ({ children }) => (
            <pre style={{ 
              backgroundColor: c.preBg,
              padding: '16px',
              borderRadius: '8px',
              overflow: 'auto',
              margin: '16px 0',
              border: `1px solid ${c.preBorder}`,
              fontSize: '14px',
              lineHeight: '1.5',
              color: c.text
            }}>
              {children}
            </pre>
          ),
          table: ({ children }) => (
            <div style={{ overflowX: 'auto', margin: '16px 0' }}>
              <table style={{ 
                width: '100%',
                borderCollapse: 'collapse',
                border: `1px solid ${c.tableBorder}`,
                borderRadius: '8px',
                overflow: 'hidden',
                backgroundColor: variant === 'dark' ? 'rgba(0,0,0,0.15)' : (c.tbodyBg ?? undefined)
              }}>
                {children}
              </table>
            </div>
          ),
          thead: ({ children }) => (
            <thead style={{ backgroundColor: c.theadBg }}>
              {children}
            </thead>
          ),
          th: ({ children }) => (
            <th style={{ 
              padding: '12px 16px',
              textAlign: 'left',
              fontWeight: '600',
              borderBottom: `1px solid ${c.tableBorder}`,
              color: c.th,
              backgroundColor: c.theadBg
            }}>
              {children}
            </th>
          ),
          td: ({ children }) => (
            <td style={{ 
              padding: '12px 16px',
              borderBottom: `1px solid ${c.tableBorder}`,
              color: c.td,
              backgroundColor: variant === 'dark' ? 'transparent' : undefined
            }}>
              {children}
            </td>
          ),
          a: ({ children, href }) => (
            <a 
              href={href} 
              target="_blank" 
              rel="noopener noreferrer"
              style={{ 
                color: c.link,
                textDecoration: 'none',
                borderBottom: '1px solid transparent',
                transition: 'border-bottom-color 0.2s'
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.borderBottomColor = c.link;
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.borderBottomColor = 'transparent';
              }}
            >
              {children}
            </a>
          ),
          strong: ({ children }) => (
            <strong style={{ fontWeight: '600', color: c.strong }}>
              {children}
            </strong>
          ),
          em: ({ children }) => (
            <em style={{ fontStyle: 'italic', color: c.em }}>
              {children}
            </em>
          ),
          hr: () => (
            <hr style={{ 
              border: 'none',
              height: '1px',
              backgroundColor: c.hr,
              margin: '24px 0'
            }} />
          )
        }}
      >
        {processedContent}
      </ReactMarkdown>
    </div>
  );
};

export default MarkdownRenderer;

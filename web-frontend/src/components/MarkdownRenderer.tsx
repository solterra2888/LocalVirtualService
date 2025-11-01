import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeHighlight from 'rehype-highlight';
import 'highlight.js/styles/github.css'; // 使用GitHub风格的代码高亮样式

interface MarkdownRendererProps {
  content: string;
  className?: string;
  style?: React.CSSProperties;
}

const MarkdownRenderer: React.FC<MarkdownRendererProps> = ({ 
  content, 
  className, 
  style 
}) => {
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
    
    return processedLines.join('\n');
  };
  
  const processedContent = preprocessContent(content);
  
  return (
    <div className={className} style={style}>
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
              color: '#1f2937',
              borderBottom: '2px solid #e5e7eb',
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
              color: '#374151'
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
              color: '#4b5563'
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
              color: '#6b7280'
            }}>
              {children}
            </h4>
          ),
          p: ({ children }) => (
            <p style={{ 
              marginBottom: '12px', 
              lineHeight: '1.6',
              color: '#374151'
            }}>
              {children}
            </p>
          ),
          ul: ({ children }) => (
            <ul style={{ 
              marginBottom: '12px', 
              paddingLeft: '20px',
              color: '#374151'
            }}>
              {children}
            </ul>
          ),
          ol: ({ children }) => (
            <ol style={{ 
              marginBottom: '12px', 
              paddingLeft: '20px',
              color: '#374151'
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
              borderLeft: '4px solid #3b82f6',
              paddingLeft: '16px',
              margin: '16px 0',
              backgroundColor: '#f8fafc',
              padding: '12px 16px',
              borderRadius: '0 4px 4px 0',
              fontStyle: 'italic',
              color: '#475569'
            }}>
              {children}
            </blockquote>
          ),
          code: ({ children, className }) => {
            const isInline = !className;
            return isInline ? (
              <code style={{ 
                backgroundColor: '#f1f5f9',
                padding: '2px 6px',
                borderRadius: '4px',
                fontSize: '0.9em',
                fontFamily: 'Monaco, Consolas, "Courier New", monospace',
                color: '#e11d48'
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
              backgroundColor: '#f8fafc',
              padding: '16px',
              borderRadius: '8px',
              overflow: 'auto',
              margin: '16px 0',
              border: '1px solid #e2e8f0',
              fontSize: '14px',
              lineHeight: '1.5'
            }}>
              {children}
            </pre>
          ),
          table: ({ children }) => (
            <div style={{ overflowX: 'auto', margin: '16px 0' }}>
              <table style={{ 
                width: '100%',
                borderCollapse: 'collapse',
                border: '1px solid #e2e8f0',
                borderRadius: '8px',
                overflow: 'hidden'
              }}>
                {children}
              </table>
            </div>
          ),
          thead: ({ children }) => (
            <thead style={{ backgroundColor: '#f8fafc' }}>
              {children}
            </thead>
          ),
          th: ({ children }) => (
            <th style={{ 
              padding: '12px 16px',
              textAlign: 'left',
              fontWeight: '600',
              borderBottom: '1px solid #e2e8f0',
              color: '#374151'
            }}>
              {children}
            </th>
          ),
          td: ({ children }) => (
            <td style={{ 
              padding: '12px 16px',
              borderBottom: '1px solid #f1f5f9',
              color: '#4b5563'
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
                color: '#3b82f6',
                textDecoration: 'none',
                borderBottom: '1px solid transparent',
                transition: 'border-bottom-color 0.2s'
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.borderBottomColor = '#3b82f6';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.borderBottomColor = 'transparent';
              }}
            >
              {children}
            </a>
          ),
          strong: ({ children }) => (
            <strong style={{ fontWeight: '600', color: '#1f2937' }}>
              {children}
            </strong>
          ),
          em: ({ children }) => (
            <em style={{ fontStyle: 'italic', color: '#6b7280' }}>
              {children}
            </em>
          ),
          hr: () => (
            <hr style={{ 
              border: 'none',
              height: '1px',
              backgroundColor: '#e5e7eb',
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

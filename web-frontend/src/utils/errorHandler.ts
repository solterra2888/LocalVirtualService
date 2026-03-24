/** FastAPI：detail 可能是 string、对象或校验错误数组，避免直接拼接出现 [object Object] */
export const formatFastAPIDetail = (detail: unknown): string => {
  if (detail == null || detail === '') return '';
  if (typeof detail === 'string') return detail;
  if (Array.isArray(detail)) {
    return detail
      .map((item: any) => {
        if (typeof item === 'string') return item;
        if (item?.msg != null) return String(item.msg);
        if (item?.message != null) return String(item.message);
        try {
          return JSON.stringify(item);
        } catch {
          return String(item);
        }
      })
      .filter(Boolean)
      .join('; ');
  }
  if (typeof detail === 'object' && detail !== null) {
    const d = detail as Record<string, unknown>;
    if (typeof d.msg === 'string' && d.msg) return d.msg;
    if (d.msg != null && typeof d.msg !== 'object') return String(d.msg);
    if ('message' in d) {
      const m = d.message;
      if (typeof m === 'string') return m;
      if (m != null && typeof m === 'object') {
        try {
          return JSON.stringify(m);
        } catch {
          return 'unknown error';
        }
      }
      if (m != null) return String(m);
    }
  }
  try {
    return JSON.stringify(detail);
  } catch {
    return String(detail);
  }
};

// 错误处理工具函数
export const getErrorMessage = (error: any, defaultMessage?: string): string => {
  if (typeof error === 'string') {
    return error;
  }
  
  if (error?.response?.data?.detail != null) {
    const formatted = formatFastAPIDetail(error.response.data.detail);
    if (formatted) return formatted;
  }
  
  if (error?.response?.data?.message) {
    return error.response.data.message;
  }
  
  if (error?.message) {
    return error.message;
  }
  
  if (error?.detail != null) {
    return formatFastAPIDetail(error.detail);
  }
  
  return defaultMessage || '未知错误';
};

export const handleAPIError = (error: any, defaultMessage: string = '操作失败'): string => {
  const message = getErrorMessage(error);
  console.error('API Error:', error);
  return message !== '未知错误' ? message : defaultMessage;
};
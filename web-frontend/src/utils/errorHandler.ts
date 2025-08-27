// 错误处理工具函数
export const getErrorMessage = (error: any, defaultMessage?: string): string => {
  if (typeof error === 'string') {
    return error;
  }
  
  if (error?.response?.data?.detail) {
    return error.response.data.detail;
  }
  
  if (error?.response?.data?.message) {
    return error.response.data.message;
  }
  
  if (error?.message) {
    return error.message;
  }
  
  if (error?.detail) {
    return error.detail;
  }
  
  return defaultMessage || '未知错误';
};

export const handleAPIError = (error: any, defaultMessage: string = '操作失败'): string => {
  const message = getErrorMessage(error);
  console.error('API Error:', error);
  return message !== '未知错误' ? message : defaultMessage;
};
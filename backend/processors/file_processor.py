# -*- coding: utf-8 -*-
"""
文件处理器模块
"""

import os
from backend.core.llama_service import LlamaService


class FileProcessor:
    """文件处理器类"""
    
    def __init__(self):
        """初始化文件处理器"""
        self.llama_service = LlamaService()
        self.supported_extensions = self.llama_service.get_supported_extensions()
    
    def scan_folder(self, folder_path):
        """
        扫描文件夹中的支持文件
        
        Args:
            folder_path: 文件夹路径
            
        Returns:
            list: 文件信息列表
        """
        if not folder_path or not os.path.isdir(folder_path):
            return []
        
        files_info = []
        
        try:
            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)
                
                # 跳过目录
                if os.path.isdir(file_path):
                    continue
                
                # 检查文件扩展名
                if self.is_supported_file(file_path):
                    file_info = self.get_file_info(file_path)
                    files_info.append(file_info)
        except Exception as e:
            print(f"扫描文件夹时出错: {e}")
        
        return files_info
    
    def is_supported_file(self, file_path):
        """
        检查文件是否为支持的类型
        
        Args:
            file_path: 文件路径
            
        Returns:
            bool: 是否支持
        """
        return self.llama_service.validate_file(file_path)
    
    def get_file_info(self, file_path):
        """
        获取文件详细信息
        
        Args:
            file_path: 文件路径
            
        Returns:
            dict: 文件信息字典
        """
        return self.llama_service.get_file_info(file_path)
    
    def validate_files(self, file_paths):
        """
        验证文件列表
        
        Args:
            file_paths: 文件路径列表
            
        Returns:
            tuple: (valid_files, invalid_files)
        """
        valid_files = []
        invalid_files = []
        
        for file_path in file_paths:
            if not os.path.exists(file_path):
                invalid_files.append({'path': file_path, 'reason': '文件不存在'})
                continue
            
            if not self.is_supported_file(file_path):
                file_ext = os.path.splitext(file_path)[1].lower()
                invalid_files.append({
                    'path': file_path, 
                    'reason': f'不支持的文件类型: {file_ext}'
                })
                continue
            
            valid_files.append(file_path)
        
        return valid_files, invalid_files
    
    def get_file_type_summary(self, file_paths):
        """
        获取文件类型汇总
        
        Args:
            file_paths: 文件路径列表
            
        Returns:
            dict: 文件类型统计
        """
        type_count = {}
        total_size = 0
        
        for file_path in file_paths:
            if os.path.exists(file_path):
                file_info = self.get_file_info(file_path)
                file_type = file_info['file_type']
                
                if file_type in type_count:
                    type_count[file_type]['count'] += 1
                    type_count[file_type]['size'] += file_info['file_size_mb']
                else:
                    type_count[file_type] = {
                        'count': 1,
                        'size': file_info['file_size_mb']
                    }
                
                total_size += file_info['file_size_mb']
        
        return {
            'types': type_count,
            'total_files': len(file_paths),
            'total_size_mb': round(total_size, 2)
        }
    
    def filter_files_by_type(self, file_paths, file_types):
        """
        按文件类型过滤文件
        
        Args:
            file_paths: 文件路径列表
            file_types: 允许的文件类型列表（如 ['PDF', 'XLSX']）
            
        Returns:
            list: 过滤后的文件路径列表
        """
        filtered_files = []
        
        for file_path in file_paths:
            if os.path.exists(file_path):
                file_info = self.get_file_info(file_path)
                if file_info['file_type'] in file_types:
                    filtered_files.append(file_path)
        
        return filtered_files
    
    def sort_files_by_size(self, file_paths, reverse=False):
        """
        按文件大小排序
        
        Args:
            file_paths: 文件路径列表
            reverse: 是否倒序（默认从小到大）
            
        Returns:
            list: 排序后的文件路径列表
        """
        file_size_pairs = []
        
        for file_path in file_paths:
            if os.path.exists(file_path):
                file_info = self.get_file_info(file_path)
                file_size_pairs.append((file_path, file_info['file_size_mb']))
        
        # 按大小排序
        file_size_pairs.sort(key=lambda x: x[1], reverse=reverse)
        
        return [pair[0] for pair in file_size_pairs]
    
    def get_supported_extensions(self):
        """获取支持的文件扩展名列表"""
        return self.supported_extensions.copy()
    
    def get_file_extension_description(self):
        """获取支持的文件扩展名描述"""
        return {
            '.pdf': 'PDF文档',
            '.xlsx': 'Excel工作簿 (新格式)',
            '.xls': 'Excel工作簿 (旧格式)',
            '.jpg': 'JPEG图片',
            '.jpeg': 'JPEG图片',
            '.png': 'PNG图片',
            '.gif': 'GIF图片',
            '.bmp': 'BMP位图'
        } 
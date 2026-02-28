# -*- coding: utf-8 -*-
"""
YouTube 视频转录服务
使用 youtube-transcript-api 获取 YouTube 视频字幕
"""

import re
import concurrent.futures
from typing import Dict, List, Any, Optional
from backend.utils.logger import info, error, warning

# 单次 transcript 网络请求的最长等待时间（秒）
_TRANSCRIPT_TIMEOUT_SEC = 30


class YouTubeService:
    """YouTube 转录服务类"""
    
    @staticmethod
    def extract_video_id(url: str) -> Optional[str]:
        """
        从 YouTube URL 中提取 video ID
        
        支持的格式：
        - https://www.youtube.com/watch?v=VIDEO_ID
        - https://youtu.be/VIDEO_ID
        - https://www.youtube.com/shorts/VIDEO_ID
        - https://www.youtube.com/embed/VIDEO_ID
        
        Args:
            url: YouTube URL
            
        Returns:
            video_id 或 None
        """
        patterns = [
            r'(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/|youtube\.com\/shorts\/)([a-zA-Z0-9_-]{11})',
            r'youtube\.com\/watch\?.*v=([a-zA-Z0-9_-]{11})',
            r'youtube\.com\/shorts\/([a-zA-Z0-9_-]{11})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        return None
    
    @staticmethod
    def fetch_transcript(video_url: str, languages: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        获取 YouTube 视频转录
        
        Args:
            video_url: YouTube 视频 URL
            languages: 优先语言列表，默认为 ['zh-Hans', 'zh', 'en']
            
        Returns:
            转录数据字典，包含：
            {
                'video_id': str,
                'video_url': str,
                'language': str,
                'is_generated': bool,
                'transcript': List[{'text': str, 'start': float, 'duration': float}]
            }
            
        Raises:
            ImportError: 如果 youtube-transcript-api 未安装
            ValueError: 如果 URL 无效或无法提取 video_id
            Exception: 其他转录获取错误
        """
        try:
            from youtube_transcript_api import YouTubeTranscriptApi
        except ImportError:
            error_msg = "youtube-transcript-api not installed. Please install: pip install youtube-transcript-api"
            error(error_msg, source="youtube_service")
            raise ImportError(error_msg)
        
        # 提取 video ID
        video_id = YouTubeService.extract_video_id(video_url)
        if not video_id:
            error_msg = f"Could not extract video ID from URL: {video_url}"
            error(error_msg, source="youtube_service")
            raise ValueError(error_msg)
        
        info(f"Fetching YouTube transcript: video_id={video_id}", source="youtube_service")
        
        # 设置默认语言优先级
        if languages is None:
            languages = ['zh-Hans', 'zh-CN', 'zh', 'zh-Hant', 'zh-TW', 'en', 'en-US']
        
        def _do_fetch() -> Dict[str, Any]:
            """在独立线程中执行实际的网络请求，以便外层能控制超时。"""
            api = YouTubeTranscriptApi()
            transcript_list = api.list(video_id)

            transcript = None
            language_used = None

            for lang in languages:
                try:
                    transcript = transcript_list.find_transcript([lang])
                    language_used = lang
                    info(f"Found transcript in language: {lang}", source="youtube_service")
                    break
                except:
                    continue

            if transcript is None:
                try:
                    transcript = transcript_list.find_manually_created_transcript(['zh', 'en'])
                    language_used = transcript.language_code
                    info(f"Found manual transcript in: {language_used}", source="youtube_service")
                except:
                    pass

            if transcript is None:
                try:
                    transcript = transcript_list.find_generated_transcript(['zh', 'en'])
                    language_used = transcript.language_code
                    warning(f"Using auto-generated transcript in: {language_used}", source="youtube_service")
                except:
                    pass

            if transcript is None:
                for t in transcript_list:
                    transcript = t
                    language_used = t.language_code
                    info(f"Using available transcript in: {language_used}", source="youtube_service")
                    break

            if transcript is None:
                raise Exception("No transcript available for this video")

            transcript_data = transcript.fetch()
            transcript_list_dict = [
                {
                    'text': entry.text,
                    'start': entry.start,
                    'duration': entry.duration
                }
                for entry in transcript_data
            ]

            return {
                "video_id": video_id,
                "video_url": video_url,
                "language": language_used,
                "is_generated": getattr(transcript, 'is_generated', False),
                "transcript": transcript_list_dict,
            }

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_do_fetch)
                try:
                    result = future.result(timeout=_TRANSCRIPT_TIMEOUT_SEC)
                except concurrent.futures.TimeoutError:
                    error_msg = (
                        f"Transcript fetch timed out after {_TRANSCRIPT_TIMEOUT_SEC}s "
                        f"(video_id={video_id})"
                    )
                    error(error_msg, source="youtube_service")
                    raise Exception(error_msg)

            info(f"Successfully fetched transcript: {len(result['transcript'])} segments",
                 source="youtube_service")
            return result

        except Exception as e:
            error_msg = f"Failed to fetch transcript: {str(e)}"
            error(error_msg, source="youtube_service")
            raise Exception(error_msg)
    
    @staticmethod
    def get_full_text(transcript_data: Dict[str, Any]) -> str:
        """
        从转录数据中提取完整文本
        
        Args:
            transcript_data: 转录数据字典
            
        Returns:
            拼接后的完整文本
        """
        if not transcript_data or 'transcript' not in transcript_data:
            return ""
        
        segments = transcript_data['transcript']
        return '\n'.join([seg['text'] for seg in segments])
    
    @staticmethod
    def format_time(seconds: float) -> str:
        """
        将秒数格式化为 MM:SS 格式
        
        Args:
            seconds: 秒数
            
        Returns:
            格式化的时间字符串 (MM:SS)
        """
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes:02d}:{secs:02d}"
    
    @staticmethod
    def get_transcript_with_timestamps(transcript_data: Dict[str, Any]) -> str:
        """
        获取带时间戳的转录文本
        
        Args:
            transcript_data: 转录数据字典
            
        Returns:
            带时间戳的文本，每行格式：[MM:SS] text
        """
        if not transcript_data or 'transcript' not in transcript_data:
            return ""
        
        segments = transcript_data['transcript']
        lines = []
        
        for seg in segments:
            timestamp = YouTubeService.format_time(seg['start'])
            lines.append(f"[{timestamp}] {seg['text']}")
        
        return '\n'.join(lines)

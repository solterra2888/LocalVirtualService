# -*- coding: utf-8 -*-
"""
新闻主题初始化脚本
"""

from typing import Dict, List
from .eodhd_news_service import NewsThemeService


def initialize_default_themes():
    """初始化系统默认主题"""
    
    default_themes = {
        'political': {
            'description': '政治相关新闻',
            'color': '#FFE4B5',
            'keywords': [
                'president', 'election', 'government', 'congress', 'senate', 'house', 
                'democrat', 'republican', 'parliament', 'prime minister', 'vote', 
                'campaign', 'policy', 'politician', 'administration', 'ballot', 
                'legislation', 'lawmaker', 'politics', 'political'
            ]
        },
        'geopolitical': {
            'description': '地缘政治相关新闻',
            'color': '#FFB6C1',
            'keywords': [
                'sanctions', 'trade war', 'treaty', 'international', 'diplomatic', 
                'alliance', 'conflict', 'territorial', 'dispute', 'summit', 'foreign policy',
                'global tension', 'security', 'defense', 'military', 'nuclear', 'territory',
                'borders', 'sovereignty', 'geopolitical', 'diplomatic relations'
            ]
        },
        'economic': {
            'description': '经济相关新闻',
            'color': '#98FB98',
            'keywords': [
                'economy', 'economic', 'GDP', 'inflation', 'recession', 'growth',
                'central bank', 'fed', 'federal reserve', 'interest rate', 'monetary policy',
                'fiscal policy', 'budget', 'deficit', 'debt', 'economic outlook'
            ]
        },
        'conflict': {
            'description': '冲突和军事相关新闻',
            'color': '#FFA07A',
            'keywords': [
                'war', 'military', 'armed conflict', 'invasion', 'troops', 'bombing',
                'missile', 'attack', 'violence', 'combat', 'battlefield', 'artillery',
                'casualties', 'hostilities', 'fighting', 'insurgency', 'civil war'
            ]
        },
        'world': {
            'description': '世界新闻',
            'color': '#87CEEB',
            'keywords': [
                'global', 'world', 'international', 'foreign', 'overseas', 'worldwide',
                'continent', 'nation', 'country', 'region', 'UN', 'united nations',
                'diplomatic', 'international relations', 'global affairs'
            ]
        },
        'technology': {
            'description': '科技相关新闻',
            'color': '#DDA0DD',
            'keywords': [
                'technology', 'tech', 'AI', 'artificial intelligence', 'machine learning',
                'software', 'hardware', 'digital', 'innovation', 'startup', 'silicon valley',
                'blockchain', 'cryptocurrency', 'cyber', 'internet', 'cloud computing'
            ]
        },
        'finance': {
            'description': '金融市场新闻',
            'color': '#F0E68C',
            'keywords': [
                'stock market', 'financial', 'finance', 'investment', 'trading', 'broker',
                'wall street', 'nasdaq', 'nyse', 'dow jones', 's&p 500', 'earnings',
                'revenue', 'profit', 'loss', 'dividend', 'ipo', 'merger', 'acquisition'
            ]
        },
        'energy': {
            'description': '能源相关新闻',
            'color': '#FFA500',
            'keywords': [
                'energy', 'oil', 'gas', 'petroleum', 'renewable', 'solar', 'wind',
                'coal', 'nuclear', 'electricity', 'power', 'fuel', 'battery',
                'electric vehicle', 'green energy', 'climate', 'carbon'
            ]
        }
    }
    
    theme_service = NewsThemeService()
    
    # 获取现有主题
    existing_themes = theme_service.get_all_themes()
    existing_names = {theme['topic_name'].lower() for theme in existing_themes}
    
    created_count = 0
    for topic_name, topic_data in default_themes.items():
        if topic_name.lower() not in existing_names:
            try:
                theme_id = theme_service.create_theme(
                    topic_name=topic_name,
                    description=topic_data['description'],
                    keywords=topic_data['keywords'],
                    color=topic_data['color']
                )
                if theme_id:
                    created_count += 1
                    print(f"✓ 创建默认主题: {topic_name}")
                else:
                    print(f"✗ 创建主题失败: {topic_name}")
            except Exception as e:
                print(f"✗ 创建主题 {topic_name} 时出错: {str(e)}")
        else:
            print(f"- 主题已存在，跳过: {topic_name}")
    
    print(f"\n主题初始化完成，新创建了 {created_count} 个主题")
    return created_count


def get_default_themes_config() -> Dict[str, Dict[str, any]]:
    """获取默认主题配置（用于重置功能）"""
    return {
        'political': {
            'description': '政治相关新闻',
            'color': '#FFE4B5',
            'keywords': [
                'president', 'election', 'government', 'congress', 'senate', 'house', 
                'democrat', 'republican', 'parliament', 'prime minister', 'vote', 
                'campaign', 'policy', 'politician', 'administration', 'ballot', 
                'legislation', 'lawmaker', 'politics', 'political'
            ]
        },
        'geopolitical': {
            'description': '地缘政治相关新闻',
            'color': '#FFB6C1',
            'keywords': [
                'sanctions', 'trade war', 'treaty', 'international', 'diplomatic', 
                'alliance', 'conflict', 'territorial', 'dispute', 'summit', 'foreign policy',
                'global tension', 'security', 'defense', 'military', 'nuclear', 'territory',
                'borders', 'sovereignty', 'geopolitical', 'diplomatic relations'
            ]
        },
        'economic': {
            'description': '经济相关新闻',
            'color': '#98FB98',
            'keywords': [
                'economy', 'economic', 'GDP', 'inflation', 'recession', 'growth',
                'central bank', 'fed', 'federal reserve', 'interest rate', 'monetary policy',
                'fiscal policy', 'budget', 'deficit', 'debt', 'economic outlook'
            ]
        },
        'conflict': {
            'description': '冲突和军事相关新闻',
            'color': '#FFA07A',
            'keywords': [
                'war', 'military', 'armed conflict', 'invasion', 'troops', 'bombing',
                'missile', 'attack', 'violence', 'combat', 'battlefield', 'artillery',
                'casualties', 'hostilities', 'fighting', 'insurgency', 'civil war'
            ]
        },
        'world': {
            'description': '世界新闻',
            'color': '#87CEEB',
            'keywords': [
                'global', 'world', 'international', 'foreign', 'overseas', 'worldwide',
                'continent', 'nation', 'country', 'region', 'UN', 'united nations',
                'diplomatic', 'international relations', 'global affairs'
            ]
        },
        'technology': {
            'description': '科技相关新闻',
            'color': '#DDA0DD',
            'keywords': [
                'technology', 'tech', 'AI', 'artificial intelligence', 'machine learning',
                'software', 'hardware', 'digital', 'innovation', 'startup', 'silicon valley',
                'blockchain', 'cryptocurrency', 'cyber', 'internet', 'cloud computing'
            ]
        },
        'finance': {
            'description': '金融市场新闻',
            'color': '#F0E68C',
            'keywords': [
                'stock market', 'financial', 'finance', 'investment', 'trading', 'broker',
                'wall street', 'nasdaq', 'nyse', 'dow jones', 's&p 500', 'earnings',
                'revenue', 'profit', 'loss', 'dividend', 'ipo', 'merger', 'acquisition'
            ]
        },
        'energy': {
            'description': '能源相关新闻',
            'color': '#FFA500',
            'keywords': [
                'energy', 'oil', 'gas', 'petroleum', 'renewable', 'solar', 'wind',
                'coal', 'nuclear', 'electricity', 'power', 'fuel', 'battery',
                'electric vehicle', 'green energy', 'climate', 'carbon'
            ]
        }
    }


if __name__ == "__main__":
    # 直接运行脚本时初始化默认主题
    print("开始初始化默认新闻主题...")
    try:
        count = initialize_default_themes()
        print(f"初始化完成！创建了 {count} 个新主题。")
    except Exception as e:
        print(f"初始化失败: {str(e)}")


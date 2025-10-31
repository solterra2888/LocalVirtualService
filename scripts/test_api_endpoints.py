#!/usr/bin/env python
"""
测试统一主题API端点
"""
import requests
import json

BASE_URL = "http://localhost:8000/api"

# 需要先登录获取token
def login():
    """登录获取token"""
    response = requests.post(
        f"{BASE_URL}/auth/login",
        data={
            "username": "admin",  # 替换为你的用户名
            "password": "admin"   # 替换为你的密码
        }
    )
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        print(f"登录失败: {response.text}")
        return None

def test_trending_themes(token):
    """测试trending主题API"""
    print("\n1. 测试 GET /themes-unified/trending")
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(
        f"{BASE_URL}/themes-unified/trending",
        headers=headers,
        params={"days": 7, "min_news_count": 2, "limit": 50}
    )
    print(f"状态码: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"✅ 成功！返回 {data['total']} 个trending主题")
        if data['items']:
            print(f"示例主题: {data['items'][0]['theme_name']} ({data['items'][0]['news_count']}条新闻)")
    else:
        print(f"❌ 失败: {response.text}")

def test_fused_themes(token, news_id=1556):
    """测试单个新闻的融合主题API"""
    print(f"\n2. 测试 GET /themes-unified/news/{news_id}/fused-themes")
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(
        f"{BASE_URL}/themes-unified/news/{news_id}/fused-themes",
        headers=headers,
        params={"min_confidence": 0.3, "top_n": 10}
    )
    print(f"状态码: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"✅ 成功！返回 {len(data.get('themes', []))} 个融合主题")
        if data.get('themes'):
            print(f"示例主题: {data['themes'][0]['theme_name']}")
    else:
        print(f"❌ 失败: {response.text}")

def test_batch_fused_themes(token, news_ids=[1556, 1557, 1558]):
    """测试批量获取融合主题API"""
    print(f"\n3. 测试 POST /themes-unified/news/fused-themes/batch")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    response = requests.post(
        f"{BASE_URL}/themes-unified/news/fused-themes/batch",
        headers=headers,
        json={
            "news_ids": news_ids,
            "min_confidence": 0.3,
            "top_n": 10
        }
    )
    print(f"状态码: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"✅ 成功！返回 {len(data)} 条新闻的融合主题")
        for news_id, themes in list(data.items())[:2]:
            print(f"  新闻{news_id}: {len(themes.get('themes', []))} 个主题")
    else:
        print(f"❌ 失败: {response.text}")

def main():
    print("="*60)
    print("  测试统一主题API端点")
    print("="*60)
    
    # 登录
    token = login()
    if not token:
        print("❌ 无法获取token，测试终止")
        return
    
    print("\n✅ 登录成功")
    
    # 测试各个端点
    test_trending_themes(token)
    test_fused_themes(token)
    test_batch_fused_themes(token)
    
    print("\n" + "="*60)
    print("  测试完成")
    print("="*60)

if __name__ == "__main__":
    main()


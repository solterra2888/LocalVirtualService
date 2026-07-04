import requests
import json

# --- 配置区域：请替换为你的信息 ---
ACCESS_TOKEN = '2.00v6_i9CK7kElC7cc8c426428jCRWB'  # 替换成你刚刚获取到的Access Token
USER_ID = '2256564147'           # 替换成你想查询的微博用户ID
# USER_ID = '1223178222'           # 替换成你想查询的微博用户ID
# ---------------------------------

def get_user_timeline(user_id, access_token, count=5):
    """
    获取指定用户的最新微博
    """
    url = 'https://api.weibo.com/2/statuses/user_timeline.json'  # API接口地址[reference:0][reference:1]
    params = {
        'access_token': access_token,  # 访问令牌[reference:2]
        'uid': user_id,                # 用户ID[reference:3]
        'count': count                 # 获取的微博数量
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # 如果请求失败(如401, 403等)，会抛出异常
        
        result = response.json()
        return result
    except requests.exceptions.RequestException as e:
        print(f"请求出错: {e}")
        return None

if __name__ == '__main__':
    print("正在获取用户微博...")
    data = get_user_timeline(USER_ID, ACCESS_TOKEN)
    
    if data and 'statuses' in data:
        print(f"\n成功获取 {len(data['statuses'])} 条微博:\n")
        print(json.dumps(data, indent=2, ensure_ascii=False))  # 打印完整的JSON数据
    elif data and 'error' in data:
        print(f"\nAPI返回错误: {data['error']} (错误码: {data.get('error_code', 'N/A')})")
    else:
        print("\n获取数据失败，请检查网络或参数。")
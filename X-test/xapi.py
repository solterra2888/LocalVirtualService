import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import psycopg2
from datetime import datetime, timedelta
import pytz
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 配置
X_API_KEY = "new1_958a42497f844e1e9dd1b866cb54a9d9"
TARGET_ACCOUNTS = ["zerohedge", "wallstengine"]  # 需要监控的账号列表
CHECK_INTERVAL_DAY = 30 * 60  # 白天每20分钟检查一次（秒）
CHECK_INTERVAL_NIGHT = 2 * 60 * 60  # 晚上每2小时检查一次（秒）
ACCOUNT_REQUEST_INTERVAL = 10  # 账号之间请求间隔（秒）
INITIAL_QUERY_HOURS = 12  # 首次运行时查询过去多少小时的推文

# 数据库配置
DB_NAME = "rc_database"
DB_USER = "postgres"
DB_PASSWORD = "1q23l,Sc45+@!"
DB_HOST = "111.230.48.119"
DB_PORT = "5432"

# 初始化上次检查时间（每个账号单独记录）
last_checked_times = {account: datetime.utcnow() - timedelta(hours=INITIAL_QUERY_HOURS) for account in TARGET_ACCOUNTS}

def create_session_with_retries():
    """创建带有重试机制的requests session"""
    session = requests.Session()
    
    # 配置重试策略
    retry_strategy = Retry(
        total=5,  # 总共重试5次
        backoff_factor=2,  # 重试间隔：2, 4, 8, 16, 32秒
        status_forcelist=[429, 500, 502, 503, 504],  # 这些状态码会触发重试
        allowed_methods=["GET", "POST"]  # 允许重试的HTTP方法
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def is_night_time():
    """判断当前是否是美国晚上12点到早上10点（使用美国东部时间）"""
    us_eastern = pytz.timezone('US/Eastern')
    now = datetime.now(us_eastern)
    hour = now.hour
    return 0 <= hour < 10

def get_check_interval():
    """根据时间段返回检查间隔"""
    return CHECK_INTERVAL_NIGHT if is_night_time() else CHECK_INTERVAL_DAY

def init_database():
    """初始化数据库连接和表"""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            connect_timeout=10
        )
        cursor = conn.cursor()
        
        # 创建twitter_tweets表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS twitter_tweets (
            id SERIAL PRIMARY KEY,
            tweet_id VARCHAR(50) UNIQUE,
            author VARCHAR(255),
            time TIMESTAMP,
            text TEXT,
            url VARCHAR(255),
            has_retweet BOOLEAN,
            retweet_author VARCHAR(255),
            retweet_time TIMESTAMP,
            retweet_text TEXT,
            retweet_url VARCHAR(255),
            has_quoted BOOLEAN,
            quoted_text TEXT,
            quoted_url VARCHAR(255),
            media_url_https TEXT
        )
        ''')
        
        # 为已存在的表添加tweet_id字段（如果不存在）
        cursor.execute('''
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'twitter_tweets' AND column_name = 'tweet_id'
            ) THEN
                ALTER TABLE twitter_tweets ADD COLUMN tweet_id VARCHAR(50) UNIQUE;
            END IF;
        END $$;
        ''')
        
        conn.commit()
        return conn, cursor
    except psycopg2.Error as e:
        logging.error(f"数据库连接失败: {str(e)}")
        raise

def extract_media_urls(tweet_obj):
    """从推文对象中提取所有媒体URL"""
    media_urls = []
    
    # 从extendedEntities中提取媒体
    if 'extendedEntities' in tweet_obj and 'media' in tweet_obj['extendedEntities']:
        for media in tweet_obj['extendedEntities']['media']:
            if 'media_url_https' in media:
                media_urls.append(media['media_url_https'])
    
    return media_urls

def save_tweet_to_db(conn, cursor, tweet):
    """将推文保存到数据库"""
    try:
        # 提取推文ID（唯一标识）
        tweet_id = tweet.get('id')
        if not tweet_id:
            logging.warning("  推文缺少ID，跳过保存")
            return False
        
        # 提取基本信息
        author = tweet['author']['name'] if 'author' in tweet and 'name' in tweet['author'] else None
        created_at = tweet['createdAt'] if 'createdAt' in tweet else None
        text = tweet['text'] if 'text' in tweet else None
        url = tweet['url'] if 'url' in tweet else None
        
        # 提取媒体URL
        media_urls = []
        
        # 1. 从主推文提取媒体
        media_urls.extend(extract_media_urls(tweet))
        
        # 提取转发信息
        has_retweet = 'retweeted_tweet' in tweet and tweet['retweeted_tweet'] is not None
        retweet_author = None
        retweet_time = None
        retweet_text = None
        retweet_url = None
        
        if has_retweet:
            rt = tweet['retweeted_tweet']
            retweet_author = rt['author']['name'] if 'author' in rt and 'name' in rt['author'] else None
            retweet_time = rt['createdAt'] if 'createdAt' in rt else None
            retweet_text = rt['text'] if 'text' in rt else None
            retweet_url = rt['url'] if 'url' in rt else None
            
            # 2. 从转发的推文提取媒体
            media_urls.extend(extract_media_urls(rt))
            
            # 3. 从转发推文的引用推文中提取媒体
            if 'quoted_tweet' in rt and rt['quoted_tweet'] is not None:
                media_urls.extend(extract_media_urls(rt['quoted_tweet']))
        
        # 提取引用信息
        has_quoted = 'quoted_tweet' in tweet and tweet['quoted_tweet'] is not None
        quoted_text = None
        quoted_url = None
        
        if has_quoted:
            qt = tweet['quoted_tweet']
            quoted_text = qt['text'] if 'text' in qt else None
            quoted_url = qt['url'] if 'url' in qt else None
            
            # 4. 从引用的推文提取媒体
            media_urls.extend(extract_media_urls(qt))
        
        # 将媒体URL列表转换为逗号分隔的字符串
        media_url_https = ','.join(media_urls) if media_urls else None
        
        # 插入数据库，使用ON CONFLICT避免重复
        cursor.execute('''
        INSERT INTO twitter_tweets 
        (tweet_id, author, time, text, url, has_retweet, retweet_author, retweet_time, retweet_text, retweet_url, has_quoted, quoted_text, quoted_url, media_url_https)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tweet_id) DO NOTHING
        RETURNING id
        ''', (
            tweet_id, author, created_at, text, url, 
            has_retweet, retweet_author, retweet_time, retweet_text, retweet_url,
            has_quoted, quoted_text, quoted_url, media_url_https
        ))
        
        result = cursor.fetchone()
        conn.commit()
        
        if result:  # 如果有返回值，说明插入成功
            if media_urls:
                logging.info(f"  └─ 保存了 {len(media_urls)} 张图片URL")
            return True
        else:  # 没有返回值说明冲突被忽略
            return False
            
    except psycopg2.Error as e:
        logging.error(f"数据库插入错误: {str(e)}")
        conn.rollback()
        return False
    except Exception as e:
        logging.error(f"保存推文时发生错误: {str(e)}")
        conn.rollback()
        return False

def check_for_new_tweets(account, conn, cursor, session):
    """检查指定账号的新推文并保存到数据库"""
    global last_checked_times
    
    try:
        # 为API查询格式化时间
        until_time = datetime.utcnow()
        since_time = last_checked_times[account]
        
        # 按Twitter API要求的格式将时间转换为字符串
        since_str = since_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
        until_str = until_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
        
        # 构建查询语句
        query = f"from:{account} since:{since_str} until:{until_str} include:nativeretweets"
        
        # 计算查询的时间跨度
        time_span = until_time - since_time
        hours_span = time_span.total_seconds() / 3600
        
        logging.info(f"  查询时间范围: {since_str} 至 {until_str} (跨度: {hours_span:.1f}小时)")
        logging.debug(f"  查询语句: {query}")
        
        # API接口地址
        url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
        
        # 请求参数
        params = {
            "query": query,
            "queryType": "Latest"
        }
        
        # 带API密钥的请求头
        headers = {
            "X-API-Key": X_API_KEY
        }
        
        # 发起请求并处理分页
        all_tweets = []
        next_cursor = None
        
        while True:
            try:
                # 如果有游标则添加到参数中
                if next_cursor:
                    params["cursor"] = next_cursor
                
                # 使用session发起请求，添加超时设置
                response = session.get(url, headers=headers, params=params, timeout=30, verify=True)
                
                # 解析响应
                if response.status_code == 200:
                    data = response.json()
                    
                    # 调试：显示API响应的结构
                    logging.debug(f"  API响应键: {list(data.keys())}")
                    
                    # Twitter API可能返回不同的数据结构
                    # 尝试从不同位置获取推文数据
                    tweets = None
                    if isinstance(data, list):
                        # 如果返回的直接是数组
                        tweets = data
                        logging.debug(f"  API直接返回推文数组，数量: {len(tweets)}")
                    elif "tweets" in data:
                        tweets = data.get("tweets", [])
                        logging.debug(f"  从data['tweets']获取推文，数量: {len(tweets) if tweets else 0}")
                    elif "data" in data:
                        tweets = data.get("data", [])
                        logging.debug(f"  从data['data']获取推文，数量: {len(tweets) if tweets else 0}")
                    else:
                        tweets = []
                        logging.warning(f"  未找到推文数据，API响应: {str(data)[:200]}")
                    
                    if tweets:
                        all_tweets.extend(tweets)
                        logging.info(f"  本次请求获取 {len(tweets)} 条推文")
                    
                    # 检查是否有更多分页
                    has_next = data.get("has_next_page", False) or data.get("hasNextPage", False)
                    next_cursor_val = data.get("next_cursor", "") or data.get("nextCursor", "")
                    
                    if has_next and next_cursor_val:
                        next_cursor = next_cursor_val
                        logging.debug(f"  有更多分页，继续获取...")
                        time.sleep(1)  # 分页请求之间稍微等待
                        continue
                    else:
                        break
                else:
                    logging.error(f"API请求失败: {response.status_code} - {response.text[:200]}")
                    break
                    
            except requests.exceptions.SSLError as e:
                logging.error(f"SSL错误，账号 {account}: {str(e)[:100]}")
                logging.info("等待30秒后继续...")
                time.sleep(30)
                break
            except requests.exceptions.Timeout as e:
                logging.error(f"请求超时，账号 {account}: {str(e)}")
                break
            except requests.exceptions.RequestException as e:
                logging.error(f"网络请求错误，账号 {account}: {str(e)[:100]}")
                break
                
        # 处理所有收集到的推文
        if all_tweets:
            logging.info(f"✓ 发现 {account} 的推文总数: {len(all_tweets)} 条！")
            saved_count = 0
            skipped_count = 0
            
            for tweet in all_tweets:
                try:
                    tweet_text = tweet.get('text', '')[:60]
                    logging.info(f"  [{tweet.get('createdAt', 'N/A')}] {tweet_text}...")
                    
                    # save_tweet_to_db 返回 True 表示保存成功，False 表示跳过
                    if save_tweet_to_db(conn, cursor, tweet):
                        saved_count += 1
                    else:
                        skipped_count += 1
                        
                except Exception as e:
                    logging.error(f"  保存推文时出错: {str(e)}")
                    skipped_count += 1
                    continue
            
            if saved_count > 0:
                logging.info(f"✓ 成功保存 {saved_count} 条新推文")
            if skipped_count > 0:
                logging.info(f"○ 跳过 {skipped_count} 条已存在的推文")
            if saved_count == 0 and len(all_tweets) > 0:
                logging.info(f"○ 所有推文均已存在")
        else:
            logging.info(f"○ 在指定时间范围内，{account} 没有新推文。")
        
        # 更新最后检查时间
        last_checked_times[account] = until_time
        
    except Exception as e:
        logging.error(f"检查账号 {account} 时发生未预期错误: {str(e)}")
        logging.exception("详细错误信息:")

def main():
    """主函数"""
    logging.info("初始化数据库连接...")
    
    conn = None
    cursor = None
    session = None
    
    try:
        conn, cursor = init_database()
        logging.info("数据库连接成功！")
        
        # 创建带重试机制的session
        session = create_session_with_retries()
        logging.info("HTTP Session 已创建")
        
        logging.info(f"开始监控以下账号的推文: {', '.join(TARGET_ACCOUNTS)}")
        logging.info(f"首次查询时间范围: 过去 {INITIAL_QUERY_HOURS} 小时")
        
        while True:
            try:
                current_interval = get_check_interval()
                time_status = "晚上" if is_night_time() else "白天"
                logging.info(f"当前为{time_status}时段，检查间隔为 {current_interval} 秒")
                
                for account in TARGET_ACCOUNTS:
                    try:
                        logging.info(f"正在检查账号 @{account} 的推文...")
                        check_for_new_tweets(account, conn, cursor, session)
                        
                        # 账号之间的请求间隔
                        if account != TARGET_ACCOUNTS[-1]:  # 如果不是最后一个账号
                            logging.info(f"等待 {ACCOUNT_REQUEST_INTERVAL} 秒后检查下一个账号...")
                            time.sleep(ACCOUNT_REQUEST_INTERVAL)
                    except Exception as e:
                        logging.error(f"处理账号 {account} 时出错: {str(e)}")
                        continue  # 继续处理下一个账号
                
                logging.info(f"所有账号检查完毕，等待 {current_interval} 秒后进行下一轮检查...")
                time.sleep(current_interval)
                
            except Exception as e:
                logging.error(f"检查循环中发生错误: {str(e)}")
                logging.info("等待60秒后继续...")
                time.sleep(60)
                
    except KeyboardInterrupt:
        logging.info("监控已停止（用户中断）")
    except Exception as e:
        logging.error(f"程序发生致命错误: {str(e)}")
        logging.exception("详细错误信息:")
    finally:
        # 关闭资源
        if session:
            session.close()
            logging.info("HTTP Session 已关闭")
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logging.info("数据库连接已关闭")

if __name__ == "__main__":
    main()
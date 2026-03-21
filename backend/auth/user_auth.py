# -*- coding: utf-8 -*-
"""
用户认证模块
"""

import hashlib
import os
import secrets
import psycopg2
from datetime import datetime
from config.settings import config


class UserAuth:
    """用户认证类"""
    
    def __init__(self, db_conn_params=None):
        """初始化用户认证"""
        self.db_conn_params = db_conn_params or {}
    
    def get_connection(self):
        """获取数据库连接"""
        try:
            # 使用PostgreSQL连接
            if (self.db_conn_params.get('db_host') and 
                self.db_conn_params.get('db_name') and 
                self.db_conn_params.get('db_user')):
                
                print(f"正在连接PostgreSQL: {self.db_conn_params.get('db_host')}:{self.db_conn_params.get('db_port')}")
                conn = psycopg2.connect(
                    host=self.db_conn_params.get('db_host', ''),
                    port=self.db_conn_params.get('db_port', '5432'),
                    database=self.db_conn_params.get('db_name', ''),
                    user=self.db_conn_params.get('db_user', ''),
                    password=self.db_conn_params.get('db_password', ''),
                    connect_timeout=10
                )
                print("PostgreSQL连接成功！")
                return conn
            else:
                print("数据库连接参数不完整，无法连接到PostgreSQL")
                return None
        except Exception as e:
            print(f"PostgreSQL连接失败: {e}")
            return None
    
    def get_user_info(self, username: str) -> dict:
        """获取用户信息"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            # 首先检查表结构
            cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'users' AND column_name = 'last_login'
            """)
            has_last_login = cursor.fetchone() is not None
            
            cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'users' AND column_name = 'avatar_url'
            """)
            has_avatar = cursor.fetchone() is not None

            cols = "id, username, password_hash, salt, is_admin, created_at"
            if has_last_login:
                cols += ", last_login"
            if has_avatar:
                cols += ", avatar_url"

            cursor.execute(f"SELECT {cols} FROM users WHERE username = %s", (username,))
            result = cursor.fetchone()
            if not result:
                return None
            
            idx = 6
            last_login = None
            avatar_url = None
            if has_last_login:
                last_login = result[idx]; idx += 1
            if has_avatar:
                avatar_url = result[idx]

            return {
                'id': result[0],
                'username': result[1],
                'password_hash': result[2],
                'salt': result[3],
                'is_admin': bool(result[4]),
                'created_at': result[5],
                'last_login': last_login,
                'avatar_url': avatar_url
            }
        except Exception as e:
            print(f"获取用户信息时出错: {e}")
            return None
        finally:
            conn.close()
    
    def update_last_login(self, username: str):
        """更新最后登录时间"""
        conn = self.get_connection()
        if not conn:
            return
        
        try:
            cursor = conn.cursor()
            # 检查是否有last_login列
            cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'users' AND column_name = 'last_login'
            """)
            has_last_login = cursor.fetchone() is not None
            
            if has_last_login:
                cursor.execute("""
                UPDATE users SET last_login = %s WHERE username = %s
                """, (datetime.now(), username))
                conn.commit()
            else:
                print("users表没有last_login列，跳过更新最后登录时间")
        except Exception as e:
            print(f"更新最后登录时间时出错: {e}")
        finally:
            conn.close()

    def get_token_version(self, username: str):
        """读取用户当前会话版本（用于校验 JWT 是否仍为最新登录会话）"""
        conn = self.get_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'users' AND column_name = 'token_version'
            """)
            if cursor.fetchone() is None:
                return None
            cursor.execute(
                "SELECT token_version FROM users WHERE username = %s",
                (username,),
            )
            row = cursor.fetchone()
            if row is None:
                return -1
            return int(row[0])
        except Exception as e:
            print(f"读取token_version时出错: {e}")
            return None
        finally:
            conn.close()

    def increment_token_version(self, username: str):
        """新登录时递增会话版本，使其它设备上的旧 JWT 失效"""
        conn = self.get_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'users' AND column_name = 'token_version'
            """)
            if cursor.fetchone() is None:
                return None
            cursor.execute(
                """
                UPDATE users SET token_version = token_version + 1
                WHERE username = %s
                RETURNING token_version
                """,
                (username,),
            )
            row = cursor.fetchone()
            conn.commit()
            return int(row[0]) if row else None
        except Exception as e:
            print(f"递增token_version时出错: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()
    
    @staticmethod
    def hash_password(password, salt=None):
        """使用盐值哈希密码"""
        if salt is None:
            salt = secrets.token_hex(16)  # 生成32字符的随机盐值
        
        # 使用PBKDF2算法和SHA256哈希函数
        key = hashlib.pbkdf2_hmac(
            'sha256',  # 哈希算法
            password.encode('utf-8'),  # 密码转为字节
            salt.encode('utf-8'),  # 盐值转为字节
            100000,  # 迭代次数
            dklen=64  # 密钥长度
        )
        
        # 将结果转换为十六进制
        hashed_password = key.hex()
        
        return hashed_password, salt
    
    @staticmethod
    def verify_password(stored_hash, stored_salt, provided_password):
        """验证密码是否匹配存储的哈希值"""
        try:
            # 确保输入参数有效
            if not stored_hash or not stored_salt or not provided_password:
                print("验证密码失败: 参数无效")
                return False
                
            print(f"验证密码: 存储的哈希值: {stored_hash[:10]}..., 盐值: {stored_salt}")
            
            # 使用相同的盐值哈希提供的密码
            calculated_hash, _ = UserAuth.hash_password(provided_password, stored_salt)
            
            print(f"计算的哈希值: {calculated_hash[:10]}...")
            
            # 比较计算出的哈希值和存储的哈希值
            result = calculated_hash == stored_hash
            print(f"密码验证结果: {'匹配' if result else '不匹配'}")
            
            return result
        except Exception as e:
            print(f"密码验证过程中出错: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    @staticmethod
    def setup_users_table(conn):
        """设置用户表（仅支持PostgreSQL）"""
        cursor = conn.cursor()
        
        try:
            # PostgreSQL
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                salt VARCHAR(255) NOT NULL,
                is_admin BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Ensure avatar_url column exists
            cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'users' AND column_name = 'avatar_url'
            """)
            if cursor.fetchone() is None:
                cursor.execute("ALTER TABLE users ADD COLUMN avatar_url TEXT")
                print("已添加avatar_url列到users表")

            cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'users' AND column_name = 'token_version'
            """)
            if cursor.fetchone() is None:
                cursor.execute(
                    "ALTER TABLE users ADD COLUMN token_version INTEGER NOT NULL DEFAULT 1"
                )
                cursor.execute("UPDATE users SET token_version = 1")
                print("已添加token_version列到users表")
            
            # 检查是否有任何用户
            cursor.execute("SELECT COUNT(*) FROM users")
            user_count = cursor.fetchone()[0]
            
            # 如果没有用户，创建默认管理员
            if user_count == 0:
                print("创建默认管理员账户(PostgreSQL)...")
                admin_config = {
                    'user': os.getenv('DEFAULT_ADMIN_USER', 'admin'),
                    'password': os.getenv('DEFAULT_ADMIN_PASSWORD')
                }
                
                if not admin_config['password']:
                    raise ValueError("DEFAULT_ADMIN_PASSWORD 环境变量未设置，无法创建默认管理员账户")
                admin_pass, admin_salt = UserAuth.hash_password(admin_config['password'])
                cursor.execute("""
                INSERT INTO users (username, password_hash, salt, is_admin) 
                VALUES (%s, %s, %s, %s)
                """, (admin_config['user'], admin_pass, admin_salt, True))
                print(f"已创建默认管理员账户，用户名：{admin_config['user']}，密码：{'*' * len(admin_config['password'])}")
                print(f"密码哈希: {admin_pass[:10]}... 盐值: {admin_salt}")
            else:
                print(f"PostgreSQL数据库中已存在{user_count}个用户账户")
            
            conn.commit()
        except Exception as e:
            print(f"设置用户表时出错: {e}")
            conn.rollback()
            raise
    
    @staticmethod
    def authenticate_user(conn, username, password):
        """验证用户凭据"""
        cursor = conn.cursor()
        
        try:
            # PostgreSQL
            cursor.execute("""
            SELECT password_hash, salt, is_admin FROM users 
            WHERE username = %s
            """, (username,))
            
            result = cursor.fetchone()
            
            if not result:
                print(f"用户 {username} 不存在")
                return False, False  # 用户不存在
            
            stored_hash, stored_salt, is_admin = result
            print(f"数据库中的信息: 哈希: {stored_hash[:10]}..., 盐值: {stored_salt}, 管理员: {is_admin}")
            
            # 验证密码
            if UserAuth.verify_password(stored_hash, stored_salt, password):
                print(f"密码验证成功")
                # 返回认证成功和管理员状态
                return True, bool(is_admin)
            else:
                print(f"密码验证失败")
                return False, False  # 密码错误
                
        except Exception as e:
            print(f"认证过程中出错: {e}")
            import traceback
            traceback.print_exc()
            return False, False
    
    def get_users_list(self, limit=20, offset=0):
        """获取用户列表（支持分页）"""
        conn = self.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            # 检查是否有last_login列
            cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'users' AND column_name = 'last_login'
            """)
            has_last_login = cursor.fetchone() is not None
            
            cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'users' AND column_name = 'avatar_url'
            """)
            has_avatar = cursor.fetchone() is not None

            cols = "id, username, is_admin, created_at"
            if has_last_login:
                cols += ", last_login"
            if has_avatar:
                cols += ", avatar_url"

            cursor.execute(f"""
                SELECT {cols}
                FROM users
                ORDER BY id ASC
                LIMIT %s OFFSET %s
            """, (limit, offset))
            
            results = cursor.fetchall()
            users = []
            
            for result in results:
                idx = 4
                last_login = None
                avatar_url = None
                if has_last_login:
                    last_login = result[idx]; idx += 1
                if has_avatar:
                    avatar_url = result[idx]
                users.append({
                    'id': result[0],
                    'username': result[1],
                    'is_admin': bool(result[2]),
                    'created_at': result[3],
                    'last_login': last_login,
                    'avatar_url': avatar_url
                })
            
            return users
        except Exception as e:
            print(f"获取用户列表时出错: {e}")
            return []
        finally:
            conn.close()
    
    def get_user_by_id(self, user_id: int):
        """根据ID获取用户信息"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            # 检查是否有last_login列
            cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'users' AND column_name = 'last_login'
            """)
            has_last_login = cursor.fetchone() is not None
            
            cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'users' AND column_name = 'avatar_url'
            """)
            has_avatar = cursor.fetchone() is not None

            cols = "id, username, is_admin, created_at"
            if has_last_login:
                cols += ", last_login"
            if has_avatar:
                cols += ", avatar_url"

            cursor.execute(f"SELECT {cols} FROM users WHERE id = %s", (user_id,))
            result = cursor.fetchone()
            if not result:
                return None

            idx = 4
            last_login = None
            avatar_url = None
            if has_last_login:
                last_login = result[idx]; idx += 1
            if has_avatar:
                avatar_url = result[idx]

            return {
                'id': result[0],
                'username': result[1],
                'is_admin': bool(result[2]),
                'created_at': result[3],
                'last_login': last_login,
                'avatar_url': avatar_url
            }
        except Exception as e:
            print(f"根据ID获取用户信息时出错: {e}")
            return None
        finally:
            conn.close()
    
    def create_user(self, username: str, password_hash: str, is_admin: bool = False):
        """创建新用户"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            # 对于bcrypt哈希，我们不需要单独的盐值，因为bcrypt已经包含了盐值
            # 为了保持数据库结构兼容性，我们存储一个空字符串作为盐值
            salt = ""
            
            cursor.execute("""
            INSERT INTO users (username, password_hash, salt, is_admin) 
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """, (username, password_hash, salt, is_admin))
            
            user_id = cursor.fetchone()[0]
            conn.commit()
            return user_id
        except Exception as e:
            print(f"创建用户时出错: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()
    
    def update_user(self, user_id: int, **kwargs):
        """更新用户信息"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # 构建UPDATE语句
            update_fields = []
            update_values = []
            
            if 'username' in kwargs:
                update_fields.append("username = %s")
                update_values.append(kwargs['username'])
            
            if 'password_hash' in kwargs:
                update_fields.append("password_hash = %s")
                update_values.append(kwargs['password_hash'])
                # 对于bcrypt哈希，设置空盐值
                if 'salt' not in kwargs:
                    update_fields.append("salt = %s")
                    update_values.append("")
            
            if 'salt' in kwargs:
                update_fields.append("salt = %s")
                update_values.append(kwargs['salt'])
                
            if 'is_admin' in kwargs:
                update_fields.append("is_admin = %s")
                update_values.append(kwargs['is_admin'])
            
            if 'avatar_url' in kwargs:
                update_fields.append("avatar_url = %s")
                update_values.append(kwargs['avatar_url'])
            
            if not update_fields:
                return True  # 没有需要更新的字段
            
            # 添加user_id到values列表
            update_values.append(user_id)
            
            query = f"""
            UPDATE users 
            SET {', '.join(update_fields)}
            WHERE id = %s
            """
            
            cursor.execute(query, update_values)
            affected_rows = cursor.rowcount
            conn.commit()
            
            return affected_rows > 0
        except Exception as e:
            print(f"更新用户时出错: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def delete_user(self, user_id: int):
        """删除用户"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
            affected_rows = cursor.rowcount
            conn.commit()
            
            return affected_rows > 0
        except Exception as e:
            print(f"删除用户时出错: {e}")
            conn.rollback()
            return False
        finally:
            conn.close() 
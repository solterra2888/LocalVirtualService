# -*- coding: utf-8 -*-
"""
数据库管理模块 - 仅支持PostgreSQL
"""

import os
import psycopg2
import json
import time
import contextlib
from config.settings import config, get_app_config_path
from backend.auth.user_auth import UserAuth


class DatabaseManager:
    """数据库管理类 - 仅支持PostgreSQL"""
    
    def __init__(self, db_conn_params=None):
        """
        初始化数据库管理器
        
        Args:
            db_conn_params: 数据库连接参数字典
        """
        self.db_conn_params = db_conn_params or {}
    
    def get_connection(self, max_retries=3, retry_delay=1):
        """获取PostgreSQL数据库连接，支持重试机制"""
        last_error = None
        
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(
                    host=self.db_conn_params.get('db_host', ''),
                    port=self.db_conn_params.get('db_port', '5432'),
                    database=self.db_conn_params.get('db_name', ''),
                    user=self.db_conn_params.get('db_user', ''),
                    password=self.db_conn_params.get('db_password', ''),
                    connect_timeout=15,  # 统一连接超时到15秒
                    options='-c statement_timeout=600000',  # 设置10分钟语句超时
                    keepalives_idle=30,  # 30秒后开始发送keepalive
                    keepalives_interval=5,  # 每5秒发送一次keepalive
                    keepalives_count=6  # 最多发送6次keepalive
                )
                return conn
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    print(f"数据库连接失败，{retry_delay}秒后重试 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                else:
                    print(f"连接PostgreSQL数据库时出错 (所有重试都失败): {str(e)}")
        
        return None
    
    @contextlib.contextmanager
    def get_connection_context(self, max_retries=3, retry_delay=1):
        """提供安全的数据库连接上下文管理器"""
        conn = None
        try:
            conn = self.get_connection(max_retries, retry_delay)
            if conn is None:
                raise Exception("无法建立数据库连接")
            yield conn
            conn.commit()  # 自动提交事务
        except Exception as e:
            if conn:
                try:
                    conn.rollback()  # 发生错误时回滚
                except:
                    pass
            raise e
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def _create_tables(self, conn):
        """创建数据库表"""
        cursor = conn.cursor()
        
        try:
            # PostgreSQL表创建
            # 处理统计表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS processing_stats (
                id SERIAL PRIMARY KEY,
                timestamp TEXT NOT NULL,
                files_processed INTEGER DEFAULT 0,
                files_failed INTEGER DEFAULT 0,
                total_time REAL DEFAULT 0,
                total_file_processing_time REAL DEFAULT 0,
                total_ai_time REAL DEFAULT 0,
                upload_params TEXT DEFAULT '{}',
                total_tokens INTEGER DEFAULT 0,
                total_price REAL DEFAULT 0,
                task_name TEXT DEFAULT 'default',
                "user" TEXT DEFAULT 'guest',
                final_report TEXT DEFAULT '',
                rc_agent_status TEXT DEFAULT 'pending'
            )
            """)
            
            # 文件处理详情表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_processing_details (
                id SERIAL PRIMARY KEY,
                processing_stats_id INTEGER REFERENCES processing_stats(id) ON DELETE CASCADE,
                filename TEXT NOT NULL,
                file_type TEXT,
                file_size_mb REAL DEFAULT 0,
                processing_time REAL DEFAULT 0,
                ai_time REAL DEFAULT 0,
                content_length INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                total_tokens INTEGER DEFAULT 0,
                cost REAL DEFAULT 0,
                content TEXT DEFAULT '',
                error_message TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                task_id TEXT DEFAULT ''
            )
            """)
            
            # 迁移：为现有表添加rc_agent_status字段（保留用于旧数据库升级）
            self._migrate_add_rc_agent_status(cursor)
            
            # 迁移：为file_processing_details表添加original_file_path字段
            self._migrate_add_original_file_path(cursor)
            
            conn.commit()
            print("数据库表创建/验证完成")
        except Exception as e:
            print(f"创建数据库表时出错: {str(e)}")
            conn.rollback()
            raise
    
    def _migrate_add_rc_agent_status(self, cursor):
        """迁移：为processing_stats表添加rc_agent_status字段"""
        try:
            # 检查字段是否已存在
            cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='processing_stats' AND column_name='rc_agent_status'
            """)
            
            if not cursor.fetchone():
                # 字段不存在，添加它
                cursor.execute("""
                ALTER TABLE processing_stats 
                ADD COLUMN rc_agent_status TEXT DEFAULT 'pending'
                """)
                print("已添加rc_agent_status字段到processing_stats表")
        except Exception as e:
            print(f"迁移rc_agent_status字段时出错: {str(e)}")
            # 迁移失败不应该阻止整个表创建过程
    
    def _migrate_add_original_file_path(self, cursor):
        """迁移：为file_processing_details表添加original_file_path字段"""
        try:
            # 检查字段是否已存在
            cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='file_processing_details' AND column_name='original_file_path'
            """)
            
            if not cursor.fetchone():
                # 字段不存在，添加它
                cursor.execute("""
                ALTER TABLE file_processing_details 
                ADD COLUMN original_file_path TEXT DEFAULT ''
                """)
                print("已添加original_file_path字段到file_processing_details表")
        except Exception as e:
            print(f"迁移original_file_path字段时出错: {str(e)}")
            # 迁移失败不应该阻止整个表创建过程
    
    def _migrate_add_primaryticker_to_target_tracking_config(self, cursor):
        """迁移：为target_tracking_config表添加primaryticker字段"""
        try:
            # 检查字段是否已存在
            cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='target_tracking_config' AND column_name='primaryticker'
            """)
            
            if not cursor.fetchone():
                # 字段不存在，添加它
                cursor.execute("""
                ALTER TABLE target_tracking_config 
                ADD COLUMN primaryticker TEXT DEFAULT ''
                """)
                print("已添加primaryticker字段到target_tracking_config表")
        except Exception as e:
            print(f"迁移primaryticker字段时出错: {str(e)}")
            # 迁移失败不应该阻止整个表创建过程
    

    
    def migrate_database_schema(self, conn):
        """迁移数据库架构到最新版本"""
        cursor = conn.cursor()
        
        print("正在检查和迁移数据库架构...")
        
        try:
            # 清理已废弃的 processing_files 表
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'processing_files'
            )
            """)
            processing_files_exists = cursor.fetchone()[0]
            
            if processing_files_exists:
                print("检测到废弃的 processing_files 表，正在删除...")
                cursor.execute("DROP TABLE IF EXISTS processing_files CASCADE")
                print("已删除废弃的 processing_files 表")
            
            # 检查processing_stats表的列
            cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'processing_stats'
            """)
            existing_columns = [row[0] for row in cursor.fetchall()]
            
            # 需要添加的列（已更新为新字段名）
            columns_to_add = [
                ('total_file_processing_time', 'REAL DEFAULT 0'),
                ('total_ai_time', 'REAL DEFAULT 0'),
                ('total_tokens', 'INTEGER DEFAULT 0'),
                ('total_price', 'REAL DEFAULT 0'),
                ('task_name', 'TEXT DEFAULT \'default\''),
                ('"user"', 'TEXT DEFAULT \'guest\''),  # user是PostgreSQL关键字，需要引号
                ('log_content', 'TEXT DEFAULT \'\''),
                ('final_report', 'TEXT DEFAULT \'\'')
            ]
            
            # 添加缺失的列
            for column_name, column_def in columns_to_add:
                clean_column_name = column_name.strip('"')
                if clean_column_name not in existing_columns:
                    try:
                        cursor.execute(f"ALTER TABLE processing_stats ADD COLUMN {column_name} {column_def}")
                        print(f"已添加列: processing_stats.{column_name}")
                    except Exception as e:
                        print(f"添加列 {column_name} 时出错: {e}")
            
            # 清理未使用的冗余字段
            columns_to_remove = ['final_report_content']
            
            for column_name in columns_to_remove:
                if column_name in existing_columns:
                    try:
                        cursor.execute(f"ALTER TABLE processing_stats DROP COLUMN IF EXISTS {column_name}")
                        print(f"已删除未使用的冗余列: processing_stats.{column_name}")
                    except Exception as e:
                        print(f"删除列 {column_name} 时出错: {e}")
            
            # 检查是否存在file_processing_details表，如果不存在则创建
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'file_processing_details'
            )
            """)
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                print("创建file_processing_details表...")
                cursor.execute("""
                CREATE TABLE file_processing_details (
                    id SERIAL PRIMARY KEY,
                    processing_stats_id INTEGER REFERENCES processing_stats(id) ON DELETE CASCADE,
                    filename TEXT NOT NULL,
                    file_type TEXT,
                    file_size_mb REAL DEFAULT 0,
                    processing_time REAL DEFAULT 0,
                    ai_time REAL DEFAULT 0,
                    content_length INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    total_tokens INTEGER DEFAULT 0,
                    cost REAL DEFAULT 0,
                    content TEXT DEFAULT '',
                    error_message TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    task_id TEXT DEFAULT ''
                )
                """)
                print("file_processing_details表创建成功")
            
            conn.commit()
            print("数据库架构迁移完成")
        except Exception as e:
            print(f"数据库架构迁移时出错: {str(e)}")
            conn.rollback()
            raise
    
    def test_postgresql_connection(self, host, port, name, user, password):
        """测试PostgreSQL数据库连接"""
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=name,
                user=user,
                password=password,
                connect_timeout=15  # 统一连接超时到15秒
            )
            conn.close()
            return True, "连接成功"
        except Exception as e:
            return False, f"连接失败: {str(e)}"
    
    def ensure_schema(self):
        """确保数据库架构是最新的"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
            
            cursor = conn.cursor()
            
            # 创建agent_credentials表
            # 检查表是否存在
            cursor.execute("SELECT to_regclass('public.agent_credentials')")
            if cursor.fetchone()[0] is None:
                cursor.execute('''
                CREATE TABLE agent_credentials (
                    id SERIAL PRIMARY KEY,
                    agent_name TEXT NOT NULL,
                    agent_token TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    shared BOOLEAN DEFAULT false,
                    is_default BOOLEAN DEFAULT false,
                    input_fields JSONB,
                    remarks TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                ''')
                print("已创建agent_credentials表")
            
            # 确保用户表存在
            UserAuth.setup_users_table(conn)
            
            # 进行架构迁移
            self.migrate_database_schema(conn)
            
            # 检查并添加缺失的remarks列
            self._ensure_remarks_column(conn)
            
            # 检查并添加缺失的agent_type列
            self._ensure_agent_type_column(conn)
            
            # 检查并添加target_tracking_config表的primaryticker列
            self._migrate_add_primaryticker_to_target_tracking_config(conn.cursor())
            
            # 创建调度器相关表
            self._create_scheduler_tables(conn)
            
            # 创建宏观指标相关表
            self._create_macro_indicators_tables(conn)
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"确保数据库架构时出错: {e}")
            if conn:
                conn.rollback()
                conn.close()
            return False
    
    def _ensure_remarks_column(self, conn):
        """确保agent_credentials表存在remarks列"""
        cursor = conn.cursor()
        
        # PostgreSQL检查列是否存在
        cursor.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'agent_credentials' AND column_name = 'remarks'
        """)
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE agent_credentials ADD COLUMN remarks TEXT")
            print("已添加agent_credentials表的remarks列")
    
    def _ensure_agent_type_column(self, conn):
        """确保agent_credentials表存在agent_type列"""
        cursor = conn.cursor()
        
        # PostgreSQL检查列是否存在
        cursor.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'agent_credentials' AND column_name = 'agent_type'
        """)
        if not cursor.fetchone():
            cursor.execute("""
            ALTER TABLE agent_credentials ADD COLUMN agent_type VARCHAR(20) NOT NULL DEFAULT 'chatbot';
            ALTER TABLE agent_credentials ADD CONSTRAINT agent_type_check 
            CHECK (agent_type IN ('chatbot', 'workflow'))
            """)
            print("已添加agent_credentials表的agent_type列")
    
    def _create_scheduler_tables(self, conn):
        """创建调度器相关表"""
        cursor = conn.cursor()
        
        try:
            # 创建定时任务配置表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS task_scheduling (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                type VARCHAR(50) NOT NULL,
                schedule_type VARCHAR(20) NOT NULL,
                cron_expression VARCHAR(100),
                interval_minutes INTEGER,
                timezone VARCHAR(50) NOT NULL DEFAULT 'America/New_York',
                enabled BOOLEAN NOT NULL DEFAULT true,
                params JSONB DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                last_run TIMESTAMP WITH TIME ZONE,
                next_run TIMESTAMP WITH TIME ZONE,
                success_count INTEGER DEFAULT 0,
                fail_count INTEGER DEFAULT 0,
                created_by VARCHAR(100),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT valid_schedule_type CHECK (schedule_type IN ('cron', 'interval')),
                CONSTRAINT valid_task_type CHECK (type IN ('eodhd_news', 'watchlist_news', 'economic_events', 'macro_indicators', 'eodhd_fundamentals', 'finviz_screener')),
                CONSTRAINT cron_or_interval CHECK (
                    (schedule_type = 'cron' AND cron_expression IS NOT NULL) OR
                    (schedule_type = 'interval' AND interval_minutes IS NOT NULL)
                )
            )
            """)
            
            # 更新现有表的约束（如果表已存在）
            self._update_task_scheduling_constraints(cursor)
            
            # 创建任务执行日志表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS task_execution_logs (
                id SERIAL PRIMARY KEY,
                task_id VARCHAR(255) NOT NULL,
                task_name VARCHAR(255) NOT NULL,
                task_type VARCHAR(50) NOT NULL,
                started_at TIMESTAMP WITH TIME ZONE NOT NULL,
                finished_at TIMESTAMP WITH TIME ZONE,
                duration_ms INTEGER,
                status VARCHAR(20) NOT NULL,
                result_message TEXT,
                result_data JSONB,
                error_message TEXT,
                error_traceback TEXT,
                execution_params JSONB,
                
                CONSTRAINT valid_execution_status CHECK (status IN ('running', 'success', 'failed', 'timeout')),
                CONSTRAINT fk_task_execution_logs_task_id FOREIGN KEY (task_id) REFERENCES task_scheduling(id) ON DELETE CASCADE
            )
            """)
            
            # task_default_configs 表已删除，不再需要创建和插入默认配置
            
            print("调度器相关表创建/更新完成")
        except Exception as e:
            print(f"创建调度器表时出错: {str(e)}")
            # 不抛出异常，让系统继续运行
            pass

    def _update_task_scheduling_constraints(self, cursor):
        """更新现有task_scheduling表的约束"""
        try:
            # 检查表是否存在
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'task_scheduling'
            )
            """)
            
            if not cursor.fetchone()[0]:
                return  # 表不存在，无需更新约束
            
            # 检查并添加缺失的约束
            # 1. 检查valid_schedule_type约束
            cursor.execute("""
            SELECT constraint_name FROM information_schema.table_constraints 
            WHERE table_name = 'task_scheduling' AND constraint_name = 'valid_schedule_type'
            """)
            
            if not cursor.fetchone():
                try:
                    cursor.execute("""
                    ALTER TABLE task_scheduling 
                    ADD CONSTRAINT valid_schedule_type CHECK (schedule_type IN ('cron', 'interval'))
                    """)
                    print("已添加valid_schedule_type约束")
                except Exception as e:
                    print(f"添加valid_schedule_type约束时出错: {e}")
            
            # 2. 检查valid_task_type约束
            cursor.execute("""
            SELECT constraint_name FROM information_schema.table_constraints 
            WHERE table_name = 'task_scheduling' AND constraint_name = 'valid_task_type'
            """)
            
            if not cursor.fetchone():
                try:
                    cursor.execute("""
                    ALTER TABLE task_scheduling 
                    ADD CONSTRAINT valid_task_type CHECK (type IN ('eodhd_news', 'economic_events', 'macro_indicators', 'eodhd_fundamentals', 'finviz_screener'))
                    """)
                    print("已添加valid_task_type约束")
                except Exception as e:
                    print(f"添加valid_task_type约束时出错: {e}")
            
            # 3. 检查cron_or_interval约束
            cursor.execute("""
            SELECT constraint_name FROM information_schema.table_constraints 
            WHERE table_name = 'task_scheduling' AND constraint_name = 'cron_or_interval'
            """)
            
            if not cursor.fetchone():
                try:
                    cursor.execute("""
                    ALTER TABLE task_scheduling 
                    ADD CONSTRAINT cron_or_interval CHECK (
                        (schedule_type = 'cron' AND cron_expression IS NOT NULL) OR
                        (schedule_type = 'interval' AND interval_minutes IS NOT NULL)
                    )
                    """)
                    print("已添加cron_or_interval约束")
                except Exception as e:
                    print(f"添加cron_or_interval约束时出错: {e}")
                    
        except Exception as e:
            print(f"更新task_scheduling表约束时出错: {str(e)}")
            # 不抛出异常，让系统继续运行

    def _create_macro_indicators_tables(self, conn):
        """创建宏观指标相关表"""
        cursor = conn.cursor()
        
        try:
            # 创建宏观指标数据表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS macro_indicators (
                id SERIAL PRIMARY KEY,
                country_code VARCHAR(10) NOT NULL,
                country_name VARCHAR(100) NOT NULL,
                indicator VARCHAR(100) NOT NULL,
                indicator_name VARCHAR(200) NOT NULL,
                year INTEGER NOT NULL,
                value NUMERIC,
                date_str VARCHAR(20),
                fetch_timestamp TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE(country_code, indicator, year)
            )
            """)
            
            # 创建宏观指标获取日志表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS macro_indicators_fetch_logs (
                id SERIAL PRIMARY KEY,
                country_code VARCHAR(10) NOT NULL,
                indicator VARCHAR(100) NOT NULL,
                fetch_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) NOT NULL,
                record_count INTEGER DEFAULT 0,
                error_message TEXT,
                duration_seconds NUMERIC DEFAULT 0
            )
            """)
            
            # 创建索引以提高查询性能
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_macro_indicators_country 
            ON macro_indicators(country_code)
            """)
            
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_macro_indicators_indicator 
            ON macro_indicators(indicator)
            """)
            
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_macro_indicators_year 
            ON macro_indicators(year)
            """)
            
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_macro_indicators_country_indicator 
            ON macro_indicators(country_code, indicator)
            """)
            
            print("宏观指标相关表创建/更新完成")
        except Exception as e:
            print(f"创建宏观指标表时出错: {str(e)}")
            # 不抛出异常，让系统继续运行
            pass
    
    def get_agent_credentials(self, username, is_admin=False):
        """获取用户可访问的AGENT凭据"""
        conn = self.get_connection()
        if not conn:
            return []
        
        cursor = conn.cursor()
        
        try:
            # 查询所有可用的agent凭据
            if is_admin:
                # 管理员可以看到所有凭据
                cursor.execute("""
                SELECT id, agent_name, agent_token, is_default, input_fields, remarks, created_by, created_at, 
                       COALESCE(agent_type, 'chatbot') as agent_type
                FROM agent_credentials 
                ORDER BY is_default DESC, created_at ASC
                """)
            else:
                # 普通用户只能看到自己创建的、共享的或默认的
                cursor.execute("""
                SELECT id, agent_name, agent_token, is_default, input_fields, remarks, created_by, created_at,
                       COALESCE(agent_type, 'chatbot') as agent_type
                FROM agent_credentials 
                WHERE created_by = %s 
                OR shared = true 
                OR is_default = true
                ORDER BY is_default DESC, created_at ASC
                """, (username,))
            
            agents = cursor.fetchall()
            conn.close()
            return agents
        except Exception as e:
            print(f"获取AGENT凭据时出错: {str(e)}")
            conn.close()
            return []
    
    def save_agent_credentials(self, username, agents_dict):
        """保存AGENT凭据到数据库"""
        conn = self.get_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        try:
            # 开启事务
            cursor.execute("BEGIN")
            
            # 处理每个RC Agent
            for task_id, task_info in agents_dict.items():
                agent_name = task_info['name']
                agent_token = task_info['token']
                is_default = (task_id == "default")
                input_fields_json = json.dumps(task_info.get('input_fields', []))
                remarks = task_info.get('remarks', '')
                agent_type = task_info.get('agent_type', 'chatbot')
                
                # 检查是否已存在此Agent
                if is_default:
                    cursor.execute("SELECT id FROM agent_credentials WHERE is_default = true")
                else:
                    try:
                        int_task_id = int(task_id)
                        cursor.execute("SELECT id FROM agent_credentials WHERE id = %s", (int_task_id,))
                    except ValueError:
                        cursor.execute("SELECT id FROM agent_credentials WHERE agent_name = %s", (agent_name,))
                
                existing_record = cursor.fetchone()
                
                if existing_record:
                    # 更新现有记录
                    existing_id = existing_record[0]
                    cursor.execute("""
                    UPDATE agent_credentials 
                    SET agent_name = %s, agent_token = %s, shared = %s, 
                        input_fields = %s, is_default = %s, remarks = %s, agent_type = %s
                    WHERE id = %s
                    """, (agent_name, agent_token, True, input_fields_json, 
                          is_default, remarks, agent_type, existing_id))
                else:
                    # 插入新记录
                    cursor.execute("""
                    INSERT INTO agent_credentials 
                    (agent_name, agent_token, created_by, shared, is_default, input_fields, remarks, agent_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (agent_name, agent_token, username, True, is_default, 
                          input_fields_json, remarks, agent_type))
            
            # 提交事务
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"保存AGENT凭据到数据库时出错: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False
    
    def delete_agent_credential(self, task_id, agent_name=None):
        """删除AGENT凭据"""
        conn = self.get_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        try:
            cursor.execute("BEGIN")
            
            # 删除AGENT凭据记录
            affected_rows = 0
            try:
                int_task_id = int(task_id)
                cursor.execute("DELETE FROM agent_credentials WHERE id = %s", (int_task_id,))
                affected_rows = cursor.rowcount
            except ValueError:
                # 如果task_id不是数字，则通过名称删除
                if agent_name:
                    cursor.execute("DELETE FROM agent_credentials WHERE agent_name = %s", (agent_name,))
                    affected_rows = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            # 检查是否真的删除了记录
            if affected_rows > 0:
                print(f"成功删除了 {affected_rows} 条AGENT凭据记录")
                return True
            else:
                print(f"没有找到要删除的AGENT凭据: task_id={task_id}, agent_name={agent_name}")
                return False
                
        except Exception as e:
            print(f"删除AGENT凭据时出错: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False
    
    def execute_query(self, query, params=None):
        """执行SQL查询（INSERT, UPDATE, DELETE等）"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"执行SQL查询时出错: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False
    
    def fetch_one(self, query, params=None):
        """执行查询并获取一行结果"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchone()
            
            # 如果是INSERT/UPDATE/DELETE语句，需要commit
            if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                conn.commit()
            
            conn.close()
            return result
        except Exception as e:
            print(f"获取查询结果时出错: {str(e)}")
            if conn:
                conn.close()
            return None
    
    def fetch_all(self, query, params=None):
        """执行查询并获取所有结果"""
        conn = self.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            conn.close()
            return results
        except Exception as e:
            print(f"获取查询结果时出错: {str(e)}")
            if conn:
                conn.close()
            return [] 
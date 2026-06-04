# -*- coding: utf-8 -*-
"""
数据库管理模块 - 仅支持PostgreSQL
"""

import os
import psycopg2
import json
import time
import contextlib
from config.settings import config
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
    
    def get_connection(self, max_retries=5, retry_delay=2):
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
                    connect_timeout=30,  # 增加连接超时到30秒
                    # 系统级时区契约: 全部 timestamp 列以 UTC 解释 / 写入.
                    # 服务器实际部署在 HK (UTC+8); 不显式 SET TIME ZONE 会导致 NOW()
                    # 默认值与 Python naive datetime 写入 naive 列时相差 8h.
                    # 详见 backend/utils/datetime_utils.py 与 docs/instructions/DAILY_MODULE.md "时区契约" 一节.
                    options='-c statement_timeout=600000 -c TimeZone=UTC',
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
                    retry_delay = min(retry_delay * 1.5, 10)  # 渐进式退避，最大10秒
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
                rc_agent_status TEXT DEFAULT 'pending',
                llm_model VARCHAR(255),
                llm_provider VARCHAR(255)
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
                task_id TEXT DEFAULT '',
                file_storage_type VARCHAR(10) DEFAULT 'local',
                oss_bucket VARCHAR(100)
            )
            """)
            
            # 迁移：为现有表添加rc_agent_status字段（保留用于旧数据库升级）
            self._migrate_add_rc_agent_status(cursor)
            
            # 迁移：为file_processing_details表添加original_file_path字段
            self._migrate_add_original_file_path(cursor)
            
            # 迁移：为file_processing_details表添加asr_with_diarization字段
            self._migrate_add_asr_with_diarization(cursor)
            
            # 迁移：为file_processing_details表添加display_name字段（AI自动命名）
            self._migrate_add_display_name(cursor)
            
            # 创建 file_shares 表（文件分享）
            self._create_file_shares_table(cursor)
            
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
    
    def _migrate_add_asr_with_diarization(self, cursor):
        """迁移：为file_processing_details表添加asr_with_diarization字段（用于存储说话人分离的结构化数据）"""
        try:
            # 检查字段是否已存在
            cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='file_processing_details' AND column_name='asr_with_diarization'
            """)
            
            if not cursor.fetchone():
                # 字段不存在，添加它（使用 JSONB 类型以支持高效查询）
                cursor.execute("""
                ALTER TABLE file_processing_details 
                ADD COLUMN asr_with_diarization JSONB DEFAULT NULL
                """)
                print("已添加asr_with_diarization字段到file_processing_details表")
        except Exception as e:
            print(f"迁移asr_with_diarization字段时出错: {str(e)}")
            # 迁移失败不应该阻止整个表创建过程
    
    def _migrate_add_display_name(self, cursor):
        """迁移：为file_processing_details表添加display_name字段（AI自动命名）"""
        try:
            cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='file_processing_details' AND column_name='display_name'
            """)
            
            if not cursor.fetchone():
                cursor.execute("""
                ALTER TABLE file_processing_details 
                ADD COLUMN display_name TEXT DEFAULT NULL
                """)
                print("已添加display_name字段到file_processing_details表")
        except Exception as e:
            print(f"迁移display_name字段时出错: {str(e)}")

    def _create_file_folders_table(self, cursor):
        """创建 file_folders 表（一级文件夹功能）"""
        try:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_folders (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                username TEXT NOT NULL,
                color VARCHAR(20) DEFAULT '',
                sort_order INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(username, name)
            )
            """)
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_folders_username ON file_folders(username)
            """)
        except Exception as e:
            print(f"创建file_folders表时出错: {str(e)}")

    def _migrate_add_folder_id_to_file_processing_details(self, cursor):
        """迁移：为file_processing_details表添加folder_id字段"""
        try:
            cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='file_processing_details' AND column_name='folder_id'
            """)
            if not cursor.fetchone():
                cursor.execute("""
                ALTER TABLE file_processing_details 
                ADD COLUMN folder_id INTEGER REFERENCES file_folders(id) ON DELETE SET NULL
                """)
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_fpd_folder_id ON file_processing_details(folder_id)
                """)
                print("已添加folder_id字段到file_processing_details表")
        except Exception as e:
            print(f"迁移folder_id字段时出错: {str(e)}")

    def _create_file_shares_table(self, cursor):
        """创建 file_shares 表（文件分享功能）"""
        try:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_shares (
                id SERIAL PRIMARY KEY,
                file_id INTEGER NOT NULL REFERENCES file_processing_details(id) ON DELETE CASCADE,
                shared_by_user_id INTEGER NOT NULL REFERENCES users(id),
                shared_to_user_id INTEGER NOT NULL REFERENCES users(id),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(file_id, shared_to_user_id)
            )
            """)
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_shares_to_user ON file_shares(shared_to_user_id)
            """)
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_shares_file ON file_shares(file_id)
            """)
        except Exception as e:
            print(f"创建file_shares表时出错: {str(e)}")

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
    
    def _migrate_add_sort_order_to_target_tracking_config(self, cursor):
        """迁移：为target_tracking_config表添加sort_order字段"""
        try:
            # 检查表是否存在
            cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'target_tracking_config'
            )
            """)
            if not cursor.fetchone()[0]:
                return
            
            # 检查字段是否已存在
            cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='target_tracking_config' AND column_name='sort_order'
            """)
            
            if not cursor.fetchone():
                cursor.execute("""
                ALTER TABLE target_tracking_config 
                ADD COLUMN sort_order INTEGER
                """)
                print("已添加sort_order字段到target_tracking_config表")
            
            # 使用现有priority/added_at初始化排序值（仅在为空或为0时更新）
            cursor.execute("""
            WITH ranked_watchlist AS (
                SELECT 
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY user_id, COALESCE(group_name, 'default')
                        ORDER BY priority DESC, added_at DESC, id DESC
                    ) AS rn
                FROM target_tracking_config
            )
            UPDATE target_tracking_config AS t
            SET sort_order = ranked_watchlist.rn
            FROM ranked_watchlist
            WHERE ranked_watchlist.id = t.id
              AND (t.sort_order IS NULL OR t.sort_order = 0)
            """)
            
            # 创建索引以提升排序查询效率
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_target_tracking_config_user_group_sort
                ON target_tracking_config (
                    user_id,
                    (COALESCE(group_name, 'default')),
                    sort_order
                )
            """)
        except Exception as e:
            print(f"迁移sort_order字段时出错: {str(e)}")
            # 不阻断整体迁移流程
    

    
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
                ('final_report', 'TEXT DEFAULT \'\''),
                # PR-2: LLM 元数据列（与 fix_processing_stats_llm_columns.sql 对齐）
                ('llm_model', 'VARCHAR(255)'),
                ('llm_provider', 'VARCHAR(255)')
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
                    task_id TEXT DEFAULT '',
                    file_storage_type VARCHAR(10) DEFAULT 'local',
                    oss_bucket VARCHAR(100)
                )
                """)
                print("file_processing_details表创建成功")
            
            # 旧库可能仍为不含 skipped 的 CHECK；调度器并发跳过写入需要该状态
            self._ensure_task_execution_logs_status_constraint(cursor)
            
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

            # Agent / MCP 个人访问令牌 (PAT)
            self._ensure_api_personal_tokens_table(cursor)
            
            # 进行架构迁移
            self.migrate_database_schema(conn)
            
            # 检查并添加缺失的remarks列
            self._ensure_remarks_column(conn)
            
            # 检查并添加缺失的agent_type列
            self._ensure_agent_type_column(conn)
            
            # 检查并添加target_tracking_config表的primaryticker列
            self._migrate_add_primaryticker_to_target_tracking_config(conn.cursor())
            # 检查并添加target_tracking_config表的sort_order列
            self._migrate_add_sort_order_to_target_tracking_config(conn.cursor())
            
            # 创建调度器相关表
            self._create_scheduler_tables(conn)
            
            # 创建宏观指标相关表
            self._create_macro_indicators_tables(conn)
            
            # 创建LLM配置相关表
            self._create_llm_config_tables(conn)
            
            # 创建Futu主题相关表
            self._create_futu_themes_tables(conn)
            
            # 创建市场数据相关表
            self._create_market_data_tables(conn)
            
            # 创建财报日历相关表
            self._create_earnings_calendar_tables(conn)
            
            # 创建 IPO 日历相关表
            self._create_ipo_calendar_tables(conn)
            
            # 创建文件夹相关表 + 迁移 folder_id 字段
            self._create_file_folders_table(conn.cursor())
            self._migrate_add_folder_id_to_file_processing_details(conn.cursor())
            
            # 迁移 display_name 字段（AI自动命名）
            self._migrate_add_display_name(conn.cursor())
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"确保数据库架构时出错: {e}")
            if conn:
                conn.rollback()
                conn.close()
            return False
    
    def _ensure_api_personal_tokens_table(self, cursor):
        """创建 api_personal_tokens 表 (Agent / MCP PAT)."""
        cursor.execute("SELECT to_regclass('public.api_personal_tokens')")
        if cursor.fetchone()[0] is None:
            cursor.execute(
                """
                CREATE TABLE api_personal_tokens (
                    id           BIGSERIAL PRIMARY KEY,
                    user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    name         TEXT NOT NULL,
                    prefix       TEXT NOT NULL,
                    token_hash   TEXT NOT NULL,
                    scopes       JSONB NOT NULL DEFAULT '["daily:read", "reports:read"]'::jsonb,
                    expires_at   TIMESTAMPTZ,
                    last_used_at TIMESTAMPTZ,
                    revoked_at   TIMESTAMPTZ,
                    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_apt_user ON api_personal_tokens(user_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_apt_prefix ON api_personal_tokens(prefix)"
            )
            print("已创建 api_personal_tokens 表")

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
                CONSTRAINT valid_task_type CHECK (type IN ('twitter_subscription', 'youtube_subscription', 'economic_events', 'macro_indicators', 'eodhd_fundamentals', 'watchlist_fundamentals', 'finviz_screener', 'eodhd_stock_prices', 'eodhd_eod_stock_prices', 'market_data', 'earnings_calendar', 'ipo_calendar', 'news_feed', 'twitter_author_summary', 'twitter_daily_report', 'youtube_author_summary', 'theme_report', 'cctv_news', 'eodhd_news', 'watchlist_news')),
                CONSTRAINT cron_or_interval CHECK (
                    (schedule_type = 'cron' AND cron_expression IS NOT NULL) OR
                    (schedule_type = 'interval' AND interval_minutes IS NOT NULL)
                )
            )
            """)
            
            # 更新现有表的约束（如果表已存在）
            self._update_task_scheduling_constraints(cursor)
            
            # 创建图表事件表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS chart_events (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                event_date DATE NOT NULL,
                event_type VARCHAR(50) NOT NULL,
                title VARCHAR(255) NOT NULL,
                description TEXT,
                position VARCHAR(20) DEFAULT 'middle',
                related_url TEXT,
                related_file_id VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT valid_position CHECK (position IN ('top', 'middle', 'bottom')),
                CONSTRAINT valid_event_type CHECK (event_type IN ('news', 'earnings', 'dividend', 'split', 'merger', 'research', 'custom', 'file'))
            )
            """)
            
            # 创建图表事件索引
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_chart_events_user_symbol ON chart_events(user_id, symbol);
            CREATE INDEX IF NOT EXISTS idx_chart_events_date ON chart_events(event_date);
            CREATE INDEX IF NOT EXISTS idx_chart_events_type ON chart_events(event_type);
            CREATE INDEX IF NOT EXISTS idx_chart_events_created_at ON chart_events(created_at);
            """)
            
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
                
                CONSTRAINT valid_execution_status CHECK (status IN ('running', 'success', 'failed', 'timeout', 'skipped', 'cancelled')),
                CONSTRAINT fk_task_execution_logs_task_id FOREIGN KEY (task_id) REFERENCES task_scheduling(id) ON DELETE CASCADE
            )
            """)
            
            self._ensure_task_execution_logs_status_constraint(cursor)
            self._ensure_task_execution_period_lock_column(cursor)
            self._deduplicate_task_scheduling(cursor)
            
            # task_default_configs 表已删除，不再需要创建和插入默认配置
            
            print("调度器相关表创建/更新完成")
        except Exception as e:
            print(f"创建调度器表时出错: {str(e)}")
            # 不抛出异常，让系统继续运行
            pass

    def _ensure_task_execution_period_lock_column(self, cursor):
        """为 task_execution_logs 添加 period_key 列和唯一索引（跨机器分布式锁）。

        历史原因：旧实现用各机器本地 Redis SET NX 做周期锁，无法跨机器协调——
        本地 Mac Mini 和远程 43.99.37.76 的调度器各自拿到「各自的 Redis 锁」，
        同一 cron 周期内都提交了一次 Celery 任务。

        修复：改用 PostgreSQL INSERT ON CONFLICT DO NOTHING 作为真正的分布式锁：
          - period_key = cron fire_time 的 'YYYYMMDDHHMM' / interval bucket 字符串
          - 唯一索引 (task_type, period_key) WHERE period_key IS NOT NULL
          - 第一个执行 INSERT 的调度实例拿到锁（RETURNING id 返回行），
            第二个 INSERT 触发冲突，ON CONFLICT DO NOTHING 返回 0 行 → 跳过。
        """
        try:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'task_execution_logs'
                )
            """)
            if not cursor.fetchone()[0]:
                return

            cursor.execute("""
                ALTER TABLE task_execution_logs
                ADD COLUMN IF NOT EXISTS period_key VARCHAR(30)
            """)

            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_task_execution_period
                ON task_execution_logs (task_type, period_key)
                WHERE period_key IS NOT NULL
            """)
            print("已确保 task_execution_logs.period_key 列和唯一索引存在")
        except Exception as e:
            print(f"确保 task_execution_logs.period_key 时出错: {e}")

    def _ensure_task_execution_logs_status_constraint(self, cursor):
        """已有库可能仍为旧版 CHECK（不含 skipped）；与 scheduler 跳过并发执行时写入的 status 对齐。"""
        try:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'task_execution_logs'
                )
            """)
            if not cursor.fetchone()[0]:
                return
            cursor.execute("ALTER TABLE task_execution_logs DROP CONSTRAINT IF EXISTS valid_execution_status")
            cursor.execute("""
                ALTER TABLE task_execution_logs ADD CONSTRAINT valid_execution_status
                CHECK (status IN ('running', 'success', 'failed', 'timeout', 'skipped', 'cancelled'))
            """)
            print("已确保 task_execution_logs.valid_execution_status 含 skipped")
        except Exception as e:
            print(f"更新 task_execution_logs 状态约束时出错: {e}")

    def _deduplicate_task_scheduling(self, cursor):
        """清理 task_scheduling 中同一 type 的重复行，保留 created_at 最新的一条。

        根本原因：_save_task_config 在 _get_task_by_type 失败（连接抖动等）时会
        INSERT 新行而非 UPDATE，导致同一 task_type 存在多行，每行都会在
        _load_saved_tasks 中注册到 APScheduler，引发同一天多次执行。

        注意：task_execution_logs 有 ON DELETE CASCADE 外键指向 task_scheduling.id，
        直接删除重复行会同时删除历史执行日志。因此先把日志的 task_id 改为保留行的 id，
        再删除多余的 task_scheduling 行。
        """
        try:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'task_scheduling'
                )
            """)
            if not cursor.fetchone()[0]:
                return

            # 查询每个 type 保留的 id（created_at 最新）及需要删除的 id 列表
            cursor.execute("""
                SELECT
                    keep.type,
                    keep.id   AS keep_id,
                    dup.id    AS dup_id
                FROM (
                    SELECT DISTINCT ON (type) id, type
                    FROM task_scheduling
                    ORDER BY type, created_at DESC
                ) keep
                JOIN task_scheduling dup
                    ON dup.type = keep.type AND dup.id <> keep.id
            """)
            rows = cursor.fetchall()
            if not rows:
                print("task_scheduling 无重复任务类型记录")
                return

            for task_type, keep_id, dup_id in rows:
                # 将日志的 task_id 指向保留的记录，避免级联删除丢失历史
                cursor.execute("""
                    UPDATE task_execution_logs
                    SET task_id = %s
                    WHERE task_id = %s
                """, (keep_id, dup_id))
                updated = cursor.rowcount

                cursor.execute("DELETE FROM task_scheduling WHERE id = %s", (dup_id,))
                print(
                    f"已合并重复任务类型 {task_type}：删除 {dup_id}，"
                    f"保留 {keep_id}，迁移 {updated} 条执行日志"
                )
        except Exception as e:
            print(f"清理 task_scheduling 重复记录时出错: {e}")

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
            constraint_exists = cursor.fetchone()
            
            # 获取现有的所有任务类型
            cursor.execute("""
                SELECT DISTINCT type FROM task_scheduling WHERE type IS NOT NULL
            """)
            existing_types = [row[0] for row in cursor.fetchall()]
            
            # 定义完整的任务类型列表
            all_task_types = [
                'twitter_subscription',
                'youtube_subscription',
                'economic_events',
                'macro_indicators',
                'eodhd_fundamentals',
                'watchlist_fundamentals',
                'finviz_screener',
                'eodhd_stock_prices',
                'eodhd_eod_stock_prices',
                'market_data',
                'earnings_calendar',
                'ipo_calendar',
                'news_feed',
                'twitter_author_summary',
                'twitter_daily_report',
                'youtube_author_summary',
                'theme_report',
                # legacy rows in some deployments
                'cctv_news',
                'eodhd_news',
                'watchlist_news',
            ]
            
            # 合并现有类型和新类型（去重）
            merged_types = list(set(existing_types + all_task_types))
            types_str = "', '".join(merged_types)
            
            if not constraint_exists:
                try:
                    cursor.execute(f"""
                    ALTER TABLE task_scheduling 
                    ADD CONSTRAINT valid_task_type CHECK (type IN ('{types_str}'))
                    """)
                    print(f"已添加valid_task_type约束，支持类型: {merged_types}")
                except Exception as e:
                    print(f"添加valid_task_type约束时出错: {e}")
            else:
                # 约束已存在，尝试更新
                try:
                    # 使用savepoint避免影响整个事务
                    cursor.execute("SAVEPOINT before_constraint_update")
                    cursor.execute("""
                    ALTER TABLE task_scheduling DROP CONSTRAINT IF EXISTS valid_task_type
                    """)
                    cursor.execute(f"""
                    ALTER TABLE task_scheduling 
                    ADD CONSTRAINT valid_task_type CHECK (type IN ('{types_str}'))
                    """)
                    cursor.execute("RELEASE SAVEPOINT before_constraint_update")
                    print(f"已更新valid_task_type约束，支持类型: {merged_types}")
                except Exception as e:
                    cursor.execute("ROLLBACK TO SAVEPOINT before_constraint_update")
                    print(f"更新valid_task_type约束时出错: {e}")
                    print(f"将保留原有约束，继续执行")
            
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
    
    def delete_agent_credential(self, task_id, agent_name=None, username=None, is_admin=False):
        """删除AGENT凭据"""
        conn = self.get_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        try:
            cursor.execute("BEGIN")
            
            # 首先检查记录是否存在并获取创建者信息
            try:
                int_task_id = int(task_id)
                cursor.execute("SELECT id, created_by FROM agent_credentials WHERE id = %s", (int_task_id,))
            except ValueError:
                # 如果task_id不是数字，则通过名称查找
                if agent_name:
                    cursor.execute("SELECT id, created_by FROM agent_credentials WHERE agent_name = %s", (agent_name,))
                else:
                    return False
            
            record = cursor.fetchone()
            if not record:
                return False
            
            record_id, created_by = record
            
            # 权限检查：普通用户只能删除自己创建的AI Agent
            if not is_admin and created_by != username:
                return False
            
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
    
    def _create_futu_themes_tables(self, conn):
        """创建Futu投资主题相关表"""
        cursor = conn.cursor()
        
        try:
            # 1. 富途投资主题表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS futu_investment_themes (
                id SERIAL PRIMARY KEY,
                
                -- 基本信息
                theme_id VARCHAR(50) NOT NULL,
                theme_name VARCHAR(200) NOT NULL,
                market VARCHAR(10) NOT NULL,
                theme_url VARCHAR(500),
                
                -- 市场表现
                change_percent VARCHAR(20),
                gainers VARCHAR(20),
                losers VARCHAR(20),
                unchanged VARCHAR(20),
                
                -- 领涨股信息
                top_stock VARCHAR(200),
                top_stock_change VARCHAR(200),
                
                -- 股票数量
                stock_count INTEGER DEFAULT 0,
                
                -- 爬取时间
                scraped_at TIMESTAMP NOT NULL,
                
                -- 元数据
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- 唯一约束：同一主题ID、市场和爬取日期应该是唯一的
                CONSTRAINT unique_theme_market_date UNIQUE(theme_id, market, scraped_at)
            )
            """)
            
            # 2. 富途主题股票表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS futu_theme_stocks (
                id SERIAL PRIMARY KEY,
                
                -- 关联主题
                theme_id INTEGER REFERENCES futu_investment_themes(id) ON DELETE CASCADE,
                
                -- 股票信息
                symbol VARCHAR(50) NOT NULL,
                stock_name VARCHAR(200) NOT NULL,
                stock_url VARCHAR(500),
                market VARCHAR(10) NOT NULL,
                
                -- 元数据
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # 创建索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_futu_themes_market ON futu_investment_themes(market)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_futu_themes_theme_id ON futu_investment_themes(theme_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_futu_themes_scraped_at ON futu_investment_themes(scraped_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_futu_theme_stocks_theme_id ON futu_theme_stocks(theme_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_futu_theme_stocks_symbol ON futu_theme_stocks(symbol)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_futu_theme_stocks_market ON futu_theme_stocks(market)")
            
            # 创建唯一索引确保同一主题ID、市场和日期只能有一条记录
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS unique_theme_market_date_idx ON futu_investment_themes(theme_id, market, DATE(scraped_at))")
            
            print("Futu投资主题相关表创建完成")
        except Exception as e:
            print(f"创建Futu投资主题表时出错: {str(e)}")
            pass
    
    def _create_llm_config_tables(self, conn):
        """创建LLM配置相关表"""
        cursor = conn.cursor()
        
        try:
            # 创建LLM提供商配置表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS llm_provider_configs (
                id SERIAL PRIMARY KEY,
                provider_id VARCHAR(50) NOT NULL UNIQUE,
                provider_name VARCHAR(100) NOT NULL,
                display_name VARCHAR(100) NOT NULL,
                
                -- API配置
                api_key TEXT NOT NULL,
                base_url TEXT NOT NULL,
                
                -- 模型配置
                enabled BOOLEAN DEFAULT false,
                default_model VARCHAR(100),
                available_models JSONB,
                
                -- 参数配置
                max_tokens INTEGER DEFAULT 2000,
                temperature DECIMAL(3,2) DEFAULT 0.7,
                top_p DECIMAL(3,2) DEFAULT 0.8,
                timeout INTEGER DEFAULT 30,
                
                -- 元数据
                description TEXT,
                icon VARCHAR(10),
                
                -- 管理字段
                created_by VARCHAR(255) NOT NULL,
                updated_by VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- 使用统计
                total_requests INTEGER DEFAULT 0,
                total_tokens BIGINT DEFAULT 0,
                total_cost DECIMAL(10,4) DEFAULT 0.0,
                last_used_at TIMESTAMP,
                
                -- 故障转移配置
                is_default_provider BOOLEAN DEFAULT false,
                fallback_priority INTEGER DEFAULT 999,
                
                CONSTRAINT chk_temperature CHECK (temperature >= 0 AND temperature <= 2),
                CONSTRAINT chk_top_p CHECK (top_p >= 0 AND top_p <= 1)
            )
            """)
            
            # 创建索引
            cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_llm_provider_id 
                ON llm_provider_configs(provider_id);
            CREATE INDEX IF NOT EXISTS idx_llm_enabled 
                ON llm_provider_configs(enabled);
            CREATE INDEX IF NOT EXISTS idx_llm_default_provider 
                ON llm_provider_configs(is_default_provider);
            CREATE INDEX IF NOT EXISTS idx_llm_fallback_priority 
                ON llm_provider_configs(fallback_priority);
            """)
            
            # 创建LLM全局配置表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS llm_global_settings (
                id SERIAL PRIMARY KEY,
                setting_key VARCHAR(100) NOT NULL UNIQUE,
                setting_value JSONB NOT NULL,
                description TEXT,
                updated_by VARCHAR(255),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT chk_valid_setting_key CHECK (
                    setting_key IN (
                        'default_provider',
                        'fallback_enabled',
                        'fallback_order',
                        'global_timeout',
                        'global_retry_count',
                        'enable_logging',
                        'enable_monitoring'
                    )
                )
            )
            """)
            
            # 插入默认的全局配置（如果不存在）
            cursor.execute("""
            INSERT INTO llm_global_settings (setting_key, setting_value, description, updated_by)
            VALUES 
                ('default_provider', '"qwen"', '默认LLM提供商', 'system'),
                ('fallback_enabled', 'true', '是否启用故障转移', 'system'),
                ('fallback_order', '["qwen", "doubao", "glm", "claude"]', '故障转移顺序', 'system'),
                ('global_timeout', '120', 'provider 未单独配置时的默认请求超时（秒）', 'system'),
                ('global_retry_count', '2', '全局重试次数（连接/5xx，不含读超时）', 'system'),
                ('enable_logging', 'true', '是否启用详细日志', 'system'),
                ('enable_monitoring', 'true', '是否启用性能监控', 'system')
            ON CONFLICT (setting_key) DO NOTHING
            """)
            
            # 插入默认的提供商配置（如果不存在）
            cursor.execute("""
            INSERT INTO llm_provider_configs (
                provider_id, provider_name, display_name, api_key, base_url,
                enabled, default_model, available_models, max_tokens, temperature,
                top_p, timeout, description, icon, created_by, updated_by,
                is_default_provider, fallback_priority
            ) VALUES 
                ('qwen', 'qwen', 'Qwen', '', 'https://dashscope.aliyuncs.com',
                 false, 'qwen-turbo', '["qwen-turbo", "qwen-plus", "qwen-max", "qwen-max-longcontext", "qwen3-turbo", "qwen3-plus", "qwen3-max"]',
                 2000, 0.7, 0.8, 30, 'Alibaba Cloud Qwen large language model with multiple specifications', '🔷',
                 'system', 'system', true, 1),
                ('doubao', 'doubao', 'Doubao', '', 'https://ark.cn-beijing.volces.com',
                 false, 'doubao-seed-1-6-250615', '["doubao-seed-1-6-250615"]',
                 4000, 0.7, 0.9, 30, 'ByteDance Doubao large language model with strong Chinese understanding', '🫘',
                 'system', 'system', false, 2),
                ('glm', 'glm', 'GLM', '', 'https://open.bigmodel.cn',
                 false, 'glm-4.6', '["glm-4.6", "glm-4", "glm-4-flash"]',
                 8000, 0.7, 0.8, 30, 'Tsinghua Zhipu AI large language model', '🧠',
                 'system', 'system', false, 3),
                ('claude', 'claude', 'Claude', '', 'https://open.cherryin.net',
                 false, 'anthropic/claude-sonnet-4.6',
                 '["anthropic/claude-sonnet-4.6","anthropic/claude-opus-4.7","anthropic/claude-opus-4-20250514","anthropic/claude-sonnet-4-20250514","anthropic/claude-3-5-sonnet-20241022","anthropic/claude-3-5-haiku-20241022"]',
                 4096, 0.7, 0.9, 120, 'Anthropic Claude via CherryIN (open.cherryin.net) - OpenAI-compatible gateway, billed in USD', '🤖',
                 'system', 'system', false, 4)
            ON CONFLICT (provider_id) DO NOTHING
            """)
            
            print("✅ LLM配置相关表创建/更新完成")
            print("   - 已插入默认的4个提供商配置（API密钥为空，需要手动配置）")
        except Exception as e:
            print(f"❌ 创建LLM配置表时出错: {str(e)}")
            # 不抛出异常，让系统继续运行
            pass
    
    # LLM配置相关的数据库操作方法
    
    def get_llm_provider_configs(self, enabled_only=False):
        """获取LLM提供商配置列表"""
        conn = self.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            if enabled_only:
                cursor.execute("""
                SELECT id, provider_id, provider_name, display_name, api_key, 
                       base_url, enabled, default_model, available_models,
                       max_tokens, temperature, top_p, timeout, description, icon,
                       created_by, updated_by, created_at, updated_at,
                       total_requests, total_tokens, total_cost, last_used_at,
                       is_default_provider, fallback_priority
                FROM llm_provider_configs
                WHERE enabled = true
                ORDER BY fallback_priority ASC, provider_id ASC
                """)
            else:
                cursor.execute("""
                SELECT id, provider_id, provider_name, display_name, api_key, 
                       base_url, enabled, default_model, available_models,
                       max_tokens, temperature, top_p, timeout, description, icon,
                       created_by, updated_by, created_at, updated_at,
                       total_requests, total_tokens, total_cost, last_used_at,
                       is_default_provider, fallback_priority
                FROM llm_provider_configs
                ORDER BY fallback_priority ASC, provider_id ASC
                """)
            
            results = cursor.fetchall()
            conn.close()
            return results
        except Exception as e:
            print(f"获取LLM提供商配置失败: {str(e)}")
            if conn:
                conn.close()
            return []
    
    def get_llm_provider_config(self, provider_id):
        """获取单个LLM提供商配置"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT id, provider_id, provider_name, display_name, api_key, 
                   base_url, enabled, default_model, available_models,
                   max_tokens, temperature, top_p, timeout, description, icon,
                   created_by, updated_by, created_at, updated_at,
                   total_requests, total_tokens, total_cost, last_used_at,
                   is_default_provider, fallback_priority
            FROM llm_provider_configs
            WHERE provider_id = %s
            """, (provider_id,))
            
            result = cursor.fetchone()
            conn.close()
            return result
        except Exception as e:
            print(f"获取LLM提供商配置失败: {str(e)}")
            if conn:
                conn.close()
            return None
    
    def save_llm_provider_config(self, config_data, username):
        """保存或更新LLM提供商配置"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # 检查是否已存在
            cursor.execute(
                "SELECT id FROM llm_provider_configs WHERE provider_id = %s",
                (config_data['provider_id'],)
            )
            existing = cursor.fetchone()
            
            if existing:
                # 更新现有配置
                cursor.execute("""
                UPDATE llm_provider_configs
                SET provider_name = %s, display_name = %s, api_key = %s,
                    base_url = %s, enabled = %s, default_model = %s,
                    available_models = %s, max_tokens = %s, temperature = %s,
                    top_p = %s, timeout = %s, description = %s, icon = %s,
                    updated_by = %s, updated_at = CURRENT_TIMESTAMP,
                    is_default_provider = %s, fallback_priority = %s
                WHERE provider_id = %s
                """, (
                    config_data['provider_name'],
                    config_data['display_name'],
                    config_data['api_key'],
                    config_data['base_url'],
                    config_data.get('enabled', False),
                    config_data.get('default_model'),
                    config_data.get('available_models'),
                    config_data.get('max_tokens', 2000),
                    config_data.get('temperature', 0.7),
                    config_data.get('top_p', 0.8),
                    config_data.get('timeout', 30),
                    config_data.get('description'),
                    config_data.get('icon'),
                    username,
                    config_data.get('is_default_provider', False),
                    config_data.get('fallback_priority', 999),
                    config_data['provider_id']
                ))
            else:
                # 插入新配置
                cursor.execute("""
                INSERT INTO llm_provider_configs (
                    provider_id, provider_name, display_name, api_key,
                    base_url, enabled, default_model, available_models,
                    max_tokens, temperature, top_p, timeout,
                    description, icon, created_by, updated_by,
                    is_default_provider, fallback_priority
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    config_data['provider_id'],
                    config_data['provider_name'],
                    config_data['display_name'],
                    config_data['api_key'],
                    config_data['base_url'],
                    config_data.get('enabled', False),
                    config_data.get('default_model'),
                    config_data.get('available_models'),
                    config_data.get('max_tokens', 2000),
                    config_data.get('temperature', 0.7),
                    config_data.get('top_p', 0.8),
                    config_data.get('timeout', 30),
                    config_data.get('description'),
                    config_data.get('icon'),
                    username,
                    username,
                    config_data.get('is_default_provider', False),
                    config_data.get('fallback_priority', 999)
                ))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"保存LLM提供商配置失败: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False
    
    def get_llm_global_settings(self):
        """获取LLM全局配置"""
        conn = self.get_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT setting_key, setting_value, description, updated_by, updated_at
            FROM llm_global_settings
            ORDER BY setting_key
            """)
            
            results = cursor.fetchall()
            conn.close()
            
            # 转换为字典格式
            settings = {}
            for row in results:
                settings[row[0]] = {
                    'value': row[1],
                    'description': row[2],
                    'updated_by': row[3],
                    'updated_at': row[4]
                }
            return settings
        except Exception as e:
            print(f"获取LLM全局配置失败: {str(e)}")
            if conn:
                conn.close()
            return {}
    
    def update_llm_global_setting(self, setting_key, setting_value, username):
        """更新LLM全局配置"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
            UPDATE llm_global_settings
            SET setting_value = %s, updated_by = %s, updated_at = CURRENT_TIMESTAMP
            WHERE setting_key = %s
            """, (setting_value, username, setting_key))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"更新LLM全局配置失败: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return False
    
    def _create_market_data_tables(self, conn):
        """创建市场数据相关表"""
        cursor = conn.cursor()
        
        try:
            # 读取并执行SQL迁移脚本
            migration_file = os.path.join(
                os.path.dirname(__file__),
                'migrations/create_market_data_tables.sql'
            )
            
            if os.path.exists(migration_file):
                with open(migration_file, 'r', encoding='utf-8') as f:
                    sql = f.read()
                cursor.execute(sql)
                print("✅ 市场数据相关表创建/更新完成")
            else:
                print(f"⚠️ 市场数据迁移脚本不存在: {migration_file}")
        except Exception as e:
            print(f"❌ 创建市场数据表时出错: {str(e)}")
            # 不抛出异常，让系统继续运行
            pass
    
    def _create_earnings_calendar_tables(self, conn):
        """创建财报日历相关表"""
        cursor = conn.cursor()
        
        try:
            # 读取并执行SQL迁移脚本
            migration_file = os.path.join(
                os.path.dirname(__file__),
                'migrations/create_earnings_calendar_tables.sql'
            )
            
            if os.path.exists(migration_file):
                with open(migration_file, 'r', encoding='utf-8') as f:
                    sql = f.read()
                cursor.execute(sql)
                print("✅ 财报日历相关表创建/更新完成")
            # 脚本不存在时静默跳过，不打印警告
        except Exception as e:
            print(f"❌ 创建财报日历表时出错: {str(e)}")
            # 不抛出异常，让系统继续运行
            pass 
    
    def _create_ipo_calendar_tables(self, conn):
        """创建 IPO 日历相关表"""
        cursor = conn.cursor()
        
        try:
            # 读取并执行SQL迁移脚本
            migration_file = os.path.join(
                os.path.dirname(__file__),
                'migrations/create_ipo_calendar_tables.sql'
            )
            
            if os.path.exists(migration_file):
                with open(migration_file, 'r', encoding='utf-8') as f:
                    sql = f.read()
                cursor.execute(sql)
                print("✅ IPO 日历相关表创建/更新完成")
            # 脚本不存在时静默跳过，不打印警告
        except Exception as e:
            print(f"❌ 创建 IPO 日历表时出错: {str(e)}")
            # 不抛出异常，让系统继续运行
            pass
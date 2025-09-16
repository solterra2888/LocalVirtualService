#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
混合部署启动脚本
本地：前端(3000) + 主Web应用(8000)
远程：调度器服务(8001) + 数据库(5432)
"""

import os
import sys
import subprocess
import webbrowser
import time
import threading
import signal
import requests
from pathlib import Path

def check_python_version():
    """检查Python版本"""
    version = sys.version_info
    if version.major == 3 and version.minor >= 8:
        print(f"✅ Python版本: {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print(f"❌ Python版本过低: {version.major}.{version.minor}.{version.micro}")
        print("   需要Python 3.8或更高版本")
        return False

def check_node_installed():
    """检查Node.js是否安装"""
    try:
        result = subprocess.run(['node', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            version = result.stdout.strip()
            print(f"✅ Node.js版本: {version}")
            return True
        else:
            return False
    except FileNotFoundError:
        return False

def check_remote_services():
    """检查远程服务状态"""
    print("🔍 检查远程服务状态...")
    
    services = {
        "数据库": "111.230.48.119:5432",
        "调度器服务": "http://111.230.48.119:8001/api/health"
    }
    
    # 检查数据库连接
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex(('111.230.48.119', 5432))
        sock.close()
        
        if result == 0:
            print("✅ 远程数据库连接正常")
            db_ok = True
        else:
            print("❌ 远程数据库连接失败")
            db_ok = False
    except Exception as e:
        print(f"❌ 数据库连接检查失败: {e}")
        db_ok = False
    
    # 检查调度器服务
    try:
        response = requests.get("http://111.230.48.119:8001/api/health", timeout=5)
        if response.status_code == 200:
            print("✅ 远程调度器服务正常")
            scheduler_ok = True
        else:
            print(f"❌ 远程调度器服务异常: HTTP {response.status_code}")
            scheduler_ok = False
    except Exception as e:
        print(f"❌ 调度器服务连接失败: {e}")
        scheduler_ok = False
    
    return db_ok, scheduler_ok

def install_python_dependencies():
    """安装Python依赖"""
    print("📦 正在安装Python依赖...")
    try:
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", "-r", "requirements.txt"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Python依赖安装完成")
            return True
        else:
            print(f"❌ Python依赖安装失败: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ 安装Python依赖时出错: {e}")
        return False

def install_node_dependencies():
    """安装Node.js依赖"""
    frontend_dir = Path(__file__).parent / "web-frontend"
    
    if not frontend_dir.exists():
        print("❌ 前端目录不存在")
        return False
    
    print("📦 正在安装前端依赖...")
    print("   这可能需要几分钟时间...")
    
    try:
        result = subprocess.run(
            ['npm', 'install'], 
            cwd=frontend_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("✅ 前端依赖安装完成")
            return True
        else:
            print(f"❌ 前端依赖安装失败: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ 安装前端依赖时出错: {e}")
        return False

def start_main_web_app():
    """启动主Web应用服务器 (连接远程数据库和调度器)"""
    print("🌐 正在启动主Web应用...")
    
    # 设置环境变量，确保连接到远程服务
    env = {
        **os.environ,
        'PYTHONPATH': str(Path(__file__).parent),
        'DB_HOST': '111.230.48.119',
        'SCHEDULER_SERVICE_URL': 'http://111.230.48.119:8001',
        'DEPLOYMENT_MODE': 'hybrid'
    }
    
    try:
        process = subprocess.Popen([
            sys.executable, "-m", "uvicorn", 
            "backend.main:app", 
            "--host", "0.0.0.0", 
            "--port", "8000",
            "--reload"
        ], env=env)
        
        print("✅ 主Web应用启动成功!")
        print("🌐 主应用地址: http://localhost:8000")
        print("📖 API文档: http://localhost:8000/api/docs")
        print("🔗 连接到远程数据库: 111.230.48.119:5432")
        print("🔗 连接到远程调度器: 111.230.48.119:8001")
        
        return process
    except Exception as e:
        print(f"❌ 主Web应用启动失败: {e}")
        return None

def start_frontend():
    """启动前端服务器"""
    frontend_dir = Path(__file__).parent / "web-frontend"
    
    print("🎨 正在启动前端开发服务器...")
    
    # 设置前端环境变量
    env = {
        **os.environ,
        'BROWSER': 'none',  # 禁止自动打开浏览器
        'REACT_APP_API_BASE_URL': 'http://localhost:8000/api',
        'REACT_APP_SCHEDULER_BASE_URL': 'http://111.230.48.119:8001'
    }
    
    try:
        process = subprocess.Popen([
            'npm', 'start'
        ], cwd=frontend_dir, env=env)
        
        print("✅ 前端服务器启动成功!")
        print("🌐 前端地址: http://localhost:3000")
        print("🔗 API连接: http://localhost:8000/api")
        
        return process
    except Exception as e:
        print(f"❌ 前端启动失败: {e}")
        return None

def wait_for_service(service_name: str, url: str, max_attempts: int = 30) -> bool:
    """等待服务启动"""
    print(f"⏳ 等待{service_name}启动...")
    for i in range(max_attempts):
        try:
            response = requests.get(url, timeout=1)
            if response.status_code == 200:
                print(f"✅ {service_name}已就绪")
                return True
        except:
            pass
        time.sleep(1)
    
    print(f"⚠️ {service_name}启动超时")
    return False

def check_hybrid_services_health():
    """检查混合部署服务健康状态"""
    print("\n🔍 混合部署服务健康检查:")
    
    services = {
        "本地主Web应用": "http://localhost:8000/api/health",
        "本地前端应用": "http://localhost:3000",
        "远程调度器服务": "http://111.230.48.119:8001/api/health",
        "调度器代理通信": "http://localhost:8000/api/scheduler/health"
    }
    
    all_healthy = True
    
    for service_name, url in services.items():
        try:
            response = requests.get(url, timeout=3)
            if response.status_code == 200:
                print(f"   ✅ {service_name}: 正常")
            else:
                print(f"   ⚠️ {service_name}: HTTP {response.status_code}")
                all_healthy = False
        except Exception as e:
            print(f"   ❌ {service_name}: 连接失败")
            all_healthy = False
    
    return all_healthy

def open_browser():
    """打开浏览器"""
    try:
        webbrowser.open("http://localhost:3000")
        print("🌐 已自动打开浏览器")
    except:
        print("⚠️ 无法自动打开浏览器，请手动访问: http://localhost:3000")

def terminate_processes(processes):
    """终止所有进程"""
    print("\n🔄 正在停止本地服务...")
    
    for name, process in processes.items():
        if process:
            try:
                process.terminate()
                # 等待进程结束
                try:
                    process.wait(timeout=5)
                    print(f"✅ {name}已停止")
                except subprocess.TimeoutExpired:
                    # 强制结束
                    process.kill()
                    process.wait()
                    print(f"🔫 {name}被强制停止")
            except Exception as e:
                print(f"⚠️ 停止{name}时出错: {e}")

def main():
    """主函数"""
    print("=" * 70)
    print("🚀 RC Project - 混合部署启动器 V1.0.0")
    print("=" * 70)
    print("📌 混合部署架构:")
    print("   本地服务:")
    print("   - 前端React应用 (端口3000)")
    print("   - 主Web应用服务器 (端口8000)")
    print()
    print("   远程服务 (111.230.48.119):")
    print("   - 调度器服务 (端口8001)")
    print("   - PostgreSQL数据库 (端口5432)")
    print()
    print("💡 优势:")
    print("   - 前端和API在本地，开发调试方便")
    print("   - 调度器和数据库在远程，性能稳定")
    print("   - 真实测试分离式架构")
    print()
    
    # 环境检查
    print("🔍 环境检查...")
    
    if not check_python_version():
        return
    
    if not check_node_installed():
        print("❌ 未检测到Node.js")
        print("💡 请先安装Node.js:")
        print("   - 访问 https://nodejs.org/")
        print("   - 下载并安装最新的LTS版本")
        return
    
    # 检查远程服务
    db_ok, scheduler_ok = check_remote_services()
    if not db_ok:
        print("⚠️ 远程数据库连接异常，但会继续启动本地服务")
    if not scheduler_ok:
        print("⚠️ 远程调度器服务异常，调度功能可能不可用")
    
    print()
    
    # 依赖检查和安装
    print("📦 依赖检查...")
    
    # 检查Python依赖
    try:
        import fastapi, uvicorn, pydantic, httpx
        print("✅ Python依赖已安装")
    except ImportError:
        print("📦 需要安装Python依赖...")
        if not install_python_dependencies():
            return
    
    # 检查Node.js依赖
    frontend_dir = Path(__file__).parent / "web-frontend"
    node_modules = frontend_dir / "node_modules"
    
    if not node_modules.exists():
        print("📦 需要安装前端依赖...")
        if not install_node_dependencies():
            return
    else:
        print("✅ 前端依赖已安装")
    
    print()
    
    # 启动本地服务
    print("🚀 启动本地服务...")
    
    processes = {}
    
    try:
        # 1. 启动主Web应用
        webapp_process = start_main_web_app()
        if not webapp_process:
            print("❌ 主Web应用启动失败，退出")
            return
        processes["主Web应用"] = webapp_process
        
        # 等待主应用启动
        webapp_ready = wait_for_service("主Web应用", "http://localhost:8000/api/health")
        
        # 2. 启动前端
        frontend_process = start_frontend()
        if not frontend_process:
            terminate_processes(processes)
            return
        processes["前端应用"] = frontend_process
        
        # 等待前端启动
        frontend_ready = wait_for_service("前端应用", "http://localhost:3000", max_attempts=60)
        
        print()
        print("=" * 70)
        print("🎉 混合部署启动成功!")
        print("=" * 70)
        print("🌐 本地服务地址:")
        print("   - 前端应用:     http://localhost:3000")
        print("   - 主Web应用:    http://localhost:8000")
        print("   - API文档:      http://localhost:8000/api/docs")
        print()
        print("🌐 远程服务地址:")
        print("   - 调度器服务:   http://111.230.48.119:8001")
        print("   - 数据库:       111.230.48.119:5432")
        print()
        print("🔧 架构说明:")
        print("   - 前端和API在本地运行，便于开发调试")
        print("   - 调度器和数据库在远程运行，模拟生产环境")
        print("   - 主应用通过代理API与远程调度器通信")
        print()
        print("⌨️ 操作说明:")
        print("   - 按 Ctrl+C 停止本地服务")
        print("   - 远程服务需要单独管理")
        print("=" * 70)
        
        # 健康检查
        time.sleep(3)
        check_hybrid_services_health()
        
        # 自动打开浏览器
        if frontend_ready:
            time.sleep(2)
            open_browser()
        
        # 设置信号处理
        def signal_handler(signum, frame):
            terminate_processes(processes)
            print("\n👋 本地服务已停止，感谢使用RC Project混合部署!")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # 等待用户中断
        try:
            while True:
                time.sleep(1)
                # 可以在这里添加定期健康检查
        except KeyboardInterrupt:
            pass
            
    except KeyboardInterrupt:
        pass
    finally:
        terminate_processes(processes)
        print("👋 本地服务已停止，感谢使用RC Project混合部署!")

if __name__ == "__main__":
    main()

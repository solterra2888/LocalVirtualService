#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全栈应用一键启动脚本
同时启动后端API服务器和前端React开发服务器
"""

import os
import sys
import subprocess
import webbrowser
import time
import threading
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

def start_backend():
    """启动后端服务器"""
    print("🚀 正在启动后端API服务器...")
    
    try:
        process = subprocess.Popen([
            sys.executable, "-m", "uvicorn", 
            "backend.main:app", 
            "--host", "0.0.0.0", 
            "--port", "8000",
            "--reload"
        ], 
        env={**os.environ, 'PYTHONPATH': str(Path(__file__).parent)}
        )
        
        print("✅ 后端服务器启动成功!")
        print("🌐 API地址: http://localhost:8000")
        print("📖 API文档: http://localhost:8000/api/docs")
        
        return process
    except Exception as e:
        print(f"❌ 后端启动失败: {e}")
        return None

def start_frontend():
    """启动前端服务器"""
    frontend_dir = Path(__file__).parent / "web-frontend"
    
    print("🎨 正在启动前端开发服务器...")
    
    try:
        process = subprocess.Popen([
            'npm', 'start'
        ],
        cwd=frontend_dir,
        env={**os.environ, 'BROWSER': 'none'}  # 禁止自动打开浏览器
        )
        
        print("✅ 前端服务器启动成功!")
        print("🌐 前端地址: http://localhost:3000")
        
        return process
    except Exception as e:
        print(f"❌ 前端启动失败: {e}")
        return None

def wait_for_backend():
    """等待后端服务器启动"""
    import requests
    
    print("⏳ 等待后端服务器启动...")
    for i in range(30):  # 最多等待30秒
        try:
            response = requests.get("http://localhost:8000/api/health", timeout=1)
            if response.status_code == 200:
                print("✅ 后端服务器已就绪")
                return True
        except:
            pass
        time.sleep(1)
    
    print("⚠️  后端服务器启动超时")
    return False

def wait_for_frontend():
    """等待前端服务器启动"""
    import requests
    
    print("⏳ 等待前端服务器启动...")
    for i in range(60):  # 最多等待60秒
        try:
            response = requests.get("http://localhost:3000", timeout=1)
            if response.status_code == 200:
                print("✅ 前端服务器已就绪")
                return True
        except:
            pass
        time.sleep(1)
    
    print("⚠️  前端服务器启动超时")
    return False

def open_browser():
    """打开浏览器"""
    try:
        webbrowser.open("http://localhost:3000")
        print("🌐 已自动打开浏览器")
    except:
        print("⚠️  无法自动打开浏览器，请手动访问: http://localhost:3000")

def main():
    """主函数"""
    print("=" * 70)
    print("🚀 Universal Glance V1.0.0- 全栈应用启动器")
    print("=" * 70)
    print("📌 这将启动完整的Web应用程序:")
    print("   - 后端API服务器 (FastAPI)")
    print("   - 前端React应用")
    print("   - 自动打开浏览器")
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
    
    print()
    
    # 依赖检查和安装
    print("📦 依赖检查...")
    
    # 检查Python依赖
    try:
        import fastapi, uvicorn, pydantic
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
    
    # 启动服务
    print("🚀 启动服务...")
    
    # 启动后端
    backend_process = start_backend()
    if not backend_process:
        return
    
    # 等待后端启动
    backend_ready = wait_for_backend()
    
    # 启动前端
    frontend_process = start_frontend()
    if not frontend_process:
        backend_process.terminate()
        return
    
    # 等待前端启动
    frontend_ready = wait_for_frontend()
    
    print()
    print("=" * 70)
    print("🎉 应用启动成功!")
    print("=" * 70)
    print("🌐 访问地址:")
    print("   - 前端应用: http://localhost:3000")
    print("   - 后端API:  http://localhost:8000")
    print("   - API文档:  http://localhost:8000/api/docs")
    print()
    print()
    print("⌨️  操作说明:")
    print("   - 按 Ctrl+C 停止所有服务")
    print("   - 查看日志输出了解运行状态")
    print("=" * 70)
    
    # 自动打开浏览器
    if frontend_ready:
        time.sleep(2)
        open_browser()
    
    try:
        # 等待用户中断
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🔄 正在停止服务...")
        
        if frontend_process:
            frontend_process.terminate()
            print("✅ 前端服务器已停止")
        
        if backend_process:
            backend_process.terminate()
            print("✅ 后端服务器已停止")
        
        print("👋 感谢使用Universal Glance!")

if __name__ == "__main__":
    main() 
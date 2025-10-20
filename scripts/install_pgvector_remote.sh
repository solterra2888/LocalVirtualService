#!/bin/bash
# 远程PostgreSQL服务器pgvector安装脚本
# 使用方法: bash install_pgvector_remote.sh

echo "=========================================="
echo "PostgreSQL pgvector 远程安装脚本"
echo "=========================================="

# 检查系统类型
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$NAME
    VER=$VERSION_ID
else
    echo "无法检测操作系统类型"
    exit 1
fi

echo "检测到操作系统: $OS $VER"

# 检查PostgreSQL版本
PG_VERSION=$(psql --version | grep -oE '[0-9]+\.[0-9]+' | head -1)
echo "PostgreSQL版本: $PG_VERSION"

# 根据操作系统安装pgvector
if [[ "$OS" == *"Ubuntu"* ]] || [[ "$OS" == *"Debian"* ]]; then
    echo "使用Ubuntu/Debian安装方法..."
    
    # 更新包管理器
    apt update
    
    # 检查PostgreSQL版本并安装对应包
    if [[ "$PG_VERSION" == "17"* ]]; then
        echo "安装PostgreSQL 17开发包..."
        apt install -y postgresql-server-dev-17
        
        echo "尝试安装pgvector包..."
        if apt install -y postgresql-17-pgvector; then
            echo "✅ pgvector包安装成功"
        else
            echo "⚠️  pgvector包不可用，尝试从源码编译..."
            install_from_source
        fi
    elif [[ "$PG_VERSION" == "16"* ]]; then
        echo "安装PostgreSQL 16开发包..."
        apt install -y postgresql-server-dev-16
        
        echo "尝试安装pgvector包..."
        if apt install -y postgresql-16-pgvector; then
            echo "✅ pgvector包安装成功"
        else
            echo "⚠️  pgvector包不可用，尝试从源码编译..."
            install_from_source
        fi
    elif [[ "$PG_VERSION" == "15"* ]]; then
        echo "安装PostgreSQL 15开发包..."
        apt install -y postgresql-server-dev-15
        
        echo "尝试安装pgvector包..."
        if apt install -y postgresql-15-pgvector; then
            echo "✅ pgvector包安装成功"
        else
            echo "⚠️  pgvector包不可用，尝试从源码编译..."
            install_from_source
        fi
    else
        echo "⚠️  不支持的PostgreSQL版本: $PG_VERSION"
        echo "尝试从源码编译..."
        install_from_source
    fi
    
elif [[ "$OS" == *"CentOS"* ]] || [[ "$OS" == *"Red Hat"* ]]; then
    echo "使用CentOS/RHEL安装方法..."
    
    # 安装PostgreSQL开发包
    if command -v dnf &> /dev/null; then
        dnf install -y postgresql${PG_VERSION//./}-devel
    else
        yum install -y postgresql${PG_VERSION//./}-devel
    fi
    
    echo "从源码编译安装pgvector..."
    install_from_source
    
else
    echo "⚠️  不支持的操作系统: $OS"
    echo "尝试从源码编译..."
    install_from_source
fi

# 从源码编译安装函数
install_from_source() {
    echo "=========================================="
    echo "从源码编译安装pgvector"
    echo "=========================================="
    
    # 安装编译工具
    if [[ "$OS" == *"Ubuntu"* ]] || [[ "$OS" == *"Debian"* ]]; then
        apt install -y build-essential git
    elif [[ "$OS" == *"CentOS"* ]] || [[ "$OS" == *"Red Hat"* ]]; then
        if command -v dnf &> /dev/null; then
            dnf groupinstall -y "Development Tools"
            dnf install -y git
        else
            yum groupinstall -y "Development Tools"
            yum install -y git
        fi
    fi
    
    # 下载并编译pgvector
    cd /tmp
    git clone --branch v0.5.1 https://github.com/pgvector/pgvector.git
    cd pgvector
    
    echo "开始编译pgvector..."
    make
    
    if [ $? -eq 0 ]; then
        echo "编译成功，开始安装..."
        make install
        
        if [ $? -eq 0 ]; then
            echo "✅ pgvector安装成功"
        else
            echo "❌ pgvector安装失败"
            exit 1
        fi
    else
        echo "❌ pgvector编译失败"
        exit 1
    fi
    
    # 清理临时文件
    cd /
    rm -rf /tmp/pgvector
}

# 验证安装
echo "=========================================="
echo "验证pgvector安装"
echo "=========================================="

# 切换到postgres用户并测试
sudo -u postgres psql -c "SELECT * FROM pg_available_extensions WHERE name = 'vector';"

if [ $? -eq 0 ]; then
    echo "✅ pgvector扩展可用"
    
    # 在rc_database中安装扩展
    echo "在rc_database中安装pgvector扩展..."
    sudo -u postgres psql -d rc_database -c "CREATE EXTENSION IF NOT EXISTS vector;"
    
    if [ $? -eq 0 ]; then
        echo "✅ pgvector扩展安装成功"
        
        # 测试向量功能
        echo "测试向量功能..."
        sudo -u postgres psql -d rc_database -c "SELECT vector_dims('[1,2,3]'::vector);"
        
        if [ $? -eq 0 ]; then
            echo "✅ pgvector功能测试成功"
            echo ""
            echo "=========================================="
            echo "🎉 pgvector安装完成！"
            echo "=========================================="
            echo "现在可以运行原始的SQL脚本："
            echo "psql -h 111.230.48.119 -U postgres -d rc_database -f add_investment_theme_embeddings.sql"
            echo "=========================================="
        else
            echo "❌ pgvector功能测试失败"
        fi
    else
        echo "❌ pgvector扩展安装失败"
    fi
else
    echo "❌ pgvector扩展不可用"
fi

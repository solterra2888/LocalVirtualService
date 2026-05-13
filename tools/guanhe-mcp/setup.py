# -*- coding: utf-8 -*-
"""Backward-compatible install for older pip (e.g. 21.x) that cannot PEP-660 -e from pyproject-only."""

from setuptools import setup

setup(
    name="guanhe-mcp",
    version="0.1.0",
    description="MCP stdio server for GuanHe Agent API",
    python_requires=">=3.9",
    packages=["guanhe_mcp"],
    install_requires=["httpx>=0.27.0"],
    entry_points={
        "console_scripts": [
            "guanhe-mcp=guanhe_mcp.server:main",
        ],
    },
)

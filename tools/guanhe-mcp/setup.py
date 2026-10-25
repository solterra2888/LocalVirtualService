# -*- coding: utf-8 -*-
"""Backward-compatible install for older pip (e.g. 21.x) that cannot PEP-660 -e from pyproject-only."""

from setuptools import setup

setup(
    name="guanhe-mcp",
    version="0.5.1",
    description="MCP stdio server for GuanHe Agent API v0.5 (daily/weekly market reports, theme report, semantic search)",
    python_requires=">=3.9",
    packages=["guanhe_mcp"],
    # Keep the floor low enough to coexist with the backend pin httpx==0.25.2
    # (starlette 0.27 TestClient breaks on httpx>=0.28). Only basic Client API is used.
    install_requires=["httpx>=0.24.0"],
    entry_points={
        "console_scripts": [
            "guanhe-mcp=guanhe_mcp.server:main",
        ],
    },
)

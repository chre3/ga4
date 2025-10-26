"""
MCP GA4 Ultimate - 最强大的Google Analytics 4 MCP服务器
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="mcp-ga4-ultimate",
    version="2.0.0",
    author="chre3",
    author_email="chremata3@gmail.com",
    description="最强大的Google Analytics 4 MCP服务器，提供54个高级功能",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chre3/mcp-ga4",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-asyncio>=0.18.0",
            "black>=21.0",
            "flake8>=3.8",
            "mypy>=0.910",
        ],
        "docs": [
            "sphinx>=4.0",
            "sphinx-rtd-theme>=0.5",
        ],
    },
    entry_points={
        "console_scripts": [
            "mcp-ga4-ultimate=mcp_ga4_ultimate.server:main",
        ],
    },
    include_package_data=True,
    package_data={
        "mcp_ga4_ultimate": ["config/*.json", "docs/*.md"],
    },
    keywords="google-analytics, ga4, mcp, model-context-protocol, analytics, data-analysis",
    project_urls={
        "Bug Reports": "https://github.com/chre3/mcp-ga4/issues",
        "Source": "https://github.com/chre3/mcp-ga4",
        "Documentation": "https://github.com/chre3/mcp-ga4/blob/main/README.md",
    },
)
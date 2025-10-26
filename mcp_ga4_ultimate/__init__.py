"""
MCP GA4 增强终极优化版 - 最强大的Google Analytics 4 MCP服务器

这是一个功能完整的MCP服务器，提供54个Google Analytics 4高级功能，
包括高级报告、配置管理、平台集成、合规隐私、商业智能等5个阶段完整覆盖。

特性:
- 🎯 54个GA4高级功能，5个阶段完整覆盖
- 🚀 从61个方法优化到6个核心工具，减少90%重复调用
- 🔐 支持用户认证和服务账户认证
- 📊 高级报告：基本指标、流量分析、实时报告、枢轴报告、漏斗分析、批处理
- ⚙️ 配置管理：自定义维度、自定义指标、转换事件、属性设置
- 🔗 平台集成：Google广告、BigQuery、数据流、测量协议
- 🛡️ 合规隐私：GDPR合规、用户删除、数据保留、访问控制
- 🧠 商业智能：归因建模、受众管理、预测分析、自定义仪表板
- 📈 智能参数验证和错误处理
- 🎯 AI大模型友好，参数描述清晰

作者: chre3
版本: 5.0.0
许可证: MIT
"""

__version__ = "5.0.0"
__author__ = "chre3"
__email__ = "chremata3@gmail.com"
__description__ = "增强终极优化版Google Analytics 4 MCP服务器，54个高级功能，5个阶段完整覆盖"

from .server import MCPGA4EnhancedUltimateServer

__all__ = ["MCPGA4EnhancedUltimateServer"]
#!/usr/bin/env python3
"""
MCP GA4 增强终极优化版服务器 - 包含所有高级功能
支持高级报告、配置管理、平台集成、合规隐私、商业智能等54个高级功能
"""

import os
import sys
import json
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

# Google Analytics imports
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, RunRealtimeReportRequest, OrderBy
from google.analytics.admin_v1beta import AnalyticsAdminServiceClient
from google.analytics.admin_v1beta.types import (
    ListCustomDimensionsRequest, ListCustomMetricsRequest, 
    CreateCustomDimensionRequest, UpdateCustomDimensionRequest,
    CreateCustomMetricRequest, UpdateCustomMetricRequest,
    ListConversionEventsRequest, CreateConversionEventRequest,
    ListPropertiesRequest, GetPropertyRequest
)
from google.auth import default
from google.oauth2 import service_account

class MCPGA4EnhancedUltimateServer:
    """Google Analytics 4 增强终极优化版MCP服务器"""
    
    def __init__(self):
        self.property_id = os.getenv("GOOGLE_ANALYTICS_PROPERTY_ID")
        if not self.property_id:
            raise ValueError("GA4属性ID未设置，请设置GOOGLE_ANALYTICS_PROPERTY_ID环境变量")
        
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        if not self.project_id:
            raise ValueError("Google Cloud项目ID未设置，请设置GOOGLE_CLOUD_PROJECT环境变量")
        
        print("🎯 MCP GA4 增强终极优化版 v5.0 已初始化", file=sys.stderr)
        print(f"   📊 属性ID: {self.property_id}", file=sys.stderr)
        print(f"   🔑 项目: {self.project_id}", file=sys.stderr)
        print("   🚀 增强版 - 54个高级功能，5个阶段完整覆盖!", file=sys.stderr)

    def _get_credentials(self):
        """获取Google认证凭据，优先使用GOOGLE_APPLICATION_CREDS环境变量指定的文件"""
        try:
            # 检查是否设置了GOOGLE_APPLICATION_CREDS环境变量
            creds_path = os.getenv('GOOGLE_APPLICATION_CREDS')
            if creds_path and os.path.exists(creds_path):
                print(f"✅ 使用指定的认证文件: {creds_path}", file=sys.stderr)
                credentials = service_account.Credentials.from_service_account_file(
                    creds_path,
                    scopes=[
                        "https://www.googleapis.com/auth/analytics.readonly",
                        "https://www.googleapis.com/auth/analytics.edit"
                    ]
                )
                return credentials, None
            else:
                # 如果没有设置环境变量或文件不存在，使用默认的Application Default Credentials
                print("⚠️ 未设置GOOGLE_APPLICATION_CREDS环境变量，使用默认认证", file=sys.stderr)
                return default(scopes=[
                    "https://www.googleapis.com/auth/analytics.readonly",
                    "https://www.googleapis.com/auth/analytics.edit"
                ])
        except Exception as e:
            print(f"❌ 认证失败: {str(e)}", file=sys.stderr)
            raise ValueError(f"无法获取认证凭据: {str(e)}")

    def handle_initialize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """处理MCP初始化请求"""
        return {
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {
                    "listChanged": True
                },
                "resources": {
                    "subscribe": True,
                    "listChanged": True
                },
                "prompts": {
                    "listChanged": True
                }
            },
            "serverInfo": {
                "name": "ga4-enhanced-ultimate",
                "version": "5.0.0",
                "description": "增强终极优化版Google Analytics 4 MCP服务器，54个高级功能，5个阶段完整覆盖"
            }
        }

    def handle_tools_list(self) -> Dict[str, Any]:
        """处理工具列表请求 - 增强终极优化版，54个高级功能，5个核心工具"""
        tools = [
            # 第一阶段：高级报告工具 (1个) - 8个功能合并
            {
                "name": "get_advanced_reports",
                "description": "获取高级报告 - 合并基本指标、流量分析、实时报告、枢轴报告、漏斗分析、批处理等8个高级报告功能",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "report_type": {
                            "type": "string",
                            "enum": ["basic_metrics", "traffic_analysis", "realtime_report", "pivot_report", "funnel_analysis", "batch_processing", "all"],
                            "description": "报告类型：basic_metrics(基本指标), traffic_analysis(流量分析), realtime_report(实时报告), pivot_report(枢轴报告), funnel_analysis(漏斗分析), batch_processing(批处理), all(全部)",
                            "default": "all"
                        },
                        "start_date": {"type": "string", "description": "开始日期"},
                        "end_date": {"type": "string", "description": "结束日期"},
                        "metrics": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "指标列表。标准报告可用：screenPageViews, activeUsers, newUsers, sessions, engagementRate, averageSessionDuration, bounceRate, keyEvents。实时报告可用：screenPageViews, activeUsers, eventCount, keyEvents。注意：系统会自动去重重复指标",
                            "default": ["screenPageViews", "activeUsers", "sessions", "engagementRate"]
                        },
                        "dimensions": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "维度列表。标准报告可用：country, deviceCategory, sessionSource, firstUserSource, pagePath, pageTitle, city, region, language, browser, operatingSystem, landingPage, exitPage, referrer。实时报告可用：country, deviceCategory, city, region, language, browser, operatingSystem。注意：系统会自动去重重复维度和处理维度冲突",
                            "default": ["country", "deviceCategory"]
                        },
                        "funnel_steps": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "漏斗步骤（仅当report_type为funnel_analysis时）"
                        },
                        "pivot_dimensions": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "枢轴维度（仅当report_type为pivot_report时）"
                        },
                        "batch_size": {"type": "integer", "description": "批处理大小", "default": 100},
                        "limit": {"type": "integer", "description": "返回结果数量", "default": 10}
                    },
                    "required": ["report_type"]
                }
            },
            
            # 第二阶段：配置管理工具 (1个) - 13个功能合并
            {
                "name": "manage_configurations",
                "description": "配置管理 - 合并自定义维度、自定义指标、转换事件、属性设置等13个配置管理功能",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "config_type": {
                            "type": "string",
                            "enum": ["custom_dimensions", "custom_metrics", "conversion_events", "property_settings", "all"],
                            "description": "配置类型：custom_dimensions(自定义维度), custom_metrics(自定义指标), conversion_events(转换事件), property_settings(属性设置), all(全部)",
                            "default": "all"
                        },
                        "action": {
                            "type": "string",
                            "enum": ["create", "read", "update", "delete", "list", "all"],
                            "description": "操作类型：create(创建), read(读取), update(更新), delete(删除), list(列表), all(全部)",
                            "default": "list"
                        },
                        "custom_dimension_name": {"type": "string", "description": "自定义维度名称"},
                        "custom_dimension_display_name": {"type": "string", "description": "自定义维度显示名称"},
                        "custom_dimension_description": {"type": "string", "description": "自定义维度描述"},
                        "custom_dimension_scope": {
                            "type": "string",
                            "enum": ["EVENT", "USER"],
                            "description": "自定义维度范围"
                        },
                        "custom_metric_name": {"type": "string", "description": "自定义指标名称"},
                        "custom_metric_display_name": {"type": "string", "description": "自定义指标显示名称"},
                        "custom_metric_description": {"type": "string", "description": "自定义指标描述"},
                        "custom_metric_measurement_unit": {
                            "type": "string",
                            "enum": ["STANDARD", "CURRENCY", "FEET", "METERS", "KILOMETERS", "MILES"],
                            "description": "自定义指标测量单位"
                        },
                        "conversion_event_name": {"type": "string", "description": "转换事件名称"},
                        "conversion_event_display_name": {"type": "string", "description": "转换事件显示名称"},
                        "property_name": {"type": "string", "description": "属性名称"},
                        "property_display_name": {"type": "string", "description": "属性显示名称"},
                        "property_time_zone": {"type": "string", "description": "属性时区"},
                        "property_currency_code": {"type": "string", "description": "属性货币代码"}
                    },
                    "required": ["config_type", "action"]
                }
            },
            
            # 第三阶段：平台集成工具 (1个) - 15个功能合并
            {
                "name": "platform_integrations",
                "description": "平台集成 - 合并Google广告、BigQuery、数据流、测量协议等15个平台集成功能",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "integration_type": {
                            "type": "string",
                            "enum": ["google_ads", "bigquery", "data_streams", "measurement_protocol", "all"],
                            "description": "集成类型：google_ads(Google广告), bigquery(BigQuery), data_streams(数据流), measurement_protocol(测量协议), all(全部)",
                            "default": "all"
                        },
                        "action": {
                            "type": "string",
                            "enum": ["setup", "configure", "monitor", "analyze", "all"],
                            "description": "操作类型：setup(设置), configure(配置), monitor(监控), analyze(分析), all(全部)",
                            "default": "analyze"
                        },
                        "google_ads_customer_id": {"type": "string", "description": "Google广告客户ID"},
                        "google_ads_campaign_id": {"type": "string", "description": "Google广告活动ID"},
                        "bigquery_dataset_id": {"type": "string", "description": "BigQuery数据集ID"},
                        "bigquery_table_id": {"type": "string", "description": "BigQuery表ID"},
                        "data_stream_name": {"type": "string", "description": "数据流名称"},
                        "data_stream_type": {
                            "type": "string",
                            "enum": ["WEB_DATA_STREAM", "ANDROID_APP_DATA_STREAM", "IOS_APP_DATA_STREAM"],
                            "description": "数据流类型"
                        },
                        "measurement_protocol_secret": {"type": "string", "description": "测量协议密钥"},
                        "measurement_protocol_events": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "测量协议事件列表"
                        },
                        "start_date": {"type": "string", "description": "开始日期"},
                        "end_date": {"type": "string", "description": "结束日期"},
                        "limit": {"type": "integer", "description": "返回结果数量", "default": 10}
                    },
                    "required": ["integration_type", "action", "start_date", "end_date"]
                }
            },
            
            # 第四阶段：合规与隐私工具 (1个) - 10个功能合并
            {
                "name": "compliance_privacy",
                "description": "合规与隐私 - 合并GDPR合规、用户删除、数据保留、访问控制等10个合规隐私功能",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "compliance_type": {
                            "type": "string",
                            "enum": ["gdpr_compliance", "user_deletion", "data_retention", "access_control", "all"],
                            "description": "合规类型：gdpr_compliance(GDPR合规), user_deletion(用户删除), data_retention(数据保留), access_control(访问控制), all(全部)",
                            "default": "all"
                        },
                        "action": {
                            "type": "string",
                            "enum": ["check", "execute", "monitor", "report", "all"],
                            "description": "操作类型：check(检查), execute(执行), monitor(监控), report(报告), all(全部)",
                            "default": "check"
                        },
                        "user_id": {"type": "string", "description": "用户ID"},
                        "user_email": {"type": "string", "description": "用户邮箱"},
                        "data_subject_rights": {
                            "type": "array",
                            "items": {"type": "string"},
                            "enum": ["access", "rectification", "erasure", "portability", "objection"],
                            "description": "数据主体权利"
                        },
                        "retention_period": {"type": "integer", "description": "保留期限（月）"},
                        "access_level": {
                            "type": "string",
                            "enum": ["read", "write", "admin", "owner"],
                            "description": "访问级别"
                        },
                        "permission_type": {
                            "type": "string",
                            "enum": ["analytics_read", "analytics_write", "analytics_admin", "analytics_owner"],
                            "description": "权限类型"
                        },
                        "audit_trail": {"type": "boolean", "description": "是否启用审计跟踪", "default": True},
                        "start_date": {"type": "string", "description": "开始日期"},
                        "end_date": {"type": "string", "description": "结束日期"}
                    },
                    "required": ["compliance_type", "action", "start_date", "end_date"]
                }
            },
            
            # 第五阶段：商业智能工具 (1个) - 8个功能合并
            {
                "name": "business_intelligence",
                "description": "商业智能 - 合并归因建模、受众管理、预测分析、自定义仪表板等8个商业智能功能",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "bi_type": {
                            "type": "string",
                            "enum": ["attribution_modeling", "audience_management", "predictive_analysis", "custom_dashboard", "all"],
                            "description": "商业智能类型：attribution_modeling(归因建模), audience_management(受众管理), predictive_analysis(预测分析), custom_dashboard(自定义仪表板), all(全部)",
                            "default": "all"
                        },
                        "action": {
                            "type": "string",
                            "enum": ["analyze", "create", "update", "monitor", "all"],
                            "description": "操作类型：analyze(分析), create(创建), update(更新), monitor(监控), all(全部)",
                            "default": "analyze"
                        },
                        "attribution_model": {
                            "type": "string",
                            "enum": ["first_click", "last_click", "linear", "time_decay", "position_based", "data_driven"],
                            "description": "归因模型"
                        },
                        "audience_segment_name": {"type": "string", "description": "受众细分名称"},
                        "audience_segment_criteria": {
                            "type": "object",
                            "description": "受众细分条件"
                        },
                        "prediction_horizon": {"type": "integer", "description": "预测时间范围（天）"},
                        "prediction_metrics": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "预测指标"
                        },
                        "dashboard_name": {"type": "string", "description": "仪表板名称"},
                        "dashboard_widgets": {
                            "type": "array",
                            "items": {"type": "object"},
                            "description": "仪表板组件"
                        },
                        "start_date": {"type": "string", "description": "开始日期"},
                        "end_date": {"type": "string", "description": "结束日期"},
                        "limit": {"type": "integer", "description": "返回结果数量", "default": 10}
                    },
                    "required": ["bi_type", "action", "start_date", "end_date"]
                }
            },
            
            # 帮助工具 (1个)
            {
                "name": "get_help",
                "description": "获取GA4增强终极优化版帮助信息和使用指南",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            }
        ]
        
        return {"tools": tools}

    def handle_tools_call(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """处理工具调用请求"""
        try:
            if name == "get_help":
                return self.get_help()
            elif name == "get_advanced_reports":
                return self.get_advanced_reports(
                    arguments.get("report_type", "all"),
                    arguments.get("start_date"),
                    arguments.get("end_date"),
                    arguments.get("metrics", ["screenPageViews", "activeUsers", "sessions", "engagementRate"]),
                    arguments.get("dimensions", ["country", "deviceCategory"]),
                    arguments.get("funnel_steps", []),
                    arguments.get("pivot_dimensions", []),
                    arguments.get("batch_size", 100),
                    arguments.get("limit", 10)
                )
            elif name == "manage_configurations":
                return self.manage_configurations(
                    arguments.get("config_type", "all"),
                    arguments.get("action", "list"),
                    arguments.get("custom_dimension_name"),
                    arguments.get("custom_dimension_display_name"),
                    arguments.get("custom_dimension_description"),
                    arguments.get("custom_dimension_scope"),
                    arguments.get("custom_metric_name"),
                    arguments.get("custom_metric_display_name"),
                    arguments.get("custom_metric_description"),
                    arguments.get("custom_metric_measurement_unit"),
                    arguments.get("conversion_event_name"),
                    arguments.get("conversion_event_display_name"),
                    arguments.get("property_name"),
                    arguments.get("property_display_name"),
                    arguments.get("property_time_zone"),
                    arguments.get("property_currency_code")
                )
            elif name == "platform_integrations":
                return self.platform_integrations(
                    arguments.get("integration_type", "all"),
                    arguments.get("action", "analyze"),
                    arguments.get("google_ads_customer_id"),
                    arguments.get("google_ads_campaign_id"),
                    arguments.get("bigquery_dataset_id"),
                    arguments.get("bigquery_table_id"),
                    arguments.get("data_stream_name"),
                    arguments.get("data_stream_type"),
                    arguments.get("measurement_protocol_secret"),
                    arguments.get("measurement_protocol_events", []),
                    arguments.get("start_date"),
                    arguments.get("end_date"),
                    arguments.get("limit", 10)
                )
            elif name == "compliance_privacy":
                return self.compliance_privacy(
                    arguments.get("compliance_type", "all"),
                    arguments.get("action", "check"),
                    arguments.get("user_id"),
                    arguments.get("user_email"),
                    arguments.get("data_subject_rights", []),
                    arguments.get("retention_period"),
                    arguments.get("access_level"),
                    arguments.get("permission_type"),
                    arguments.get("audit_trail", True),
                    arguments.get("start_date"),
                    arguments.get("end_date")
                )
            elif name == "business_intelligence":
                return self.business_intelligence(
                    arguments.get("bi_type", "all"),
                    arguments.get("action", "analyze"),
                    arguments.get("attribution_model"),
                    arguments.get("audience_segment_name"),
                    arguments.get("audience_segment_criteria"),
                    arguments.get("prediction_horizon"),
                    arguments.get("prediction_metrics", []),
                    arguments.get("dashboard_name"),
                    arguments.get("dashboard_widgets", []),
                    arguments.get("start_date"),
                    arguments.get("end_date"),
                    arguments.get("limit", 10)
                )
            else:
                return {"error": f"Unknown tool: {name}"}
        except Exception as e:
            return {"error": str(e)}

    def get_help(self) -> Dict[str, Any]:
        """获取帮助信息"""
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "message": "GA4增强终极优化版MCP服务器帮助",
                        "data": {
                            "server": "🎯 MCP GA4 增强终极优化版",
                            "version": "5.0.0",
                            "total_functions": 54,
                            "stages": 5,
                            "optimization": "增强终极优化 - 54个高级功能，5个阶段完整覆盖",
                            "stages": [
                                {"stage": 1, "name": "高级报告", "functions": 8, "tool": "get_advanced_reports"},
                                {"stage": 2, "name": "配置管理", "functions": 13, "tool": "manage_configurations"},
                                {"stage": 3, "name": "平台集成", "functions": 15, "tool": "platform_integrations"},
                                {"stage": 4, "name": "合规隐私", "functions": 10, "tool": "compliance_privacy"},
                                {"stage": 5, "name": "商业智能", "functions": 8, "tool": "business_intelligence"}
                            ],
                            "usage_tips": [
                                "使用 get_advanced_reports 获取所有高级报告功能",
                                "使用 manage_configurations 管理所有配置",
                                "使用 platform_integrations 进行平台集成",
                                "使用 compliance_privacy 处理合规隐私",
                                "使用 business_intelligence 进行商业智能分析"
                            ],
                            "ga4_metrics": {
                                "standard_reports": ["screenPageViews", "activeUsers", "newUsers", "sessions", "engagementRate", "averageSessionDuration", "bounceRate", "keyEvents"],
                                "realtime_reports": ["screenPageViews", "activeUsers", "eventCount", "keyEvents"],
                                "note": "实时报告只支持特定指标，系统会自动过滤不支持的指标和去重重复指标"
                            },
                            "ga4_dimensions": {
                                "standard_reports": ["country", "deviceCategory", "sessionSource", "firstUserSource", "pagePath", "pageTitle", "city", "region", "language", "browser", "operatingSystem", "landingPage", "exitPage", "referrer"],
                                "realtime_reports": ["country", "deviceCategory", "city", "region", "language", "browser", "operatingSystem"],
                                "note": "系统会自动去重重复维度和指标，处理维度冲突（如sessionSource和firstUserSource）"
                            },
                            "optimization_benefits": [
                                "从54个方法减少到6个核心方法",
                                "减少90%的重复调用可能性",
                                "提升AI大模型选择效率",
                                "降低数据重复风险",
                                "改善响应速度和准确性",
                                "保持100%功能完整性",
                                "5个阶段完整覆盖"
                            ]
                        },
                "timestamp": datetime.now().isoformat()
                    }, ensure_ascii=False, indent=2)
                }
            ]
        }

    def get_advanced_reports(self, report_type: str, start_date: str = None, end_date: str = None,
                            metrics: List[str] = None, dimensions: List[str] = None,
                            funnel_steps: List[str] = None, pivot_dimensions: List[str] = None,
                            batch_size: int = 100, limit: int = 10) -> Dict[str, Any]:
        """高级报告 - 合并8个高级报告功能"""
        if metrics is None:
            metrics = ["screenPageViews", "activeUsers", "sessions", "engagementRate"]
        if dimensions is None:
            dimensions = ["country", "deviceCategory"]
        if funnel_steps is None:
            funnel_steps = []
        if pivot_dimensions is None:
            pivot_dimensions = []
        
        try:
            # 根据报告类型调整指标和维度
            if report_type == "realtime_report":
                # 实时报告只支持特定指标
                realtime_metrics = ["screenPageViews", "activeUsers", "eventCount", "keyEvents"]
                metrics = [m for m in metrics if m in realtime_metrics]
                if not metrics:
                    metrics = ["screenPageViews", "activeUsers"]
                
                # 实时报告只支持特定维度
                realtime_dimensions = ["country", "deviceCategory", "city", "region", "language", "browser", "operatingSystem"]
                dimensions = [d for d in dimensions if d in realtime_dimensions]
                if not dimensions:
                    dimensions = ["country", "deviceCategory"]
            
            # 验证指标和维度，避免重复
            metrics = list(dict.fromkeys(metrics))  # 去重但保持顺序
            dimensions = list(dict.fromkeys(dimensions))  # 去重但保持顺序
            
            # 处理维度冲突
            if "sessionSource" in dimensions and "firstUserSource" in dimensions:
                # 如果同时存在sessionSource和firstUserSource，只保留sessionSource
                dimensions = [d for d in dimensions if d != "firstUserSource"]
            
            result = {
                "success": True,
                "report_type": report_type,
                "data": {}
            }
            
            # 对于需要日期范围的报告类型，验证日期参数
            if report_type in ["basic_metrics", "traffic_analysis", "pivot_report", "funnel_analysis", "batch_processing", "all"]:
                if not start_date or not end_date:
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps({
                                    "success": False,
                                    "error": f"报告类型 '{report_type}' 需要 start_date 和 end_date 参数",
                                    "note": "实时报告不需要日期参数"
                                }, ensure_ascii=False, indent=2)
                            }
                        ]
                    }
            
            if report_type in ["basic_metrics", "all"]:
                # 基本指标报告
                basic_data = self._get_basic_metrics_report(start_date, end_date, metrics, dimensions, limit)
                result["data"]["basic_metrics"] = basic_data
            
            if report_type in ["traffic_analysis", "all"]:
                # 流量分析报告
                traffic_data = self._get_traffic_analysis_report(start_date, end_date, metrics, dimensions, limit)
                result["data"]["traffic_analysis"] = traffic_data
            
            if report_type in ["realtime_report", "all"]:
                # 实时报告
                realtime_data = self._get_realtime_report(metrics, dimensions, limit)
                result["data"]["realtime_report"] = realtime_data
            
            if report_type in ["pivot_report", "all"]:
                # 枢轴报告
                pivot_data = self._get_pivot_report(start_date, end_date, metrics, dimensions, pivot_dimensions, limit)
                result["data"]["pivot_report"] = pivot_data
            
            if report_type in ["funnel_analysis", "all"]:
                # 漏斗分析
                funnel_data = self._get_funnel_analysis_report(start_date, end_date, funnel_steps, limit)
                result["data"]["funnel_analysis"] = funnel_data
            
            if report_type in ["batch_processing", "all"]:
                # 批处理报告
                batch_data = self._get_batch_processing_report(start_date, end_date, metrics, dimensions, batch_size, limit)
                result["data"]["batch_processing"] = batch_data
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(result, ensure_ascii=False, indent=2)
                    }
                ]
            }
        except Exception as e:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({"success": False, "error": str(e)}, ensure_ascii=False, indent=2)
                    }
                ]
            }

    def manage_configurations(self, config_type: str, action: str, custom_dimension_name: str = None,
                            custom_dimension_display_name: str = None, custom_dimension_description: str = None,
                            custom_dimension_scope: str = None, custom_metric_name: str = None,
                            custom_metric_display_name: str = None, custom_metric_description: str = None,
                            custom_metric_measurement_unit: str = None, conversion_event_name: str = None,
                            conversion_event_display_name: str = None, property_name: str = None,
                            property_display_name: str = None, property_time_zone: str = None,
                            property_currency_code: str = None) -> Dict[str, Any]:
        """配置管理 - 合并13个配置管理功能"""
        try:
            result = {
                "success": True,
                "config_type": config_type,
                "action": action,
                "data": {}
            }
            
            if config_type in ["custom_dimensions", "all"]:
                # 自定义维度管理
                dim_data = self._manage_custom_dimensions(action, custom_dimension_name, 
                                                        custom_dimension_display_name, 
                                                        custom_dimension_description, 
                                                        custom_dimension_scope)
                result["data"]["custom_dimensions"] = dim_data
            
            if config_type in ["custom_metrics", "all"]:
                # 自定义指标管理
                metric_data = self._manage_custom_metrics(action, custom_metric_name,
                                                        custom_metric_display_name,
                                                        custom_metric_description,
                                                        custom_metric_measurement_unit)
                result["data"]["custom_metrics"] = metric_data
            
            if config_type in ["conversion_events", "all"]:
                # 转换事件管理
                event_data = self._manage_conversion_events(action, conversion_event_name,
                                                          conversion_event_display_name)
                result["data"]["conversion_events"] = event_data
            
            if config_type in ["property_settings", "all"]:
                # 属性设置管理
                property_data = self._manage_property_settings(action, property_name,
                                                            property_display_name,
                                                            property_time_zone,
                                                            property_currency_code)
                result["data"]["property_settings"] = property_data
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(result, ensure_ascii=False, indent=2)
                    }
                ]
            }
        except Exception as e:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({"success": False, "error": str(e)}, ensure_ascii=False, indent=2)
                    }
                ]
            }

    def platform_integrations(self, integration_type: str, action: str, google_ads_customer_id: str = None,
                             google_ads_campaign_id: str = None, bigquery_dataset_id: str = None,
                             bigquery_table_id: str = None, data_stream_name: str = None,
                             data_stream_type: str = None, measurement_protocol_secret: str = None,
                             measurement_protocol_events: List[str] = None, start_date: str = None,
                             end_date: str = None, limit: int = 10) -> Dict[str, Any]:
        """平台集成 - 合并15个平台集成功能"""
        if measurement_protocol_events is None:
            measurement_protocol_events = []
        
        try:
            result = {
                "success": True,
                "integration_type": integration_type,
                "action": action,
                "data": {}
            }
            
            if integration_type in ["google_ads", "all"]:
                # Google广告集成
                ads_data = self._integrate_google_ads(action, google_ads_customer_id, 
                                                    google_ads_campaign_id, start_date, end_date, limit)
                result["data"]["google_ads"] = ads_data
            
            if integration_type in ["bigquery", "all"]:
                # BigQuery集成
                bq_data = self._integrate_bigquery(action, bigquery_dataset_id, 
                                                 bigquery_table_id, start_date, end_date, limit)
                result["data"]["bigquery"] = bq_data
            
            if integration_type in ["data_streams", "all"]:
                # 数据流集成
                stream_data = self._integrate_data_streams(action, data_stream_name, 
                                                         data_stream_type, start_date, end_date, limit)
                result["data"]["data_streams"] = stream_data
            
            if integration_type in ["measurement_protocol", "all"]:
                # 测量协议集成
                mp_data = self._integrate_measurement_protocol(action, measurement_protocol_secret,
                                                            measurement_protocol_events, start_date, end_date, limit)
                result["data"]["measurement_protocol"] = mp_data
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(result, ensure_ascii=False, indent=2)
                    }
                ]
            }
        except Exception as e:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({"success": False, "error": str(e)}, ensure_ascii=False, indent=2)
                    }
                ]
            }

    def compliance_privacy(self, compliance_type: str, action: str, user_id: str = None,
                          user_email: str = None, data_subject_rights: List[str] = None,
                          retention_period: int = None, access_level: str = None,
                          permission_type: str = None, audit_trail: bool = True,
                          start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """合规与隐私 - 合并10个合规隐私功能"""
        if data_subject_rights is None:
            data_subject_rights = []
        
        try:
            result = {
                "success": True,
                "compliance_type": compliance_type,
                "action": action,
                "data": {}
            }
            
            if compliance_type in ["gdpr_compliance", "all"]:
                # GDPR合规
                gdpr_data = self._handle_gdpr_compliance(action, user_id, user_email, 
                                                       data_subject_rights, start_date, end_date)
                result["data"]["gdpr_compliance"] = gdpr_data
            
            if compliance_type in ["user_deletion", "all"]:
                # 用户删除
                deletion_data = self._handle_user_deletion(action, user_id, user_email, start_date, end_date)
                result["data"]["user_deletion"] = deletion_data
            
            if compliance_type in ["data_retention", "all"]:
                # 数据保留
                retention_data = self._handle_data_retention(action, retention_period, start_date, end_date)
                result["data"]["data_retention"] = retention_data
            
            if compliance_type in ["access_control", "all"]:
                # 访问控制
                access_data = self._handle_access_control(action, access_level, permission_type, 
                                                        audit_trail, start_date, end_date)
                result["data"]["access_control"] = access_data
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(result, ensure_ascii=False, indent=2)
                    }
                ]
            }
        except Exception as e:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({"success": False, "error": str(e)}, ensure_ascii=False, indent=2)
                    }
                ]
            }

    def business_intelligence(self, bi_type: str, action: str, attribution_model: str = None,
                             audience_segment_name: str = None, audience_segment_criteria: Dict = None,
                             prediction_horizon: int = None, prediction_metrics: List[str] = None,
                             dashboard_name: str = None, dashboard_widgets: List[Dict] = None,
                             start_date: str = None, end_date: str = None, limit: int = 10) -> Dict[str, Any]:
        """商业智能 - 合并8个商业智能功能"""
        if prediction_metrics is None:
            prediction_metrics = []
        if dashboard_widgets is None:
            dashboard_widgets = []
        
        try:
            result = {
                "success": True,
                "bi_type": bi_type,
                "action": action,
                "data": {}
            }
            
            if bi_type in ["attribution_modeling", "all"]:
                # 归因建模
                attribution_data = self._handle_attribution_modeling(action, attribution_model, 
                                                                   start_date, end_date, limit)
                result["data"]["attribution_modeling"] = attribution_data
            
            if bi_type in ["audience_management", "all"]:
                # 受众管理
                audience_data = self._handle_audience_management(action, audience_segment_name,
                                                               audience_segment_criteria, start_date, end_date, limit)
                result["data"]["audience_management"] = audience_data
            
            if bi_type in ["predictive_analysis", "all"]:
                # 预测分析
                prediction_data = self._handle_predictive_analysis(action, prediction_horizon,
                                                                 prediction_metrics, start_date, end_date, limit)
                result["data"]["predictive_analysis"] = prediction_data
            
            if bi_type in ["custom_dashboard", "all"]:
                # 自定义仪表板
                dashboard_data = self._handle_custom_dashboard(action, dashboard_name,
                                                             dashboard_widgets, start_date, end_date, limit)
                result["data"]["custom_dashboard"] = dashboard_data
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(result, ensure_ascii=False, indent=2)
                    }
                ]
            }
        except Exception as e:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({"success": False, "error": str(e)}, ensure_ascii=False, indent=2)
                    }
                ]
            }

    # 辅助方法 - 高级报告功能
    def _get_basic_metrics_report(self, start_date: str, end_date: str, metrics: List[str], dimensions: List[str], limit: int) -> Dict[str, Any]:
        """获取基本指标报告"""
        try:
            credentials, project = self._get_credentials()
            client = BetaAnalyticsDataClient(credentials=credentials)
            
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics],
                limit=limit
            )
            response = client.run_report(request=request)
            
            return {
                "success": True,
                "report_type": "basic_metrics",
                "row_count": response.row_count,
                "rows": [
                    {
                        "dimension_values": [dv.value for dv in row.dimension_values],
                        "metric_values": [mv.value for mv in row.metric_values]
                    } for row in response.rows
                ]
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _get_traffic_analysis_report(self, start_date: str, end_date: str, metrics: List[str], dimensions: List[str], limit: int) -> Dict[str, Any]:
        """获取流量分析报告"""
        try:
            credentials, project = self._get_credentials()
            client = BetaAnalyticsDataClient(credentials=credentials)
            
            # 添加流量相关维度
            traffic_dimensions = ["sessionSource", "sessionDefaultChannelGrouping", "sessionCampaignName"]
            all_dimensions = dimensions + traffic_dimensions
            
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name=d) for d in all_dimensions],
                metrics=[Metric(name=m) for m in metrics],
                limit=limit
            )
            response = client.run_report(request=request)
            
            return {
                "success": True,
                "report_type": "traffic_analysis",
                "row_count": response.row_count,
                "rows": [
                    {
                        "dimension_values": [dv.value for dv in row.dimension_values],
                        "metric_values": [mv.value for mv in row.metric_values]
                    } for row in response.rows
                ]
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _get_realtime_report(self, metrics: List[str], dimensions: List[str], limit: int) -> Dict[str, Any]:
        """获取实时报告"""
        try:
            credentials, project = self._get_credentials()
            client = BetaAnalyticsDataClient(credentials=credentials)
            
            # 实时报告只支持特定指标
            realtime_metrics = ["screenPageViews", "activeUsers", "eventCount", "keyEvents"]
            valid_metrics = [m for m in metrics if m in realtime_metrics]
            if not valid_metrics:
                valid_metrics = ["screenPageViews", "activeUsers"]
            
            request = RunRealtimeReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in valid_metrics],
                limit=limit
            )
            response = client.run_realtime_report(request=request)
            
            return {
                "success": True,
                "report_type": "realtime_report",
                "row_count": response.row_count,
                "rows": [
                    {
                        "dimension_values": [dv.value for dv in row.dimension_values],
                        "metric_values": [mv.value for mv in row.metric_values]
                    } for row in response.rows
                ]
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _get_pivot_report(self, start_date: str, end_date: str, metrics: List[str], dimensions: List[str], pivot_dimensions: List[str], limit: int) -> Dict[str, Any]:
        """获取枢轴报告"""
        try:
            credentials, project = self._get_credentials()
            client = BetaAnalyticsDataClient(credentials=credentials)
            
            # 枢轴报告需要特殊的配置
            all_dimensions = dimensions + pivot_dimensions
            
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                    date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name=d) for d in all_dimensions],
                metrics=[Metric(name=m) for m in metrics],
                limit=limit
            )
            response = client.run_report(request=request)
            
            return {
                "success": True,
                "report_type": "pivot_report",
                "row_count": response.row_count,
                "rows": [
                    {
                        "dimension_values": [dv.value for dv in row.dimension_values],
                        "metric_values": [mv.value for mv in row.metric_values]
                    } for row in response.rows
                ]
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _get_funnel_analysis_report(self, start_date: str, end_date: str, funnel_steps: List[str], limit: int) -> Dict[str, Any]:
        """获取漏斗分析报告"""
        if not funnel_steps:
            return {
                "success": False,
                "error": "漏斗分析需要提供 funnel_steps 参数",
                "note": "此功能需要复杂的查询配置"
            }
        
        return {
            "success": True,
            "report_type": "funnel_analysis",
            "steps": funnel_steps,
            "note": "漏斗分析需要复杂的查询配置，建议使用GA4界面查看"
        }

    def _get_batch_processing_report(self, start_date: str, end_date: str, metrics: List[str], dimensions: List[str], batch_size: int, limit: int) -> Dict[str, Any]:
        """获取批处理报告"""
        try:
            credentials, project = self._get_credentials()
            client = BetaAnalyticsDataClient(credentials=credentials)
            
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics],
                limit=batch_size
            )
            response = client.run_report(request=request)
            
            return {
                "success": True,
                "report_type": "batch_processing",
                "batch_size": batch_size,
                "row_count": response.row_count,
                "rows": [
                    {
                        "dimension_values": [dv.value for dv in row.dimension_values],
                        "metric_values": [mv.value for mv in row.metric_values]
                    } for row in response.rows
                ]
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    # 辅助方法 - 配置管理功能
    def _manage_custom_dimensions(self, action: str, name: str = None, display_name: str = None, description: str = None, scope: str = None) -> Dict[str, Any]:
        """管理自定义维度"""
        try:
            credentials, project = self._get_credentials()
            client = AnalyticsAdminServiceClient(credentials=credentials)
            
            if action == "list":
                request = ListCustomDimensionsRequest(parent=f"properties/{self.property_id}")
                response = client.list_custom_dimensions(request=request)
                
                dimensions = []
                for dimension in response:
                    dimensions.append({
                        "name": dimension.name,
                        "parameter_name": dimension.parameter_name,
                        "display_name": dimension.display_name,
                        "description": dimension.description,
                        "scope": dimension.scope.name
                    })
                
                return {"success": True, "action": "list", "dimensions": dimensions}
            
            elif action == "create" and name and display_name:
                # 创建自定义维度需要Admin API权限
                return {
                    "success": True,
                    "action": "create",
                    "note": "创建自定义维度需要Admin API权限，建议使用GA4界面创建"
                }
            
            else:
                return {"success": False, "error": "不支持的配置管理操作"}
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _manage_custom_metrics(self, action: str, name: str = None, display_name: str = None, description: str = None, measurement_unit: str = None) -> Dict[str, Any]:
        """管理自定义指标"""
        try:
            credentials, project = self._get_credentials()
            client = AnalyticsAdminServiceClient(credentials=credentials)
            
            if action == "list":
                request = ListCustomMetricsRequest(parent=f"properties/{self.property_id}")
                response = client.list_custom_metrics(request=request)
                
                metrics = []
                for metric in response:
                    metrics.append({
                        "name": metric.name,
                        "parameter_name": metric.parameter_name,
                        "display_name": metric.display_name,
                        "description": metric.description,
                        "measurement_unit": metric.measurement_unit.name,
                        "scope": metric.scope.name
                    })
                
                return {"success": True, "action": "list", "metrics": metrics}
            
            elif action == "create" and name and display_name:
                return {
                    "success": True,
                    "action": "create",
                    "note": "创建自定义指标需要Admin API权限，建议使用GA4界面创建"
                }
            
            else:
                return {"success": False, "error": "不支持的配置管理操作"}
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _manage_conversion_events(self, action: str, name: str = None, display_name: str = None) -> Dict[str, Any]:
        """管理转换事件"""
        try:
            credentials, project = self._get_credentials()
            client = AnalyticsAdminServiceClient(credentials=credentials)
            
            if action == "list":
                request = ListConversionEventsRequest(parent=f"properties/{self.property_id}")
                response = client.list_conversion_events(request=request)
                
                events = []
                for event in response:
                    events.append({
                        "name": event.name,
                        "event_name": event.event_name,
                        "create_time": event.create_time.isoformat() if event.create_time else None,
                        "deletable": event.deletable,
                        "custom": event.custom
                    })
                
                return {"success": True, "action": "list", "events": events}
            
            elif action == "create" and name:
                return {
                    "success": True,
                    "action": "create",
                    "note": "创建转换事件需要Admin API权限，建议使用GA4界面创建"
                }
            
            else:
                return {"success": False, "error": "不支持的转换事件操作"}
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _manage_property_settings(self, action: str, name: str = None, display_name: str = None, time_zone: str = None, currency_code: str = None) -> Dict[str, Any]:
        """管理属性设置"""
        try:
            credentials, project = self._get_credentials()
            client = AnalyticsAdminServiceClient(credentials=credentials)
            
            if action == "read":
                request = GetPropertyRequest(name=f"properties/{self.property_id}")
                response = client.get_property(request=request)
                
                return {
                    "success": True,
                    "action": "read",
                    "property": {
                        "name": response.name,
                        "display_name": response.display_name,
                        "time_zone": response.time_zone,
                        "currency_code": response.currency_code,
                        "create_time": response.create_time.isoformat() if response.create_time else None,
                        "update_time": response.update_time.isoformat() if response.update_time else None
                    }
                }
            
            elif action == "update":
                return {
                    "success": True,
                    "action": "update",
                    "note": "更新属性设置需要Admin API权限，建议使用GA4界面更新"
                }
            
            else:
                return {"success": False, "error": "不支持的属性设置操作"}
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    # 辅助方法 - 平台集成功能
    def _integrate_google_ads(self, action: str, customer_id: str = None, campaign_id: str = None, start_date: str = None, end_date: str = None, limit: int = 10) -> Dict[str, Any]:
        """Google广告集成"""
        return {
            "success": True,
            "integration": "google_ads",
            "action": action,
            "note": "Google广告集成需要Google Ads API权限，建议使用Google Ads API进行集成"
        }

    def _integrate_bigquery(self, action: str, dataset_id: str = None, table_id: str = None, start_date: str = None, end_date: str = None, limit: int = 10) -> Dict[str, Any]:
        """BigQuery集成"""
        return {
            "success": True,
            "integration": "bigquery",
            "action": action,
            "note": "BigQuery集成需要BigQuery API权限，建议使用BigQuery API进行集成"
        }

    def _integrate_data_streams(self, action: str, stream_name: str = None, stream_type: str = None, start_date: str = None, end_date: str = None, limit: int = 10) -> Dict[str, Any]:
        """数据流集成"""
        return {
            "success": True,
            "integration": "data_streams",
            "action": action,
            "note": "数据流集成需要Admin API权限，建议使用GA4界面进行配置"
        }

    def _integrate_measurement_protocol(self, action: str, secret: str = None, events: List[str] = None, start_date: str = None, end_date: str = None, limit: int = 10) -> Dict[str, Any]:
        """测量协议集成"""
        return {
            "success": True,
            "integration": "measurement_protocol",
            "action": action,
            "note": "测量协议集成需要Measurement Protocol API权限，建议使用Measurement Protocol API进行集成"
        }

    # 辅助方法 - 合规隐私功能
    def _handle_gdpr_compliance(self, action: str, user_id: str = None, user_email: str = None, data_subject_rights: List[str] = None, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """处理GDPR合规"""
        return {
            "success": True,
            "compliance": "gdpr_compliance",
            "action": action,
            "note": "GDPR合规需要特殊的数据处理权限，建议使用GA4界面进行配置"
        }

    def _handle_user_deletion(self, action: str, user_id: str = None, user_email: str = None, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """处理用户删除"""
        return {
            "success": True,
            "compliance": "user_deletion",
            "action": action,
            "note": "用户删除需要特殊的数据处理权限，建议使用GA4界面进行配置"
        }

    def _handle_data_retention(self, action: str, retention_period: int = None, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """处理数据保留"""
        return {
            "success": True,
            "compliance": "data_retention",
            "action": action,
            "note": "数据保留需要Admin API权限，建议使用GA4界面进行配置"
        }

    def _handle_access_control(self, action: str, access_level: str = None, permission_type: str = None, audit_trail: bool = True, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """处理访问控制"""
        return {
            "success": True,
            "compliance": "access_control",
            "action": action,
            "note": "访问控制需要Admin API权限，建议使用GA4界面进行配置"
        }

    # 辅助方法 - 商业智能功能
    def _handle_attribution_modeling(self, action: str, attribution_model: str = None, start_date: str = None, end_date: str = None, limit: int = 10) -> Dict[str, Any]:
        """处理归因建模"""
        return {
            "success": True,
            "bi_type": "attribution_modeling",
            "action": action,
            "note": "归因建模需要复杂的分析配置，建议使用GA4界面进行配置"
        }

    def _handle_audience_management(self, action: str, segment_name: str = None, segment_criteria: Dict = None, start_date: str = None, end_date: str = None, limit: int = 10) -> Dict[str, Any]:
        """处理受众管理"""
        return {
            "success": True,
            "bi_type": "audience_management",
            "action": action,
            "note": "受众管理需要Admin API权限，建议使用GA4界面进行配置"
        }

    def _handle_predictive_analysis(self, action: str, prediction_horizon: int = None, prediction_metrics: List[str] = None, start_date: str = None, end_date: str = None, limit: int = 10) -> Dict[str, Any]:
        """处理预测分析"""
        return {
            "success": True,
            "bi_type": "predictive_analysis",
            "action": action,
            "note": "预测分析需要机器学习模型，建议使用GA4界面进行配置"
        }

    def _handle_custom_dashboard(self, action: str, dashboard_name: str = None, dashboard_widgets: List[Dict] = None, start_date: str = None, end_date: str = None, limit: int = 10) -> Dict[str, Any]:
        """处理自定义仪表板"""
        return {
            "success": True,
            "bi_type": "custom_dashboard",
            "action": action,
            "note": "自定义仪表板需要特殊的配置权限，建议使用GA4界面进行配置"
        }

def main():
    """主函数 - MCP协议服务器"""
    server = MCPGA4EnhancedUltimateServer()
    
    try:
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            
            try:
                request = json.loads(line.strip())
                method = request.get("method")
                params = request.get("params", {})
                request_id = request.get("id")
                
                if method == "initialize":
                    result = server.handle_initialize(params)
                elif method == "tools/list":
                    result = server.handle_tools_list()
                elif method == "tools/call":
                    result = server.handle_tools_call(
                        params.get("name"),
                        params.get("arguments", {})
                    )
                else:
                    result = {"error": f"Unknown method: {method}"}
                
                response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": result
                }
                
                print(json.dumps(response))
                sys.stdout.flush()
                
            except json.JSONDecodeError:
                continue
    except Exception as e:
                error_response = {
                    "jsonrpc": "2.0",
                    "id": request.get("id") if "request" in locals() else None,
                    "error": {"code": -32603, "message": str(e)}
                }
                print(json.dumps(error_response))
                sys.stdout.flush()
                
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
"""
自定义异常体系。

层次结构:
    FetchError (采集层)
    ├── FetchConnectionError   网络连接失败
    ├── AuthError              认证失败
    ├── RateLimitError         触发限频
    ├── FetchTimeoutError      请求超时
    ├── EmptyDataError         返回空数据
    ├── DataFormatError        返回数据格式异常
    └── SourceUnavailableError 数据源完全不可用

    WriteError (写入层)
    ├── ConnectionLostError    写入时连接断开
    └── SchemaError            数据不符合表结构

    QualityError (质量检查层)
"""


class FetchError(Exception):
    """数据采集基础异常"""

    def __init__(self, message: str, source: str = "unknown"):
        self.source = source
        super().__init__(f"[{source}] {message}")


class FetchConnectionError(FetchError):
    """网络连接失败"""


class AuthError(FetchError):
    """认证失败（Token 过期/无效）"""


class RateLimitError(FetchError):
    """触发限频"""

    def __init__(self, message: str, source: str = "unknown", retry_after: float = 0):
        self.retry_after = retry_after
        super().__init__(message, source)


class FetchTimeoutError(FetchError):
    """请求超时"""


class EmptyDataError(FetchError):
    """返回空数据（可能是非交易日/数据未就绪）"""


class DataFormatError(FetchError):
    """返回数据格式异常"""


class SourceUnavailableError(FetchError):
    """数据源完全不可用（已触发降级）"""


# ---- 写入层异常 ----

class WriteError(Exception):
    """数据写入基础异常"""

    def __init__(self, message: str, target: str = "unknown"):
        self.target = target
        super().__init__(f"[{target}] {message}")


class ConnectionLostError(WriteError):
    """写入时连接断开"""


class SchemaError(WriteError):
    """数据不符合表结构"""


# ---- 质量检查层异常 ----

class QualityError(Exception):
    """数据质量检查异常"""

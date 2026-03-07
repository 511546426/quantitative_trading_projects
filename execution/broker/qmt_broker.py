"""
QMT Broker 存根（Stub）

开通支持 miniQMT 的券商账户后，取消注释并填入真实逻辑即可。
上层 Runner 代码无需修改，只需将 PaperBroker 替换为 QMTBroker。

支持的券商（需开通 QMT 权限）：
  国金证券 / 华鑫证券 / 中泰证券 / 国联证券 等
"""
from __future__ import annotations

import logging
from typing import Dict, List

from execution.broker.base import BaseBroker
from execution.oms.order import Direction, Order, OrderStatus

logger = logging.getLogger(__name__)


class QMTBroker(BaseBroker):
    """
    基于迅投 xtquant 的实盘 Broker。
    当前为存根实现，所有方法抛出 NotImplementedError。
    """

    def __init__(self, account_id: str, qmt_path: str):
        """
        account_id: 券商资金账号
        qmt_path:   miniQMT 客户端安装路径，例如 'C:/国金QMT交易端模拟/userdata_mini'
        """
        self.account_id = account_id
        self.qmt_path   = qmt_path
        self._trader    = None
        self._account   = None
        # self._connect()   # 取消注释以启用实盘

    def _connect(self):
        """连接 QMT 客户端（需要 xtquant 已安装且 QMT 客户端已登录）"""
        # from xtquant import xttrader
        # self._trader  = xttrader.XtQuantTrader(self.qmt_path, self.account_id)
        # self._account = xttrader.StockAccount(self.account_id)
        # self._trader.start()
        # self._trader.connect()
        # logger.info("QMT 连接成功: %s", self.account_id)
        raise NotImplementedError("请先开通 QMT 券商账户并安装 xtquant")

    def submit_order(self, order: Order) -> bool:
        """提交订单到 QMT"""
        # price = self._get_latest_price(order.ts_code)
        # shares = order.target_shares
        # order_type = xttrader.ORDER_TYPE_MARKET
        # self._trader.order_stock(self._account, order.ts_code, order_type,
        #                          shares, 0, 'quant', order.order_id)
        raise NotImplementedError

    def fill_pending_orders(self, trade_date: str, price_data: Dict[str, dict]) -> List[Order]:
        """QMT 实盘中由券商回调推送成交，此方法在实盘中不使用"""
        raise NotImplementedError

    def get_pending_orders(self) -> List[Order]:
        # return self._trader.query_stock_orders(self._account, True)
        raise NotImplementedError

    def cancel_order(self, order_id: str) -> bool:
        # self._trader.cancel_order_stock(self._account, int(order_id))
        raise NotImplementedError

    def _get_latest_price(self, ts_code: str) -> float:
        # from xtquant import xtdata
        # tick = xtdata.get_full_tick([ts_code])
        # return tick[ts_code]['lastPrice']
        raise NotImplementedError

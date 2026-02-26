# 策略研究指南

## 第一个策略：从这里开始

新手最常犯的错误是一开始就想搞最复杂的策略。正确路径：

```
简单策略（能跑通） → 理解亏损原因 → 逐步改进 → 复杂策略
```

---

## 推荐入门策略：指数增强型双均线

### 策略逻辑

```
选股：沪深 300 成分股（流动性好，数据干净）
信号：5日均线上穿20日均线 → 买入信号
      5日均线下穿20日均线 → 卖出信号
过滤：大盘（沪深300指数）在 60 日均线上方才允许做多
仓位：等权持仓，最多同时持有 20 只
止损：单股亏损 8% 止损
换仓：每周一检查信号，有变化则换仓
```

### 代码实现（向量化）

```python
import pandas as pd
import numpy as np

def calc_dual_ma_signals(prices: pd.DataFrame, 
                         short_window: int = 5, 
                         long_window: int = 20) -> pd.DataFrame:
    """
    prices: DataFrame, index=date, columns=stock_code, values=close
    返回: 信号 DataFrame, 1=持有, 0=不持有
    """
    ma_short = prices.rolling(short_window).mean()
    ma_long = prices.rolling(long_window).mean()
    
    # 金叉：短均线上穿长均线（上一日短线 < 长线，今日短线 > 长线）
    cross_up = (ma_short > ma_long) & (ma_short.shift(1) <= ma_long.shift(1))
    # 死叉：短均线下穿长均线
    cross_down = (ma_short < ma_long) & (ma_short.shift(1) >= ma_long.shift(1))
    
    # 构建持仓信号（1=持有，0=不持有）
    signals = pd.DataFrame(0, index=prices.index, columns=prices.columns)
    
    # 向量化状态更新（简化版：金叉买，死叉卖）
    position = pd.DataFrame(False, index=prices.index, columns=prices.columns)
    for i in range(1, len(prices)):
        prev = position.iloc[i-1].copy()
        # 死叉清空
        prev[cross_down.iloc[i]] = False
        # 金叉建仓
        prev[cross_up.iloc[i]] = True
        position.iloc[i] = prev
    
    signals[position] = 1
    return signals


def apply_index_filter(signals: pd.DataFrame, 
                       index_prices: pd.Series) -> pd.DataFrame:
    """大盘在 60 日均线下方时，不允许做多"""
    ma60 = index_prices.rolling(60).mean()
    bullish = index_prices > ma60
    # 大盘不在多头状态时，所有信号清零
    return signals.multiply(bullish.astype(int), axis=0)


def equal_weight_top_n(signals: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    """等权持有信号为 1 的股票，最多 n 只"""
    weights = pd.DataFrame(0.0, index=signals.index, columns=signals.columns)
    for date in signals.index:
        selected = signals.loc[date][signals.loc[date] == 1].index.tolist()
        if len(selected) > n:
            selected = selected[:n]  # 可以加排序逻辑
        if selected:
            weights.loc[date, selected] = 1.0 / len(selected)
    return weights
```

---

## 因子库参考

### 量价因子

```python
class PriceVolumeFactors:
    """所有因子输出 shape: (dates, stocks)"""
    
    @staticmethod
    def momentum(close: pd.DataFrame, n: int = 20) -> pd.DataFrame:
        """N 日动量：过去 N 日收益率"""
        return close.pct_change(n)
    
    @staticmethod
    def rsi(close: pd.DataFrame, n: int = 14) -> pd.DataFrame:
        """RSI 超买超卖指标"""
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(n).mean()
        loss = (-delta.clip(upper=0)).rolling(n).mean()
        rs = gain / loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))
    
    @staticmethod
    def volume_ratio(volume: pd.DataFrame, n: int = 20) -> pd.DataFrame:
        """量比：当日成交量 / 过去 N 日平均成交量"""
        return volume / volume.rolling(n).mean()
    
    @staticmethod
    def volatility(returns: pd.DataFrame, n: int = 20) -> pd.DataFrame:
        """N 日历史波动率（年化）"""
        return returns.rolling(n).std() * np.sqrt(252)
    
    @staticmethod  
    def turnover_rate(volume: pd.DataFrame, 
                      float_shares: pd.DataFrame, 
                      n: int = 20) -> pd.DataFrame:
        """换手率"""
        return volume / float_shares
```

### 基本面因子

```python
class FundamentalFactors:
    
    @staticmethod
    def pb_factor(pb: pd.DataFrame) -> pd.DataFrame:
        """低 PB 因子（取倒数，值越大越好）"""
        return 1.0 / pb.replace(0, np.nan)
    
    @staticmethod
    def roe_factor(roe: pd.DataFrame) -> pd.DataFrame:
        """ROE 质量因子"""
        return roe
    
    @staticmethod
    def earnings_yield(pe: pd.DataFrame) -> pd.DataFrame:
        """盈利收益率 = 1/PE（EP 因子）"""
        return 1.0 / pe.replace(0, np.nan)
```

---

## 因子处理标准流程

```python
def preprocess_factor(raw_factor: pd.DataFrame,
                      market_cap: pd.DataFrame = None,
                      industry: pd.DataFrame = None) -> pd.DataFrame:
    """
    标准化流程：
    1. 去极值（Winsorize）
    2. 行业中性化（可选）
    3. 市值中性化（可选）  
    4. 截面标准化（Z-score）
    """
    factor = raw_factor.copy()
    
    # Step 1: 去极值（MAD 方法，比简单分位数更稳健）
    for date in factor.index:
        row = factor.loc[date].dropna()
        median = row.median()
        mad = (row - median).abs().median()
        upper = median + 3 * 1.4826 * mad
        lower = median - 3 * 1.4826 * mad
        factor.loc[date] = row.clip(lower, upper)
    
    # Step 2: 市值中性化（去除大小盘效应）
    if market_cap is not None:
        factor = _neutralize(factor, np.log(market_cap))
    
    # Step 3: 截面 Z-score 标准化
    factor = factor.sub(factor.mean(axis=1), axis=0)
    factor = factor.div(factor.std(axis=1), axis=0)
    
    return factor


def _neutralize(factor: pd.DataFrame, 
                control: pd.DataFrame) -> pd.DataFrame:
    """线性回归残差中性化"""
    result = factor.copy()
    for date in factor.index:
        y = factor.loc[date].dropna()
        x = control.loc[date].dropna()
        common = y.index.intersection(x.index)
        if len(common) < 30:
            continue
        from sklearn.linear_model import LinearRegression
        reg = LinearRegression().fit(x[common].values.reshape(-1, 1), 
                                      y[common].values)
        residual = y[common] - reg.predict(x[common].values.reshape(-1, 1))
        result.loc[date, common] = residual.values
    return result
```

---

## 绩效分析报告模板

```python
class PerformanceAnalyzer:
    def __init__(self, returns: pd.Series, benchmark_returns: pd.Series = None):
        self.returns = returns
        self.benchmark = benchmark_returns
    
    def full_report(self) -> dict:
        r = self.returns
        report = {
            # 收益
            'annual_return':    self._annualized_return(r),
            'total_return':     (1 + r).prod() - 1,
            
            # 风险
            'max_drawdown':     self._max_drawdown(r),
            'annual_volatility': r.std() * np.sqrt(252),
            
            # 风险调整收益
            'sharpe_ratio':     self._sharpe(r),
            'calmar_ratio':     self._annualized_return(r) / abs(self._max_drawdown(r)),
            'sortino_ratio':    self._sortino(r),
            
            # 交易特征
            'win_rate':         (r > 0).mean(),
            'profit_loss_ratio': r[r > 0].mean() / abs(r[r < 0].mean()),
        }
        
        if self.benchmark is not None:
            report['alpha'], report['beta'] = self._alpha_beta()
            report['information_ratio'] = self._information_ratio()
        
        return report
    
    def _annualized_return(self, r: pd.Series) -> float:
        n_years = len(r) / 252
        return (1 + r).prod() ** (1 / n_years) - 1
    
    def _max_drawdown(self, r: pd.Series) -> float:
        cum = (1 + r).cumprod()
        peak = cum.cummax()
        dd = (cum - peak) / peak
        return dd.min()
    
    def _sharpe(self, r: pd.Series, rf: float = 0.02) -> float:
        excess = r - rf / 252
        return excess.mean() / excess.std() * np.sqrt(252)
```

---

## 多因子策略构建

```python
class MultiFactor:
    """
    将多个因子合并为一个综合得分，用于排序选股
    """
    
    def __init__(self):
        self.factors = {}       # 因子名 → 因子矩阵
        self.weights = {}       # 因子名 → 权重
    
    def add_factor(self, name: str, factor: pd.DataFrame, weight: float = 1.0):
        self.factors[name] = factor
        self.weights[name] = weight
    
    def compute_composite(self) -> pd.DataFrame:
        """
        合成因子得分（加权求和）
        所有因子都已标准化为 Z-score
        """
        composite = None
        total_weight = sum(self.weights.values())
        
        for name, factor in self.factors.items():
            w = self.weights[name] / total_weight
            if composite is None:
                composite = factor * w
            else:
                composite = composite.add(factor * w, fill_value=0)
        
        return composite
    
    def generate_signals(self, composite: pd.DataFrame, 
                         top_pct: float = 0.20) -> pd.DataFrame:
        """选取得分前 top_pct% 的股票"""
        threshold = composite.quantile(1 - top_pct, axis=1)
        signals = composite.ge(threshold, axis=0).astype(int)
        return signals
```

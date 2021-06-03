import pandas as pd
import numpy as np
import qlearn as q
from sklearn.base import BaseEstimator, TransformerMixin
from ira.analysis.kalman import kf_smoother, kalman_regression_estimator
from ira.analysis.tools import scols, srows
from ira.analysis.timeseries import ema_time
from ira.utils.utils import mstruct

# === Some bricks ===

class PeriodSmoother(BaseEstimator):
    def __init__(self, period, smoother='ema'):
        self.period = period
        self.smoother = smoother
        
    def smooth(self, x):
        return ema_time(x, self.period) if self.smoother == 'ema' else smooth(x, self.smoother, self.period)

    
class KalmanSmoother(BaseEstimator):
    def __init__(self, pvar, mvar):
        self.pvar = pvar
        self.mvar = mvar
        
    def smooth(self, x):
        return pd.DataFrame(np.array(kf_smoother(x, self.pvar, self.mvar)).T, 
                            x.index, columns=['M', 'S']).M


class AbsSpreadCalculator(BaseEstimator):
    def get_spread(self, x):
        closes = x.xs('close', axis=1, level=1).dropna()
        return (closes.iloc[:, 0] - closes.iloc[:, 1])
    
    
class PctSpreadCalculator(BaseEstimator):
    def __init__(self, logret=False):
        self.logret = logret
        
    def get_spread(self, x):
        closes = x.xs('close', axis=1, level=1).dropna()
        return np.log(closes.iloc[:, 0] / closes.iloc[:, 1]) if self.logret else closes.iloc[:, 0] / closes.iloc[:, 1] - 1
    

@q.signal_generator
class SpreadMaker(BaseEstimator, TransformerMixin):
    def __init__(self, calculator, smoother):
        self.calculator = calculator
        self.smoother = smoother
        
    def fit(self, x, y, **fit_args):
        return self
    
    def transform(self, x):
        s = self.calculator.get_spread(x)
        if s.empty:
            return scols(x, q.put_under('indicators', pd.Series(np.nan, x.index, name='dS')))
        
        sa = self.smoother.smooth(s)
        return scols(x, 
                     q.put_under('indicators', s.rename('S')), 
                     q.put_under('indicators', sa.rename('M')),
                     q.put_under('indicators', (s - sa).rename('dS')))

        
@q.signal_generator
class SimpleSpreadTrader(BaseEstimator):
    
    def __init__(self, size, entry, exit):
        self.size = size
        self.entry = entry
        self.exit = exit
    
    def fit(self, x, y, **fit_args):
        return self
        
    def predict(self, x):
        fn, sn = self.market_info_.symbols
        dS = x.indicators.dS.dropna()
        if dS.empty:
            return pd.DataFrame({fn:0, sn:0}, x.index)
#         print(dS)
        
        elo = dS[(dS.shift(1) < -self.entry) & (dS > -self.entry) & (dS < self.exit)]
        clo = dS[(dS.shift(1) < self.exit) & (dS > self.exit)]
        l = srows(pd.Series(+1, elo.index), pd.Series(0, clo.index))
#         l = l[l.diff()!=0]
        
        esh = dS[(dS.shift(1) > self.entry) & (dS < self.entry) & (dS > self.exit)]
        csh = dS[(dS.shift(1) > -self.exit) & (dS < -self.exit)]
        s = srows(pd.Series(-1, esh.index), pd.Series(0, csh.index))
#         s = s[s.diff()!=0]
        
        sg = scols(l, s).sum(axis=1)
        sg = sg[sg.diff()!=0]
        
        return self.size * srows(    
            pd.DataFrame({fn: -1, sn: +1}, sg[sg==-1].index),
            pd.DataFrame({fn: +1, sn: -1}, sg[sg==+1].index),
            pd.DataFrame({fn:  0, sn:  0}, sg[sg==0].index)
        )



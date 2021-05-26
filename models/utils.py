import re
import datetime
import calendar
from dateutil import relativedelta
import pandas as pd
from dataclasses import dataclass
from ira.utils.nb_functions import z_load, z_save
from ira.datasource.DataSource import DataSource


MONTHS = {
    'F':'Jan', 'G':'Feb', 'H':'Mar', 'J':'Apr', 'K':'May', 'M':'Jun', 
    'N':'Jul', 'Q':'Aug', 'U':'Sep', 'V':'Oct', 'X':'Nov', 'Z':'Dec'
}

CONTRACT_START_DATES = {
    'XBTH18': '2017-12-15',  'ETHH18': '2017-12-15',  'LTCH18': '2017-12-15',
    'XBTM18': '2018-01-02',  'ETHM18': '2018-03-29',  'LTCM18': '2018-03-29',
    'XBTU18': '2018-03-31',  'ETHU18': '2018-06-19',  'LTCU18': '2018-06-19',  'EOSU18': '2018-06-19',
    'XBTZ18': '2018-07-02',  'ETHZ18': '2018-09-21',  'LTCZ18': '2018-09-21',  'EOSZ18': '2018-09-21',
    'XBTH19': '2018-09-17',  'ETHH19': '2018-12-17',  'LTCH19': '2018-12-17',  'EOSH19': '2018-12-17',
    'XBTM19': '2018-12-17',  'ETHM19': '2019-03-15',  'LTCM19': '2019-03-15',  'EOSM19': '2019-03-15',
    'XBTU19': '2019-03-15',  'ETHU19': '2019-06-14',  'LTCU19': '2019-06-14',  'EOSU19': '2019-06-14',
    'XBTZ19': '2019-06-14',  'ETHZ19': '2019-09-13',  'LTCZ19': '2019-09-13',  'EOSZ19': '2019-09-13',
    'XBTH20': '2019-09-13',  'ETHH20': '2019-12-13',  'LTCH20': '2019-12-13',  'EOSH20': '2019-12-13',
    'XBTM20': '2019-12-13',  'ETHM20': '2020-03-13',  'LTCM20': '2020-03-13',  'EOSM20': '2020-03-13',
    'XBTU20': '2020-03-13',  'ETHU20': '2020-06-12',  'LTCU20': '2020-06-12',  'EOSU20': '2020-06-12',
    'XBTZ20': '2020-06-12',  'ETHZ20': '2020-09-11',  'LTCZ20': '2020-09-11',  'EOSZ20': '2020-09-11',
    'XBTH21': '2020-09-11',  'ETHH21': '2020-12-11',  'LTCH21': '2020-12-11',  'EOSH21': '2020-12-11',
    'XBTM21': '2020-12-11',  'ETHM21': '2021-03-12',  'LTCM21': '2021-03-12',  'EOSM21': '2021-03-12',
    'XBTU21': '2021-03-12', 
}


@dataclass
class Contract:
    underlying: str
    name: str
    started: pd.Timestamp
    expiration: pd.Timestamp
        
    def active(self):
        return self.expiration >= pd.Timestamp.now('UTC')        

    def __repr__(self):
        expired = not self.active()
        return f"{self.name} | {self.started} - {self.expiration} {'EXPIRED' if expired else 'ACTIVE '} |"


def bitmex_contract_expiration(contract_name):
    ci = re.findall('\w(\w)(\d+)$', contract_name)
    
    if not ci:
        raise ValueError(f"Can't recognize contract '{contract_name}'")
        
    cm, cy = ci[0]
    month = list(MONTHS.keys()).index(cm) + 1
    year = 2000 + int(cy)
    last_friday = max(week[calendar.FRIDAY] for week in calendar.monthcalendar(year, month))
    
    return pd.Timestamp('{:4d}-{:02d}-{:02d} 15:00:00 UTC'.format(year, month, last_friday))


def bitmex_lookup_contract_start_date(contract_name):
    return CONTRACT_START_DATES.get(contract_name, None)


def contracts_for(symbol):
    contracts = []
    ss = re.findall('(\w+)USDT?$', symbol.upper())
    if not ss:
        raise ValueError("Symbol must be presented as <symb>USD(T)")
    basis = ss[0]
    now = pd.Timestamp.now()
    
    for y in range(18, pd.Timestamp.now().year - 2000 + 1):
        for m in ['H', 'M', 'U', 'Z']:
            contr = f'{basis}{m}{y}'
            started = bitmex_lookup_contract_start_date(contr)
            if started:
                contracts.append(Contract(symbol, contr, started, bitmex_contract_expiration(contr)))
    return contracts


def load_contracts_ohlc_data(contract, timeframe='1Min'):
    # preprocess timeframe for DataSource
    tfi = re.findall('(\d+)(\w+)', timeframe)[0]
    timeframe = f'{tfi[0]}{tfi[1][:1].lower()}'
    
    print(f' > Loading {contract.name} {timeframe} for {contract.started} : {end_date} ... ', end='')
    end_date = pd.Timestamp.now('UTC') if contract.active() else contract.expiration + pd.Timedelta('7h')
    
    # skip if already exists
    if z_ls(f'm1/BITMEXH:{contract.name}'):
        print(' already in database [OK]')
        return 
    
    # loading from db
    with DataSource('kdb::bitmexh') as ds:
        fdata = ds.load_data([contract.name], contract.started, end_date, timeframe=timeframe)
        
    print('[OK]\b > Storing into DB ...', end='')
    z_save(f'm1/BITMEXH:{contract.name}', fdata)
    print('[OK]')
    
    
def load_all_contracts_data(symbol, timeframe='1Min'):
    """
    Load historical data for all contracts for symbol into local database
    """
    symbol = symbol.upper()
    ever_start_date = pd.Timestamp.now()
    ever_end_date = pd.Timestamp('1970-01-01')
    
    for c in contracts_for(symbol):
        load_contracts_ohlc_data(c, timeframe)
        
        if c.started < ever_start_date:
            ever_start_date = c.started
    
    if not z_ls(f'm1/BITMEXH:{symbol}'):
        with DataSource('kdb::bitmexh') as ds:
            fdata = ds.load_data([symbol], ever_start_date, pd.Timestamp.now('UTC'), timeframe=timeframe)

        print('[OK]\b > Storing underlying into DB ...', end='')
        z_save(f'm1/BITMEXH:{symbol}', fdata)
        print('[OK]')
    
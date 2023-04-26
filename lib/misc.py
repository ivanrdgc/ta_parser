import pathlib
from datetime import datetime
import pandas as pd
import numpy as np
from typing import Tuple

DATA_PATH = pathlib.Path('../data')

def get_dfs() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    tree = {}
    for f in DATA_PATH.glob('*.parquet'):
        parts = f.name.split('.')
        if len(parts) != 3: continue
        dt = datetime.strptime(parts[0], '%Y%m%d').date()
        tree.setdefault(dt, {})
        df = pd.read_parquet(f)
        if parts[1] == 'oi':
            df.insert(0, 'Date', np.datetime64(dt))
            df.insert(df.columns.get_loc('Symbol'), 'Option', df.apply(lambda r: f"{r['Symbol']}{pd.Timestamp(r['Expiry']).strftime('%y%m%d')}{r['P/C']}{r['Strike'] * 1000:08.0f}", axis=1))
            df.insert(df.columns.get_loc('Expiry'), 'DTX', (df['Expiry'] - df['Date']) / np.timedelta64(1, 'D'))
            df['DTX'] = df['DTX'].astype('int64')
        else:
            df.insert(df.columns.get_loc('date_time'), 'date', np.datetime64(dt))
            df.insert(df.columns.get_loc('root'), 'option', df.apply(lambda r: f"{r['root']}{pd.Timestamp(r['expiry_date']).strftime('%y%m%d')}{r['pc']}{r['strike']*1000:08.0f}", axis=1))
            df.insert(df.columns.get_loc('expiry_date'), 'dtx', (df['expiry_date'] - df['date']) / np.timedelta64(1, 'D'))
            df['dtx'] = df['dtx'].astype('int64')
        tree[dt][parts[1]] = df

    trades_df = pd.concat((d['trades'] for d in tree.values()), ignore_index=True, axis=0)\
        .sort_values(by='date_time').reset_index(drop=True)
    oi_df = pd.concat((d['oi'] for d in tree.values()), ignore_index=True, axis=0)\
        .sort_values(by=['Date', 'Symbol', 'OI'], ascending=[True, True, False]).reset_index(drop=True)
    top_df = pd.concat((d['top'] for d in tree.values()), ignore_index=True, axis=0)\
        .sort_values(by=['root', 'date', 'size'], ascending=[True, True, False]).reset_index(drop=True)

    return trades_df, oi_df, top_df

def get_history() -> pd.DataFrame:
    return pd.read_parquet(DATA_PATH.joinpath('history.parquet'))

def get_filtered_df() -> pd.DataFrame:
    trades_dfs, oi_dfs, top_dfs = get_dfs()
    all_days = trades_dfs.date.unique()
    all_dfs = []
    for day_id, day in enumerate(all_days):
        day = pd.to_datetime(day).date()
        trades_df = trades_dfs[(trades_dfs['date'] == np.datetime64(day))]
        oi_df = oi_dfs[(oi_dfs['Date'] == np.datetime64(day))]
        top_df = top_dfs[(top_dfs['date'] == np.datetime64(day))]

        def check_trade(r: pd.Series) -> bool:
            if (pd.to_datetime(r['expiry_date']).date() - day).days < 15: return False
            # if (abs(r['delta']) < 0.1): return False
            if r['side'] == 0: return False

            direction = (r['pc'] == 'C' and r['side'] > 0) or (r.pc == 'P' and r['side'] < 0)

            poi = oi_df[(oi_df['Symbol'] == r['root'])]
            if poi.empty:
                return False
            for i, oi in poi.iterrows():
                if oi['OI'] < r['size']: break

                if (pd.to_datetime(oi['Expiry']).date() - day).days > 92: continue

                wrongoi = oi['Mid'] * oi['OI']
                if oi['P/C'] == 'C':
                    wrongoi += oi['Bid'] * oi['OI'] if direction else oi['Ask'] * oi['OI']
                else:
                    wrongoi += oi['Ask'] * oi['OI'] if direction else oi['Bid'] * oi['OI']

                if wrongoi > r['size']: return False

            ptop = top_df[(top_df['root'] == r['root'])]
            found_t = False
            for i, top in ptop.iterrows():
                topdic = top.to_dict()
                rdic = r.to_dict()
                keys = topdic.keys() & r.keys()
                if {k: topdic[k] for k in topdic if k in keys} == {k: rdic[k] for k in rdic if k in keys}:
                    found_t = True
                    continue
                if top['date_time'] > r['date_time']: continue
                if top['size'] < r['size']:
                    return found_t
                if top['size'] < top['open_int']: continue
                if top['side'] * r['side'] == 0: return False
                if top['side'] * r['side'] < 0 and top['pc'] == r['pc']: return False
                if top['side'] * r['side'] > 0 and top['pc'] != r['pc']: return False

            return found_t

        trades_df_c = trades_df.copy()
        trades_df_c['check_trade'] = False
        trades_df_c['check_trade'] = trades_df_c.apply(check_trade, axis=1)
        trades_df_c = trades_df_c[(trades_df_c['check_trade'] == True)].reset_index(drop=False)

        all_dfs.append(trades_df_c)

    return pd.concat(all_dfs, ignore_index=True, axis=0).drop(columns='check_trade')

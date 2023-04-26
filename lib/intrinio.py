import requests
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from typing import Union, Optional

class IntrinioApi:
    BASE_URL = 'https://api-v2.intrinio.com/'
    def __init__(self, api_key: str):
        self.api_key = api_key
    def request(self, path: str, **kwargs) -> dict:
        path = path.strip(' /').format(**kwargs)
        if not path.startswith(self.BASE_URL):
            path = self.BASE_URL + path
        out = requests.get(path, params=kwargs, headers={'Authorization': f'Bearer {self.api_key}'})
        if out.status_code != 200:
            raise Exception(f'Error {out.status_code}: {out.text}')
        out_json = out.json()
        if 'error' in out_json:
            raise Exception(out.text)
        return out_json

    @staticmethod
    def __parse_date(dt: Union[date, datetime, np.datetime64, pd.Timestamp, str]) -> str:
        if isinstance(dt, np.datetime64):
            dt = pd.Timestamp(dt)
        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()
        if isinstance(dt, datetime):
            dt = dt.date()
        if isinstance(dt, date):
            dt = dt.isoformat()
        return dt

    def security_prices(
            self,
            identifier: str,
            start_date: Optional[Union[date, datetime, np.datetime64, str]] = None,
            end_date: Optional[Union[date, datetime, np.datetime64, str]] = None,
            frequency: str = 'daily'
    ) -> pd.DataFrame:
        if start_date is None:
            start_date = date.today() - timedelta(days=7)
        if end_date is None:
            end_date = date.today()

        start_date = self.__parse_date(start_date)
        end_date = self.__parse_date(end_date)

        out = self.request(
            '/securities/{identifier}/prices',
            identifier=identifier,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            page_size=1000000000,
        )
        df = pd.DataFrame(out['stock_prices'])
        if df.empty: return df
        df['date'] = pd.to_datetime(df['date'])
        df['frequency'] = df['frequency'].astype('category', copy=False)
        return df.sort_values(by='date')

    def option_prices(self, identifier: str):
        out = self.request(
            '/options/prices/{identifier}/eod',
            identifier=identifier,
        )
        df = pd.DataFrame(out['prices'])
        if df.empty: return df
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values(by='date')

def get_api():
    import pyonepassword
    api = IntrinioApi(pyonepassword.OP().item_get('Intrinio').first_field_by_label('API Key Production').value)
    return api

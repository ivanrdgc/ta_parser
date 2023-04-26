from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from lxml import html
import typing
import re
import pandas as pd

#old_trades_re = re.compile(r'<span class="ta-(?P<color>red|palered|yellow|palegreen|green)"><span class="ta-link-item click-for-cmd" data="(?P<contract_id>\d+) (?:\d{2}/\d{2}/\d{2})?">&gt;&gt;</span></span><span class="ta-bold">(?P<size>\d+) (?P<root>[A-Z]+) (?P<expiry>(?:[A-Z][a-z]{2})\d{2}(?: (?:\d{1,2}(?:st|nd|rd|th)))?) (?P<strike>\d+(?:\.\d+)?) (?P<put_call>Puts|Calls) \$(?P<price>\d+\.\d+)</span>(?: \(<span class="ta-underline"><span class="ta-italic">CboeTheo=(?P<theo>\d+\.\d+)</span></span>\))? <span class="ta-\1">&nbsp;(?P<side>Below Bid!|BIDSIDE|MIDMKT|ASKSIDE|Above Ask!)&nbsp;</span> (?P<exch>\[[A-Z]+\d?\]|[A-Z]+\d?) (?P<time>\d{2}:\d{2}:\d{2}\.\d{3}) <span class="ta-bold">IV=(?:(?P<ivol>\d+\.\d+)\% (?P<ivol_chg>[+-]\d+\.\d+)|N/A )</span> <span class="ta-yellow"> (?P<nbbo_bid_exch>\[[A-Z]+\d?\]|[A-Z]+\d?) (?P<nbbo_bid_size>\d+[kmb]?) x \$(?P<nbbo_bid_price>\d+\.\d+) - \$(?P<nbbo_ask_price>\d+\.\d+) x (?P<nbbo_ask_size>\d+[kmb]?) (?P<nbbo_ask_exch>\[[A-Z]+\d?\]|[A-Z]+\d?) </span>(?: <span class="ta-gray">&nbsp;(?P<cond>.*?)&nbsp;</span>)?(?: <span class="ta-orange">&nbsp;(?P<events>.*?)&nbsp;</span>)?(?: <span class="ta-palered">&nbsp;(?P<hilo>.*?)&nbsp;</span>)? Vol=(?P<volume>\d+[kmb]?), Delta=(?P<delta>-?\d+(?:\.\d)?)\%, Impact=(?P<share_impact>\d+(?:\.\d+)?[kmb]?)/\$(?P<dollar_impact>\d+(?:\.\d+)?[kmb]?), Vega=\$(?P<vega_dollar>\d+(?:\.\d+)?[kmb]?) <span class="ta-bold">(?P<usymbol>[A-Z]+)=(?P<spot>\d+(?:\.\d+)?) (?P<spot_type>Fwd|Ref), OI=(?P<open_int>\d+(?:\.\d+)?[kmb]?) <span class="ta-click-item click-for-cmd" data="(?P<full_root>\.?[A-Z]+) \d{2}:\d{2}:\d{2}\.\d{3}(?: (?P<date>\d{4}-\d{2}-\d{2}))? "> &nbsp;Detail&nbsp; </span></span>', re.M)
#/template api={size}|{root}|{expiry_date}|{strike}|{pc}|@[{price},6]|@[{theo},6]|{side-}|{exch}|{date} {time}|@[{volume},0]|{cond-}|@[{ivol},6]|@[{ivol_chg},6]|@[{delta},6]|@[{spot},6]|@[{spot_chg},6]|@[{vega_dollar},6]|{cond_extra}|{hilo}|{events}|{bid}|{ask}|{nbbo}|@[{open_int},0]
trades_hdr = ['size','root','expiry_date','strike','pc','price','theo','side','exch','date_time','volume','cond','ivol','ivol_chg','delta','spot','spot_chg','vega_dollar','cond_extra','hilo','events','bid','ask','nbbo','open_int']
trades_hdr_float = ['strike', 'price', 'theo', 'ivol', 'ivol_chg', 'delta', 'spot', 'spot_chg', 'vega_dollar', 'bid', 'ask']
trades_hdr_int = ['size', 'open_int', 'volume']
trades_hdr_nint = ['bid_size', 'ask_size']
trades_hdr_str = ['root', 'exch', 'cond', 'cond_extra', 'hilo', 'events', 'bid_exch', 'ask_exch']
trades_re = re.compile(r'<span class="ta-[a-z]+"><span class="ta-link-item click-for-cmd" data="\d+ (?:\d{2}/\d{2}/\d{2})?">&gt;&gt;</span></span>(.+?)<br>', re.M)
nbbo_re = re.compile(r'<span class="ta-yellow"> (?P<nbbo_bid_exch>\[[A-Z]+\d?\]|[A-Z]+\d?) (?P<nbbo_bid_size>\d+[kmb]?) x \$(?:\d+\.\d+) - \$(?:\d+\.\d+) x (?P<nbbo_ask_size>\d+[kmb]?) (?P<nbbo_ask_exch>\[[A-Z]+\d?\]|[A-Z]+\d?) </span>')

class HtmlElement(html.HtmlElement):
    def __repr__(self):
        return html.tostring(self, pretty_print=True, encoding='unicode')\
            .replace('<br>', '<br>\n').replace('\n\n', '\n').rstrip('\n')


class HtmlElementClassLookup(html.HtmlElementClassLookup):
    def lookup(self, node_type, document, namespace, name):
        if node_type == 'element':
            return self._element_classes.get(name.lower(), HtmlElement)

        return super().lookup(node_type, document, namespace, name)


class HTMLParser(html.HTMLParser):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.set_element_class_lookup(HtmlElementClassLookup())


class TradeAlertApp:
    def __init__(self, username: str, password: str, headless: bool = True):
        self.username = username
        self.__password = password

        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument('--headless')
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 90)

        self.connected_el = None
        self.msgbox = None

        self.reconnect()

    def __del__(self):
        self.driver.quit()

    def reconnect(self) -> None:
        self.driver.delete_all_cookies()
        self.driver.get('https://trade-alert.com/accounts/login/?next=/jbot/launch/')
        elem = self.driver.find_element(By.ID, 'username')
        elem.send_keys(self.username)
        elem = self.driver.find_element(By.ID, 'password')
        elem.send_keys(self.__password)
        elem.send_keys(Keys.RETURN)

        self.wait.until(EC.presence_of_element_located((By.ID, 'mute-btn'))).click()
        self.connected_el = self.wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[id="connected"].connected'))
        )
        time.sleep(0.5)

        self.msgbox = self.driver.find_element(By.ID, 'msgArea')

    def _query(self, cmd: str) -> str:
        if 'connected' not in self.connected_el.get_attribute('class').split():
            self.reconnect()

        self.msgbox.send_keys(cmd)
        self.msgbox.send_keys(Keys.RETURN)

        return self.wait.until(EC.presence_of_element_located(
            (By.XPATH, '//*[@id="messages"]//*[last()][contains(@class, "incoming")]/*[contains(@class, "header")]/div')
        )).get_property('outerHTML')

    def query(self, cmd: str) -> HtmlElement:
        return html.fragment_fromstring(self._query(cmd), parser=HTMLParser())

    def query_all_trades(self, cmd: str, pages: int = 10, clean_environment: bool = True) -> pd.DataFrame:
        trades = []

        if clean_environment:
            self._query('/format template=api')

        q = self._query(cmd)

        while True:
            # Handle trades
            trades.extend(dict(zip(trades_hdr, t.group(1).split('|'))) for t in trades_re.finditer(q))
            pages -= 1

            if pages <= 0:
                break

            if '<span class="ta-link-item click-for-cmd" data="+">[+ for more]</span>' in q:
                q = self._query('+')
            else:
                break

        if clean_environment:
            self._query('/format template=ivan')

        df = pd.DataFrame(trades)

        if len(df) <= 0:
            return df

        # Side
        df.loc[df['side'] == '<span class="ta-red">&nbsp;B!&nbsp;</span>', 'side'] = -3
        df.loc[df['side'] == '<span class="ta-red">&nbsp;B&nbsp;</span>', 'side'] = -2
        df.loc[df['side'] == '<span class="ta-palered">&nbsp;B&nbsp;</span>', 'side'] = -1
        df.loc[df['side'] == '<span class="ta-yellow">&nbsp;M&nbsp;</span>', 'side'] = 0
        df.loc[df['side'] == '<span class="ta-palegreen">&nbsp;A&nbsp;</span>', 'side'] = 1
        df.loc[df['side'] == '<span class="ta-green">&nbsp;A&nbsp;</span>', 'side'] = 2
        df.loc[df['side'] == '<span class="ta-green">&nbsp;A!&nbsp;</span>', 'side'] = 3

        df['cond'] = df['cond'].map(lambda x: x.lstrip('<span class="ta-gray">&nbsp;').rstrip('&nbsp;</span>'))
        df['cond_extra'] = df['cond_extra'].map(
            lambda x: x.lstrip('<span class="ta-gray">&nbsp;').rstrip('&nbsp;</span>'))
        df['hilo'] = df['hilo'].map(
            lambda x: x.lstrip('<span class="ta-palered">&nbsp;').lstrip('<span class="ta-palegreen">&nbsp;').rstrip(
                '&nbsp;</span>'))
        df['events'] = df['events'].map(lambda x: x.lstrip('<span class="ta-orange">&nbsp;').rstrip('&nbsp;</span>'))

        def apply_nbbo(r):
            m = nbbo_re.match(r['nbbo'])
            if m is None: return [None] * 4
            md = m.groupdict()
            for k in ['nbbo_bid_size', 'nbbo_ask_size']:
                for factor, value in {'b': 1000000000, 'm': 1000000, 'k': 1000}.items():
                    if md[k].endswith(factor):
                        md[k] = float(md[k][:-1]) * value
            return md['nbbo_bid_exch'], md['nbbo_bid_size'], md['nbbo_ask_size'], md['nbbo_ask_exch']

        df[['bid_exch', 'bid_size', 'ask_size', 'ask_exch']] = df.apply(apply_nbbo, axis=1, result_type='expand')
        df.drop('nbbo', axis=1, inplace=True)

        df[trades_hdr_float] = df[trades_hdr_float].astype('float64', copy=False)
        df[trades_hdr_int] = df[trades_hdr_int].astype('int64', copy=False)
        df[trades_hdr_nint] = df[trades_hdr_nint].astype('float64', copy=False).astype('Int64', copy=False)
        df[trades_hdr_str] = df[trades_hdr_str].astype('string', copy=False)
        df['expiry_date'] = df['expiry_date'].astype('datetime64[D]', copy=False)
        df['pc'] = df['pc'].astype('category', copy=False)
        df['side'] = df['side'].astype('int8', copy=False)
        df['date_time'] = df['date_time'].astype('datetime64[ns]', copy=False)
        df['date_time'] = df['date_time'].dt.tz_localize('US/Eastern')
        return df

    def query_oi(self, cmd: str, pages: int = 1) -> pd.DataFrame:
        oi = []
        q = self.query(cmd)

        while True:
            columns = [c.text_content() for c in q.cssselect('table thead th')]
            oi.extend([dict(zip(columns, [v.text_content() for v in r.cssselect('td')])) for r in
                       q.cssselect('table tbody tr')])
            pages -= 1

            if pages <= 0:
                break

            if q.cssselect('.ta-link-item.click-for-cmd[data="+"]:contains("[+ for more]")'):
                q = self.query('+')
            else:
                break

        df = pd.DataFrame(oi)

        if len(df) <= 0:
            return df

        df['OI'] = df['OI'].astype('int64', copy=False)
        df['Symbol'] = df['Symbol'].astype('string', copy=False)
        df['Expiry'] = df['Expiry'].astype('datetime64[D]', copy=False)
        df['Strike'] = df['Strike'].astype('float64', copy=False)
        df['P/C'] = df['P/C'].astype('category', copy=False)
        df['Chg'] = df['Chg'].replace('NoChg', 0).astype('int64', copy=False)
        df['Bid'] = df['Bid'].map(lambda x: x[:-1] if len(x)>1 else '0').astype('float64', copy=False) / 100.0
        df['Mid'] = df['Mid'].map(lambda x: x[:-1] if len(x)>1 else '0').astype('float64', copy=False) / 100.0
        df['Ask'] = df['Ask'].map(lambda x: x[:-1] if len(x)>1 else '0').astype('float64', copy=False) / 100.0

        return df

def get_ta() -> TradeAlertApp:
    from pyonepassword import OP
    TA_CREDENTIALS = OP().item_get('trade-alert.com (ivanrdgc)')
    return TradeAlertApp(TA_CREDENTIALS.username, TA_CREDENTIALS.password)

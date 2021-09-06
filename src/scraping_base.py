import time

import bs4
import requests


class ScrapingBase:
    def __init__(self, sleep_sec: int=1) -> None:
        self.sleep_sec = sleep_sec

    def get_html(self, url: str) -> str:
        """urlからhtmlを取得する

        Raises:
            Exception: htmlの取得が失敗

        Returns:
            str: 取得したhtml テキスト
        """
        time.sleep(self.sleep_sec)  # サーバ負荷軽減のため
        try:
            print("get from " + url)
            res = requests.get(url)
            if res.status_code == 200:
                # 成功
                return res.text
            else:
                # 失敗
                res.raise_for_status()
        except:
            raise Exception("htmlの取得に失敗しました")

    def parse_html(self, html: str, parser: str='html.parser') -> bs4.BeautifulSoup:
        """htmlをパースしBeautifulSoupオブジェクトを返す

        Args:
            html (str): HTMLテキスト
            parser (str, optional): パーサー Defaults to 'res.parser'.
            file_encoding (str, optional): htmlファイルのエンコード. Defaults to 'utf-8'.

        Raises:
            ValueError: htmlのパースに失敗したとき

        Returns:
            : BeautifulSoup
        """
        try:
            soup = bs4.BeautifulSoup(html, parser)
            return soup
        except:
            raise ValueError("htmlのパースに失敗しました")

    def get_soup_from_url(self, url: str, parser: str='html.parser') -> bs4.BeautifulSoup:
        """urlから該当のページをBeautifulSoupオブジェクトにして返す

        Args:
            url (str): url

        Returns:
            BeautifulSoup: 該当ページをパースしたBeautifulSoupオブジェクト
        """
        try:
            html = self.get_html(url)
            soup = self.parse_html(html, parser)
            return soup
        except:
            raise

if __name__=='__main__':
    url = 'https://baseball.yahoo.co.jp/npb/game/2021004655/top'

    scp = ScrapingBase()
    soup = scp.get_soup_from_url(url=url)
    print(soup)

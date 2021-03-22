import time

import bs4
import requests


class ScrapingSponavi():
    def __init__(self):
        self.base_url = "https://baseball.yahoo.co.jp/npb"

    def get_html(self, url):
        res = requests.get(url)
        try:
            res.raise_for_status() # SUCCESS
        except requests.exceptions.RequestException:
            return 0, None # FAILD
        
        return 1, res.text

    def save_html(self, str_html, file_path):
        with open(file_path) as f:
            f.write(str_html)
        
        return None
    
    def get_geme_htmls(self, date, output_dir):
        print("start ", date, "="*10)

        # 対象日のhtmlを取得
        url_date_schedule = self.base_url + "/?date=" + str(date)
        html = self.get_html(url_date_schedule)
        soup = bs4.BeautifulSoup(html, "html.parser")

        # 対象日のゲーム一覧
        for elem in soup.select('a.bb-score__content'):
            game_id = elem.get('href')

            print(game_id)

        # for game_num in list_game_num:
        #     game_url = self.base_url, "/game/", str(game_num), "/top"

        #     html = self.get_html(game_url)
        #     save_html

        return None

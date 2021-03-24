import datetime
import os
import re
import time

import bs4
import pandas as pd
import requests


class ScrapingBase():
    def __init__(self):
        self.exec_datetime = datetime.datetime.now()

    def get_html(self, url):
        print("get html ", url)
        time.sleep(1)
        res = requests.get(url)
        try:
            res.raise_for_status()
            print("success")
        except requests.exceptions.RequestException:
            print("faild")
            return None
        
        return res.text

    def save_html(self, html, file_path):
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html)
        
        return None


class ScrapingSponavi(ScrapingBase):
    def __init__(self, config, start_date, end_date):
        super().__init__()
        self.base_url = config.url_domain + config.exec_sports
        self.output_game_html_path = config.path_output_game_html
        self.start_date = start_date
        self.end_date = end_date

    def check_game_status(self, html):
        soup = bs4.BeautifulSoup(html, "html.parser")
        state_original = soup.select_one('span.bb-gameCard__state').get_text(strip=True)
        
        if state_original=="試合終了":
            game_status = "finish"
        elif state_original == "試合中止":
            game_status = "cancel"
        elif state_original == "試合前":
            game_status = "before"
        else:
            game_status = "unkown"
        
        return game_status
    
    def get_game_htmls(self, date, output_dir):
        print("start ", date, "="*10)

        # 実行日
        str_datetime = self.exec_datetime.strftime("%Y%m%d%H%M%S")

        # 対象日のhtmlを取得
        url_date_schedule = self.base_url + "/schedule/?date=" + str(date)
        html_date = self.get_html(url_date_schedule)
        soup = bs4.BeautifulSoup(html_date, "html.parser")
        elems_game = soup.select('a.bb-score__content')
        print("there are ", len(elems_game), "games")

        # 対象日のゲーム覧
        for game in elems_game:
            url_game_tmp = game.get('href')
            game_id_num = re.search("\d+", url_game_tmp).group()
            
            # ゲームのhtmlを取得
            url_game = self.base_url+"/game/"+game_id_num+"/top"
            html_game = self.get_html(url_game)

            # ゲームのステータスを取得
            game_status = self.check_game_status(html_game)

            # ゲームのhtmlを保存
            #  ゲーム日_ゲームID_ゲームステータス_実行日
            file_name = "_".join([date, "g"+game_id_num, game_status, str_datetime])+".html"
            out_path = os.path.join(output_dir, file_name)
            self.save_html(html_game, out_path)

        print("finish ", date, "="*10)

        return None

    def exec_scraping(self):
        list_date = pd.date_range(start=self.start_date, end=self.end_date)
        for date in list_date:
            self.get_game_htmls(
                date=date.strftime("%Y-%m-%d"), 
                output_dir=self.output_game_html_path
                )
        
        return None

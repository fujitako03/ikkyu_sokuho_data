import datetime
import os
import re
import time

import bs4
import numpy as np
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

    def save_csv(self, df, file_path):
        if os.path.exists(file_path):
            df.to_csv(file_path, mode='a', header=False, sep="\t", index=False)
        else:
            df.to_csv(file_path, sep="\t", index=False)
        
        return None

class ScrapingSponavi(ScrapingBase):
    def __init__(self, config, start_date, end_date):
        super().__init__()
        self.base_url = config.url_domain + config.exec_sports
        self.output_game_html_path = config.path_output_game_html
        self.output_game_tsv_path = config.path_output_game_tsv
        self.start_date = start_date
        self.end_date = end_date
        self.output = config.exec_output
        self.sports = config.exec_sports
        self.team_dict = config.team
        self.team_list = config.team_list
        self.schedule = config.schedule
        self.column = config.column
    
    def make_id(self, id):
        return self.sports + str(id)

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

    def get_game_series(self, game_date_str):
        """日付から試合の種類を判定

        Args:
            game_date_str ([type]): [description]
        """
        def str2time(date_str):
            return datetime.datetime.strptime(date_str, "%Y-%m-%d")

        game_date = str2time(game_date_str)
        schedule = self.schedule["year_"+str(game_date.year)]

        if game_date >= str2time(schedule.pre_season_match.start_date) and \
            game_date <= str2time(schedule.pre_season_match.end_date):
            game_series = "pre_season_match"
        elif game_date >= str2time(schedule.pennant_race.start_date) and \
            game_date <= str2time(schedule.pennant_race.end_date):
            game_series = "pennant_race"
        elif game_date >= str2time(schedule.climax_series.start_date) and \
            game_date <= str2time(schedule.climax_series.end_date):
            game_series = "climax_series"
        elif game_date >= str2time(schedule.nihon_series.start_date) and \
            game_date <= str2time(schedule.nihon_series.end_date):
            game_series = "nihon_series"

        return game_series

    
    def get_game_info(self, html, url):
        soup = bs4.BeautifulSoup(html, "html.parser")

        # 結果を格納する辞書
        result = {}

        # ゲームID
        result["game_id"] = "npb" + url.split("/")[-2]

        # 日付
        game_date = soup.select_one("title").get_text().split(" ")[0]
        result["game_date"] = datetime.datetime.strptime(game_date, "%Y年%m月%d日").strftime("%Y-%m-%d")

        # ステータス
        state_original = soup.select_one('span.bb-gameCard__state').get_text(strip=True)
        if state_original=="試合終了":
            result["game_status"] = "finish"
        elif state_original == "試合中止":
            result["game_status"] = "cancel"
            return result
        elif state_original == "試合前":
            result["game_status"] = "before"
            return result
        else:
            result["game_status"] = "unkown"
            return result

        # ゲームのタイプを判定（オープン戦、ペナント、CS、日シリ）
        result["game_series"] = self.get_game_series(result["game_date"])

        # 球場
        description = soup.select_one("p[class='bb-gameDescription']")
        result["team_top_name"] = description.get_text().split("\n")[3].replace(" ","")

        # 試合開始時間
        description = soup.select_one("p[class='bb-gameDescription']")
        result["game_start_time"] = description.get_text().split("\n")[2].replace(" ","")

        # チーム名
        teams = [x.get_text(strip=True) for x in soup.select("a.bb-gameScoreTable__team")]
        result["team_top_name"] = teams[0]
        result["team_bottom_name"] = teams[1]
        result["team_top_id"] = self.team_dict[teams[0]].team_id
        result["team_bottom_id"] = self.team_dict[teams[1]].team_id

        # 合計点
        scores = [x.get_text(strip=True) for x in soup.select("td[class='bb-gameScoreTable__total']")]
        result["score_top"] = scores[0]
        result["score_bottom"] = scores[1]

        # 勝敗
        if result["score_top"] > result["score_top"]:
            result["game_result"] = "top_win"
        elif result["score_top"] < result["score_top"]:
            result["game_result"] = "bottom_win"
        elif result["score_top"] == result["score_top"]:
            result["game_result"] = "drow"

        # 安打数
        hits = [x.get_text(strip=True) for x in soup.select("td[class='bb-gameScoreTable__total bb-gameScoreTable__data--hits']")]
        result["hit_top"] = hits[0]
        result["hit_bottom"] = hits[1]

        # 失策数
        hits = [x.get_text(strip=True) for x in soup.select("td[class='bb-gameScoreTable__total bb-gameScoreTable__data--loss']")]
        result["error_top"] = hits[0]
        result["error_bottom"] = hits[1]

        # 責任投手
        try:
            soup_pick = soup.select_one("section[id='pit_rec']")
            pitchers = [x for x in soup_pick.select("td.bb-gameTable__data")]
            list_pitcher_ids = []
            for pitcher in pitchers:
                if pitcher.get_text(strip=True) != "":
                    pitcher_url = pitcher.select_one("a[class='bb-gameTable__player']")
                    list_pitcher_ids.append(self.make_id(pitcher_url.get("href").split("/")[-2]))
                else:
                    list_pitcher_ids.append(np.nan)
            result["picher_win_id"] = list_pitcher_ids[0]
            result["picher_lose_id"] = list_pitcher_ids[1]
            result["picher_save_id"] = list_pitcher_ids[2]
        except:
            pass # 引き分け

        # 先発ピッチャー
        soup_pick = soup.select_one("section[id='strt_mem']")
        soup_target_tables = [x for x in soup_pick.select("table.bb-splitsTable")]
        soup_pitcher_top = soup_target_tables[0].select_one("a")
        soup_pitcher_bottom = soup_target_tables[2].select_one("a")
        result["starting_picher_top_id"] = self.make_id(soup_pitcher_top.get("href").split("/")[-2])
        result["starting_picher_bottom_id"] = self.make_id(soup_pitcher_bottom.get("href").split("/")[-2])

        # 審判
        soup_pick = soup.select("section[class='bb-modCommon01']")[-2]
        data = [x.get_text(strip=True) for x in soup_pick.select("td.bb-tableLeft__data")]
        result["umpire_plate"] = data[0]
        result["umpire_first"] = data[1]
        result["umpire_second"] = data[2]
        result["umpire_third"] = data[3]

        # 観客数/試合時間
        soup_pick = soup.select("section[class='bb-modCommon01']")[-1]
        data = [x.get_text(strip=True) for x in soup_pick.select("td.bb-tableLeft__data")]
        result["audience_num"] = data[0]
        result["game_time"] = data[1]

        return result

    def get_games(self, date, output_dir):
        print("start ", date, "="*10)

        # 実行日
        str_datetime = self.exec_datetime.strftime("%Y%m%d%H%M%S")

        # 対象日のゲーム一覧htmlを取得
        url_date_schedule = self.base_url + "/schedule/?date=" + str(date)
        html_date = self.get_html(url_date_schedule)
        soup = bs4.BeautifulSoup(html_date, "html.parser")
        elems_game = soup.select('a.bb-score__content')
        print("there are ", len(elems_game), "games")

        if len(elems_game) == 0:
            "試合がない日はスキップ"
            print("finish ", date, "="*10)
            return None
        else:
            df_game_info_all = pd.DataFrame()
            # 対象日のゲームをループ
            for game in elems_game:
                game_url_tmp = game.get('href')
                game_id_num = re.search("\d+", game_url_tmp).group()
                
                # ゲームのhtmlを取得
                game_url = self.base_url+"/game/"+game_id_num+"/top"
                game_html = self.get_html(game_url)

                # ゲームの結果を取得
                game_info = self.get_game_info(game_html, game_url)
                
                # ゲームのステータスを取得
                game_status = game_info["game_status"]

                # 結果を保存。試合が完了しているときのみ
                if game_status in ["finish", "cancel"]:
                    # file名：ゲーム日_ゲームID_ゲームステータス_実行日
                    file_name = "_".join([date, "g"+game_id_num, game_status, str_datetime])+".html"
                    out_path = os.path.join(output_dir, file_name)

                    if self.output:
                        self.save_html(game_html, out_path)

                    # dfを結合
                    df_game_info_all = df_game_info_all.append(pd.Series(game_info), ignore_index=True)
            
            # 実行日列を追加
            df_game_info_all["exec_datetime"] = self.exec_datetime.strftime("%Y-%m-%d %H:%M:%S")

            # 列順を並び変える
            df_game_info_all = df_game_info_all[self.column["lake_game"]]

            # 結果を出力
            if self.output:
                self.save_csv(df=df_game_info_all, file_path=os.path.join(self.output_game_tsv_path, "lake_game.tsv"))
            else:
                print(df_game_info_all)

            print("finish ", date, "="*10)

            return None

    def get_play_info(self, geme_id):
        
        score_url_start = self.base_url + f"/game/{game_id.replace('npb', '')}/score?index=0110100"
        self.get_html(score_url_start)

    
    def get_player_score():
        pb_params = ["p", "b"]
        df_all = pd.DataFrame() 
        for team in ss.team_list:
            for pb in pb_params:
                target_url = ss.base_url + "/teams/" + str(ss.team_dict[team].id) + "/memberlist?kind=" + pb
                html = ss.get_html(target_url)
                soup = bs4.BeautifulSoup(html, "html.parser")
                tb = soup.find_all('table')[0] 
                df = pd.read_html(str(tb),encoding='utf-8', header=0)[0]
                df = df.drop(len(df)-1) # 最終行を削除
                df['player_id'] = [ss.make_id(row.select_one("a").get("href").split("/")[-2]) for row in tb.select("tr[class='bb-playerTable__row']")]
                df[df == "-"] = np.nan
                df_all = df_all.append(df)

        df_all

    def exec_scraping(self):
        list_date = pd.date_range(start=self.start_date, end=self.end_date)
        for date in list_date:
            self.get_games(
                date=date.strftime("%Y-%m-%d"), 
                output_dir=self.output_game_html_path
                )
        
        return None

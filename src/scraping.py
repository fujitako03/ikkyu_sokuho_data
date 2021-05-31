import datetime
import os
import re
import time

import bs4
import numpy as np
import pandas as pd
import requests

from .db_connection import load_to_bigquery


class ScrapingBase():
    def __init__(self):
        self.exec_datetime = datetime.datetime.now()

    def get_html(self, url, show=False):
        time.sleep(0.1) 

        if show:
            print("get url ", url)

        res = requests.get(url)
        try:
            res.raise_for_status()
        except:
            print("error ", url)
            raise Exception("htmlの取得に失敗しました")
        
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
        self.project_id = os.getenv("PROJECT_ID")
        self.sports = config.exec_sports
        self.upload_flag = config.exec_upload
        self.output_flag = config.exec_output
        self.base_url = config.url_domain + config.exec_sports
        self.output_game_html_path = config.path_output_game_html
        self.output_lake_tsv_path = config.path_output_lake_tsv
        self.start_date = start_date
        self.end_date = end_date
        self.team_dict = config.team
        self.team_list = config.team_list
        self.schedule = config.schedule
        self.table = config.table
    
    def make_id(self, id):
        """スポナビ上のID番からIDを生成する

        Args:
            id (str): [番号]

        Returns:
            [str]:ID
        """
        return self.sports + str(id)

    def check_game_status(self, html):
        soup = bs4.BeautifulSoup(html, "html.parser", from_encoding="utf-8")
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
        """試合の属性や結果を集める

        Args:
            html (str): 試合トップページのhtml
            url (str): 試合のURL

        Returns:
            [type]: [description]
        """
        soup = bs4.BeautifulSoup(html, "html.parser", from_encoding="utf-8")

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
        if result["score_top"] > result["score_bottom"]:
            result["game_result"] = "top_win"
        elif result["score_top"] < result["score_bottom"]:
            result["game_result"] = "bottom_win"
        elif result["score_top"] == result["score_bottom"]:
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
            # 引き分け
            result["picher_win_id"] = np.nan
            result["picher_lose_id"] = np.nan
            result["picher_save_id"] = np.nan

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
        """対象日の全試合のデータを集める

        Args:
            date (datetime): データ収集する日
            output_dir (str): 出力先のpath

        Returns:
            [type]: [description]
        """
        print("start ", date, "="*10)

        # 実行日
        str_datetime = self.exec_datetime.strftime("%Y%m%d%H%M%S")

        # 対象日のゲーム一覧htmlを取得
        url_date_schedule = self.base_url + "/schedule/?date=" + str(date)
        html_date = self.get_html(url_date_schedule)
        soup = bs4.BeautifulSoup(html_date, "html.parser", from_encoding="utf-8")
        elems_game = soup.select('a.bb-score__content')
        print("there are ", len(elems_game), "games")

        if len(elems_game) == 0:
            # 試合がない日はスキップ
            print("finish ", date, "="*10)
            return None
        else:
            df_game_info_all = pd.DataFrame()
            df_score_info_all = pd.DataFrame()
            df_ball_info_all = pd.DataFrame()
            # 対象日のゲームをループ
            for i, game in enumerate(elems_game):
                print(i+1, " / ", len(elems_game))
                game_url_tmp = game.get('href')
                game_id_num = re.search("\d+", game_url_tmp).group()
                
                # ゲームのhtmlを取得
                game_url = self.base_url+"/game/"+game_id_num+"/top"
                game_html = self.get_html(game_url)

                # ゲームの結果を取得
                df_game_info = self.get_game_info(game_html, game_url)
                
                # 結果を保存。試合が完了しているときのみ
                if df_game_info["game_status"] == "finish":
                    # 速報ページのスコアを取得
                    df_score_info, df_ball_info = self.get_score_info(self.make_id(game_id_num))

                    # file名：ゲーム日_ゲームID_ゲームステータス_実行日
                    file_name = "_".join([date, "g"+game_id_num, df_game_info["game_status"], str_datetime])+".html"
                    out_path = os.path.join(output_dir, file_name)

                    # 出力
                    if self.output_flag:
                        self.save_html(game_html, out_path)

                    # dfを結合
                    df_game_info_all = df_game_info_all.append(pd.Series(df_game_info), ignore_index=True)
                    df_score_info_all = df_score_info_all.append(df_score_info, ignore_index=True)
                    df_ball_info_all = df_ball_info_all.append(df_ball_info, ignore_index=True)

            
            # 実行日列を追加
            df_game_info_all["exec_datetime"] = self.exec_datetime.strftime("%Y-%m-%d %H:%M:%S")
            df_score_info_all["exec_datetime"] = self.exec_datetime.strftime("%Y-%m-%d %H:%M:%S")
            df_ball_info_all["exec_datetime"] = self.exec_datetime.strftime("%Y-%m-%d %H:%M:%S")

            # 列順を並び変える
            df_game_info_all = df_game_info_all[[c.name for c in self.table.lake_game.column]]
            df_score_info_all = df_score_info_all[[c.name for c in self.table.lake_score.column]]
            df_ball_info_all = df_ball_info_all[[c.name for c in self.table.lake_ball.column]]

            # 結果を出力
            if self.output_flag:
                self.save_csv(df=df_game_info_all, file_path=os.path.join(self.output_lake_tsv_path, "lake_game.tsv"))
                self.save_csv(df=df_score_info_all, file_path=os.path.join(self.output_lake_tsv_path, "lake_score.tsv"))
                self.save_csv(df=df_ball_info_all, file_path=os.path.join(self.output_lake_tsv_path, "lake_ball.tsv"))
            else:
                # print(df_game_info_all)
                pass
            
            # bigqueryにテーブル作成
            if self.upload_flag:
                # game
                load_to_bigquery(
                    df_game_info_all, 
                    project_id=self.project_id,
                    db_name=self.table.lake_game.db_name,
                    table_name=self.table.lake_game.table_name,
                    schema_dict=self.table.lake_game.column,
                    if_exists="append",
                    )

                # score
                load_to_bigquery(
                    df_score_info_all, 
                    project_id=self.project_id,
                    db_name=self.table.lake_score.db_name,
                    table_name=self.table.lake_score.table_name,
                    schema_dict=self.table.lake_score.column,
                    if_exists="append",
                    )

                # ball
                load_to_bigquery(
                    df_ball_info_all, 
                    project_id=self.project_id,
                    db_name=self.table.lake_ball.db_name,
                    table_name=self.table.lake_ball.table_name,
                    schema_dict=self.table.lake_ball.column,
                    if_exists="append",
                    )

            print("finish ", date, "="*10)

            return None

    def get_score_info(self, game_id):
        score_url_base = self.base_url + f"/game/{game_id.replace('npb', '')}/score?index="
        param_index = "0110100"
        score_url = score_url_base + param_index

        df_score_info = pd.DataFrame()
        df_ball_info = pd.DataFrame()
        game_continue = True
        while game_continue:
            html = self.get_html(score_url)
            soup = bs4.BeautifulSoup(html, "html.parser", from_encoding="utf-8")

            result = {}
            # game_id, index
            result["game_id"] = game_id
            result["index"] = param_index # 2週目以降は上書きされたindex

            # 試合進行状況を取得
            inning_text = soup.select_one("div[id='sbo']").select_one("em").get_text(strip=True)
            result["inning"] = inning_text[0]
            result["top_buttom"] = "top" if inning_text[-1] == "表" else "bottom"

            # 試合終了判定
            if inning_text == "試合終了":
                game_continue = False
                break

            # バッター情報を取得
            soup_batter = soup.select_one("table[id='batt']")
            if soup_batter is not None:
                result["batter_id"] = self.make_id(soup_batter.select_one("a").get("href").split("/")[-2])
                result["batter_side"] = soup_batter.select_one("td.dominantHand").get_text()
            else:
                # 代打のとき
                result["batter_id"] = np.nan
                result["batter_side"] = np.nan

            # ピッチャー情報を取得
            soup_pitcher = soup.select_one("div[id='pitcherL']")
            if soup_pitcher is None:
                soup_pitcher = soup.select_one("div[id='pitcherR']")
            result["pitcher_id"] = self.make_id(soup_pitcher.select_one("a").get("href").split("/")[-2])
            result["pitcher_side"] = soup_pitcher.select_one("td.dominantHand").get_text()

            # 結果を取得
            soup_res_main = soup.select_one("div[id='result']").select_one("span")
            result["result_main"] = soup_res_main.get_text() if soup_res_main is not None else np.nan
            soup_res_sub = soup.select_one("div[id='result']").select_one("em")
            result["result_sub"] = soup_res_sub.get_text() if soup_res_sub is not None else np.nan

            # 走者情報
            for i in range(1, 4):
                base = soup.select_one(f"div[id='base{i}']")
                result[f"base_{i}"] = base.get_text() if base is not None else np.nan

            # 打球情報
            dakyu = soup.select_one(f"div[id='dakyu']")
            result[f"dakyu"] = dakyu.get("class")[0] if dakyu.get("class") is not None else np.nan

            # 1球ごとの情報
            df_ball = self.get_ball_info(soup, game_id=game_id, index=param_index)
            df_ball_info = df_ball_info.append(df_ball)

            if df_ball_info["game_id"].isnull().sum() > 0:
                print("stop")

            # dfを結合
            df_score_info = df_score_info.append(pd.Series(result), ignore_index=True)

            # 次のループへ
            param_index = soup.select_one("a[id='btn_next']").get("index") # indexを上書き
            score_url = score_url_base + param_index # urlを上書き
        
        return df_score_info, df_ball_info
    

    def get_ball_info(self, soup, game_id, index):
        # 球種
        soup_select = soup.select("td[class='bb-splitsTable__data bb-splitsTable__data--ballType']")
        balltype_list = [s.get_text(strip=True) for s in soup_select]

        # 球速
        soup_select = soup.select("td[class='bb-splitsTable__data bb-splitsTable__data--speed']")
        speed_list_pre = [s.get_text(strip=True).replace("km/h", "").replace("-", "") for s in soup_select]
        speed_list = [float(x) if x!="" else np.nan for x in speed_list_pre]

        # 結果
        soup_select = soup.select("td[class='bb-splitsTable__data bb-splitsTable__data--result']")
        result_list = [s.get_text(strip=True) for s in soup_select]

        # 場所
        soup_select = soup.select_one("div[class='bb-allocationChart']")
        soup_ball_list = soup_select.select("span.bb-icon__ballCircle")
        position_top_list = []
        position_left_list = []
        for soup_ball in soup_ball_list:
            style_text = soup_ball.get("style")
            numbers = re.findall("[0-9]+", style_text)
            position_top_list.append(float(numbers[0]))
            position_left_list.append(float(numbers[1]))

        df_output = pd.DataFrame()
        num_ball = len(result_list)
        if num_ball > 0:
            df_output["game_id"] = np.repeat(game_id, num_ball)
            df_output["index"] = np.repeat(index, num_ball)
            df_output["number"] = range(1, num_ball+1)
            df_output["balltype"] = balltype_list
            df_output["speed"] = speed_list
            df_output["result"] = result_list
            df_output["position_top"] = position_top_list
            df_output["position_left"] = position_left_list

        return df_output

    
    def get_player_info(self, html):

        soup = bs4.BeautifulSoup(html, "html.parser", from_encoding="utf-8")

        result = {}
        soup_name = soup.select_one("ruby[class='bb-profile__name']")
        result["player_name"] = soup_name.select_one("h1").get_text()

        soup_kana = soup_name.select_one("rt")
        result["player_name_kana"] = soup_kana.get_text()[1:-1] if soup_kana is not None else np.nan
        result["number"] = soup.select_one("p.bb-profile__number").get_text()

        list_title = [x.get_text() for x in soup.select("dt[class='bb-profile__title']")]
        list_text = [x.get_text() for x in soup.select("dd[class='bb-profile__text']")]

        # 情報を辞書化
        profile_dict = {}
        for i in range(len(list_text)):
            profile_dict[list_title[i]] = list_text[i]
        
        # keyを列名に修正
        result["birth_day"] = profile_dict.get("生年月日（満年齢）")
        result["birth_place"] = profile_dict.get("出身地")
        result["height"] = profile_dict.get("身長")
        result["weight"] = profile_dict.get("体重")
        result["blood_type"] = profile_dict.get("血液型")
        result["throw_batting"] = profile_dict.get("投打")
        result["draft_year"] = profile_dict.get("ドラフト年（順位）")
        result["pro_age"] = profile_dict.get("プロ通算年")
        result["keireki"] = profile_dict.get("経歴")
        result["profile_text"] = soup.select_one("p[class='bb-profile__summary']").get_text()
        result["img_url"] = soup.select_one("div[class='bb-profile__photo']").select_one("img").get("src")

        return result
    
    def get_players(self):
        df_player_info = pd.DataFrame()
        # チームの選手ページをループ
        for team in self.team_list:
            for kind in ["p", "b"]:
                team_info = self.team_dict[team]
                players_url = self.base_url + "/teams/" + team_info.team_id.replace("npb", "") + f"/memberlist?kind={kind}"
                players_html = self.get_html(players_url)
                soup = bs4.BeautifulSoup(players_html, "html.parser", from_encoding="utf-8")
                list_players_pre = soup.select("td[class='bb-playerTable__data bb-playerTable__data--player']")
                list_players = [x.select_one("a").get("href").split("/")[-2] for x in list_players_pre]

                # 順に選手情報を取得
                for player_num in list_players:
                    player_url = f"https://baseball.yahoo.co.jp/npb/player/{player_num}/top"
                    player_html = self.get_html(player_url)
                    result = self.get_player_info(player_html)
                    result["player_id"] = self.make_id(player_num)

                    # dfを結合
                    df_player_info = df_player_info.append(pd.Series(result), ignore_index=True)

        df_player_info["exec_datetime"] = self.exec_datetime.strftime("%Y-%m-%d %H:%M:%S")
        df_player_info = df_player_info[[c.name for c in self.table.lake_player.column]]

        self.save_csv(df_player_info, file_path=os.path.join(self.output_lake_tsv_path, "lake_player.tsv"))

        return None


    def get_player_score():
        pb_params = ["p", "b"]
        df_all = pd.DataFrame() 
        for team in ss.team_list:
            for pb in pb_params:
                target_url = ss.base_url + "/teams/" + str(ss.team_dict[team].id) + "/memberlist?kind=" + pb
                html = ss.get_html(target_url)
                soup = bs4.BeautifulSoup(html, "html.parser", from_encoding="utf-8")
                tb = soup.find_all('table')[0] 
                df = pd.read_html(str(tb),encoding='utf-8', header=0)[0]
                df = df.drop(len(df)-1) # 最終行を削除
                df['player_id'] = [ss.make_id(row.select_one("a").get("href").split("/")[-2]) for row in tb.select("tr[class='bb-playerTable__row']")]
                df[df == "-"] = np.nan
                df_all = df_all.append(df)

        return df_all

    def exec_score_scraping(self):
        list_date = pd.date_range(start=self.start_date, end=self.end_date)
        for date in list_date:
            self.get_games(
                date=date.strftime("%Y-%m-%d"), 
                output_dir=self.output_game_html_path
                )
        
        return None

    def exec_player_scraping(self):
        self.get_players()
        
        return None


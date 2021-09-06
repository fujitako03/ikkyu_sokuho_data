import datetime
import os
import re

import pandas as pd
from omegaconf.listconfig import ListConfig

from config import ConfigALL, ConfigTeam
from db_connection import load_to_bigquery
from scraping_base import ScrapingBase


class ScrapingGame(ScrapingBase):
    def __init__(
        self,
        project_id: str,
        flow_id: str,
        conf: ListConfig,
        league: str,
        exec_date: str,
        sleep_sec: int=1) -> None:
        super().__init__(sleep_sec=sleep_sec)
        self.project_id = project_id
        self.flow_id = flow_id
        self.league = league
        self.conf = conf
        self.exec_date = exec_date

    def get_game_numbers(self):
        # urlベース
        url_base = f"{self.conf.url_domain}/{self.league}/schedule/"

        # 対象日のゲーム一覧htmlを取得
        url_schedule = f"{url_base}?date={self.exec_date}"
        soup = self.get_soup_from_url(url_schedule)
        game_cards = soup.select('a.bb-score__content')

        if len(game_cards) == 0:
            print("試合がありません")
            return None
        else:
            game_number_list = [
                re.search("\d+", game.get('href')).group() 
                for game 
                in game_cards]
            
            return game_number_list

    def get_game_series(self, game_date_str):
        """日付から試合の種類を判定

        Args:
            game_date_str ([type]): [description]
        """
        def str2time(date_str):
            return datetime.datetime.strptime(date_str, "%Y-%m-%d")

        game_date = str2time(game_date_str)
        schedule = self.conf.schedule["year_"+str(game_date.year)]

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
    def get_game_info(self, game_number):
        """試合の属性や結果を集める

        Args:
            html (str): 試合トップページのhtml
            url (str): 試合のURL

        Returns:
            [type]: [description]
        """
        # url
        url = f"{self.conf.url_domain}/{self.league}/game/{game_number}/top"
        soup = self.get_soup_from_url(url)

        # 結果を格納する辞書
        result = {}

        # ゲームID
        result["game_id"] = "npb" + url.split("/")[-2]

        # 日付
        game_date = soup.select_one("title").get_text().split(" ")[0]
        result["game_date"] = datetime.datetime.strptime(game_date, "%Y年%m月%d日").strftime("%Y-%m-%d")

        # ゲーム名
        game_title = soup.select_one("h1.bb-head01__title").select_one("span").get_text()
        game_round = soup.select_one("p.bb-gameRound").get_text()
        result["game_title"] = game_title + " " + game_round

        # ゲームのタイプを判定（オープン戦、ペナント、CS、日シリ）
        result["game_series"] = self.get_game_series(result["game_date"])

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
        config_team = ConfigTeam(self.conf, self.league)
        result["team_top_id"] = config_team.get_team_id_from_name(teams[0])
        result["team_bottom_id"] = config_team.get_team_id_from_name(teams[1])

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
                    list_pitcher_ids.append(self.league + pitcher_url.get("href").split("/")[-2])
                else:
                    list_pitcher_ids.append(None)
            result["picher_win_id"] = list_pitcher_ids[0]
            result["picher_lose_id"] = list_pitcher_ids[1]
            result["picher_save_id"] = list_pitcher_ids[2]
        except:
            # 引き分け
            result["picher_win_id"] = None
            result["picher_lose_id"] = None
            result["picher_save_id"] = None

        # 先発ピッチャー
        soup_pick = soup.select_one("section[id='strt_mem']")
        soup_target_tables = [x for x in soup_pick.select("table.bb-splitsTable")]
        soup_pitcher_top = soup_target_tables[0].select_one("a")
        soup_pitcher_bottom = soup_target_tables[2].select_one("a")
        result["starting_picher_top_id"] = self.league + soup_pitcher_top.get("href").split("/")[-2]
        result["starting_picher_bottom_id"] = self.league + soup_pitcher_bottom.get("href").split("/")[-2]

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

        # フローID
        result["flow_id"] = self.flow_id

        return result

    def upload(self, df_game_info):
        # flow_idを追加
        df_game_info["flow_id"] = self.flow_id

        # 列順を並び替える
        cols = [c.name for c in self.conf.table.lake_game.column]
        df_game_info = df_game_info[cols]

        # upload
        load_to_bigquery(
                df=df_game_info, 
                project_id=self.project_id,
                db_name=self.conf.table.lake_game.db_name,
                table_name=self.conf.table.lake_game.table_name,
                schema_dict=self.conf.table.lake_game.column,
                if_exists="append",
                )

    def main(self):
        # 指定日の試合番号リストを取得
        game_number_list = self.get_game_numbers()

        # 試合を順に処理
        df_game_info = pd.DataFrame()
        if game_number_list is None:
            return None
        else:
            for game_number in game_number_list:
                # 試合結果と概要を取得
                result = self.get_game_info(game_number)
                df_game_info = df_game_info.append(
                    pd.Series(result),
                    ignore_index=True)
        
        # アップロード
        self.upload(df_game_info)

if __name__=='__main__':
    league = "npb"
    # 環境変数から読み込む
    project_id = os.getenv("PROJECT_ID")

    # flow_id
    flow_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # 実行対象日
    exec_date = "2021-09-04"

    # load_config 
    config_all = ConfigALL()
    conf = config_all.load_all_config()

    scraping_game = ScrapingGame(
        conf=conf, 
        project_id=project_id, 
        flow_id=flow_id,
        league=league,
        exec_date=exec_date)

    scraping_game.main()


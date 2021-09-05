import datetime
import os

import pandas as pd
from omegaconf.listconfig import ListConfig

from config import ConfigALL, ConfigTeam
from db_connection import load_to_bigquery
from scraping_base import ScrapingBase


class ScrapingPlayer(ScrapingBase):
    def __init__(
        self,
        project_id: str,
        flow_id: str,
        conf: ListConfig,
        league: str,
        sleep_sec: int=1) -> None:
        super().__init__(sleep_sec=sleep_sec)
        self.project_id = project_id
        self.flow_id = flow_id
        self.league = league
        self.conf = conf
    
    def get_team_players(self, sponavi_team_id: str):
        # urlベース
        url_base = f"{self.conf.url_domain}/{self.league}/teams/{sponavi_team_id}/memberlist"

        # player_list
        player_list_all = []

        # 投手、打者順に一覧を取得
        for kind in ["p", "b"]:
            # picher=p, batter=b
            url = f"{url_base}?kind={kind}"
            soup = self.get_soup_from_url(url)
            player_list_pre = soup.select("td[class='bb-playerTable__data bb-playerTable__data--player']")
            player_list = [x.select_one("a").get("href").split("/")[-2] for x in player_list_pre]
            player_list_all.extend(player_list)
        
        return player_list_all
    
    def get_player_info(self, player_id):
        result = {}
        url = f"{self.conf.url_domain}/{self.league}/player/{player_id}/top"

        # urlからbs4オブジェクトを取得
        soup = self.get_soup_from_url(url)
        
        # 選手名
        soup_name = soup.select_one("ruby[class='bb-profile__name']")
        result["player_name"] = soup_name.select_one("h1").get_text()

        # 選手名（かな）
        soup_kana = soup_name.select_one("rt")
        result["player_name_kana"] = soup_kana.get_text()[1:-1] if soup_kana is not None else None


        # 背番号
        result["number"] = soup.select_one("p.bb-profile__number").get_text()

        # プロフィール表を辞書型に加工
        list_title = [x.get_text() for x in soup.select("dt[class='bb-profile__title']")]
        list_text = [x.get_text() for x in soup.select("dd[class='bb-profile__text']")]
        profile_dict = {}
        for i in range(len(list_text)):
            profile_dict[list_title[i]] = list_text[i]
        
        # プロフィール表を順に取得
        birth_day_str = profile_dict.get("生年月日（満年齢）")[:-5]
        result["birth_day"] = datetime.datetime.strptime(birth_day_str, "%Y年%m月%d日").date()
        result["birth_place"] = profile_dict.get("出身地")
        result["height"] = int(profile_dict.get("身長").replace("cm", ""))
        result["weight"] = int(profile_dict.get("体重").replace("kg", ""))
        result["blood_type"] = profile_dict.get("血液型")
        result["throw_batting"] = profile_dict.get("投打")
        result["draft_year"] = profile_dict.get("ドラフト年（順位）")
        result["pro_age"] = profile_dict.get("プロ通算年").replace("年", "")
        result["keireki"] = profile_dict.get("経歴")
        result["profile_text"] = soup.select_one("p[class='bb-profile__summary']").get_text()
        result["img_url"] = soup.select_one("div[class='bb-profile__photo']").select_one("img").get("src")

        # player_id
        result["player_id"] = self.league + player_id

        return result
    
    def upload(self, df_player_info):
        # flow_idを追加
        df_player_info["flow_id"] = self.flow_id

        # 列順を並び替える
        cols = [c.name for c in self.conf.table.lake_player.column]
        df_player_info = df_player_info[cols]

        # upload
        load_to_bigquery(
                df=df_player_info, 
                project_id=self.project_id,
                db_name=self.conf.table.lake_player.db_name,
                table_name=self.conf.table.lake_player.table_name,
                schema_dict=self.conf.table.lake_player.column,
                if_exists="append",
                )


    def main(self):
        # sponavi_team_idのリストを取得
        config_team = ConfigTeam(
            conf=self.conf,
            league=self.league)
        sponavi_team_id_list = config_team.get_sponavi_team_id_list()

        # loopでデータ取得
        df_player_info = pd.DataFrame()
        for sponavi_team_id in sponavi_team_id_list:
            # 選手id一覧を取得
            player_list = self.get_team_players(sponavi_team_id=sponavi_team_id)

            # 選手idごとに選手情報を取得
            for player_id in player_list:
                result = self.get_player_info(player_id)
                df_player_info = df_player_info.append(
                    pd.Series(result),
                    ignore_index=True)
        
        # big queryへupload
        self.upload(df_player_info)


if __name__=='__main__':
    league = "npb"
    # 環境変数から読み込む
    project_id = os.getenv("PROJECT_ID")

    flow_id = datetime.datetime.now().strftime("%Y%m%d_%h%M%s")

    # load_config 
    config_all = ConfigALL()
    conf = config_all.load_all_config()

    scraping_player = ScrapingPlayer(
        conf=conf, 
        project_id=project_id, 
        flow_id=flow_id,
        league=league)

    scraping_player.main()


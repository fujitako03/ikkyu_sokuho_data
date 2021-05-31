# import argparse
import base64
import datetime
import json
import os

from omegaconf import OmegaConf

from src.line_notify import send_line_message
from src.scraping import ScrapingSponavi


def main(event, contexts):
    # env 
    APP_ENV = os.getenv("APP_ENV")

    # 実行時刻
    exec_datetime = datetime.datetime.now()

    # conf ファイルの読み込み
    conf_dir = "config"
    conf_exec = OmegaConf.load(os.path.join(conf_dir, "config_exec.yaml"))
    conf_path = OmegaConf.load(os.path.join(conf_dir, "config_path.yaml"))
    conf_url = OmegaConf.load(os.path.join(conf_dir, "config_url.yaml"))
    conf_team = OmegaConf.load(os.path.join(conf_dir, "config_team.yaml"))
    conf_schedule = OmegaConf.load(os.path.join(conf_dir, "config_schedule.yaml"))
    conf_table = OmegaConf.load(os.path.join(conf_dir, "config_table.yaml"))
    if APP_ENV == "dev":
        conf_cli = OmegaConf.from_cli()
        conf_merge = OmegaConf.merge(conf_cli, conf_exec, conf_path, conf_url, conf_team, conf_schedule, conf_table)
    elif APP_ENV =="prd":
        conf_merge = OmegaConf.merge(conf_exec, conf_path, conf_url, conf_team, conf_schedule, conf_table)
        # pub/subのメッセージをパースする
        messages   = json.loads(base64.b64decode(event['data']).decode('ascii'))
        conf_merge.start_date = messages['start_date']
        conf_merge.end_date = messages['end_date']
    else:
        pass
    
    # 開始通知をLINEに送る
    exec_config_str = f"環境：{APP_ENV} 対象日：{conf_merge.start_date} 実行日時：{exec_datetime.strftime('%y-%m-%d %H:%M:%S')}"
    send_line_message("開始! " + exec_config_str)
    print("開始! " + exec_config_str)


    # スクレイピングモジュールのインスタンス
    ss = ScrapingSponavi(
        start_date=conf_merge.start_date,
        end_date=conf_merge.end_date,
        config=conf_merge
        )

    # 試合データのスクレイピング
    if conf_merge.exec_run_score:
        ss.exec_score_scraping()

    # 選手情報のスクレイピング
    if conf_merge.exec_run_player:
        ss.exec_player_scraping()
    
    # 終了通知をLINEで送る
    send_line_message("完了！ " + exec_config_str)
    print("完了！ " + exec_config_str)


if __name__=='__main__':
    # env 
    APP_ENV = os.getenv("APP_ENV")

    # 実行環境
    if APP_ENV == "dev":
        event = ""
        contexts = ""
    elif APP_ENV =="prd":
        pass

    print(f"start at {APP_ENV} ================== ")

    main(event, contexts)

# import argparse
import datetime
import os

from omegaconf import OmegaConf

from src.scraping import ScrapingSponavi

now_datetime = datetime.datetime.now()

# 出力先
conf_dir = "config"
conf_cli = OmegaConf.from_cli()
conf_exec = OmegaConf.load(os.path.join(conf_dir, "config_exec.yaml"))
conf_path = OmegaConf.load(os.path.join(conf_dir, "config_path.yaml"))
conf_url = OmegaConf.load(os.path.join(conf_dir, "config_url.yaml"))
conf_team = OmegaConf.load(os.path.join(conf_dir, "config_team.yaml"))
conf_schedule = OmegaConf.load(os.path.join(conf_dir, "config_schedule.yaml"))
conf_table = OmegaConf.load(os.path.join(conf_dir, "config_table.yaml"))
conf_merge = OmegaConf.merge(conf_cli, conf_exec, conf_path, conf_url, conf_team, conf_schedule, conf_table)

# スクレイピング
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

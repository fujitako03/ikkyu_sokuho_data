# import argparse
import datetime
import os

from omegaconf import OmegaConf

from src.scraping import ScrapingSponavi

now_datetime = datetime.datetime.now()

# 引数
# parser = argparse.ArgumentParser(description='get Sponavi data')
# parser.add_argument('-s', '--start',  help='start date')
# parser.add_argument('-e', '--end', help='end date')
# args = parser.parse_args()

# if args.start == args.start and args.end == args.end:
#     start_date = args.start
#     end_date = args.end
# else:
#     start_date = now_datetime.strftime("%Y-%m-%d")
#     end_date = now_datetime.strftime("%Y-%m-%d")


# 出力先
conf_dir = "config"
conf_cli = OmegaConf.from_cli()
conf_exec = OmegaConf.load(os.path.join(conf_dir, "config_exec.yaml"))
conf_path = OmegaConf.load(os.path.join(conf_dir, "config_path.yaml"))
conf_url = OmegaConf.load(os.path.join(conf_dir, "config_url.yaml"))
conf_team = OmegaConf.load(os.path.join(conf_dir, "config_team.yaml"))
conf_merge = OmegaConf.merge(conf_cli, conf_exec, conf_path, conf_url, conf_team)

# スクレイピング
ss = ScrapingSponavi(
    start_date=conf_merge.start_date,
    end_date=conf_merge.end_date,
    config=conf_merge
    )

# 実行
ss.exec_scraping()

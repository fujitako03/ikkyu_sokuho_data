import argparse
import datetime

from src.scraping import ScrapingSponavi

now_datetime = datetime.datetime.now()

# 引数
parser = argparse.ArgumentParser(description='get Sponavi data')
parser.add_argument('-s', '--start',  help='start date')
parser.add_argument('-e', '--end', help='end date')
args = parser.parse_args()

if args.start == args.start and args.end == args.end:
    start_date = args.start
    end_date = args.end
else:
    start_date = now_datetime.strftime("%Y-%m-%d")
    end_date = now_datetime.strftime("%Y-%m-%d")

# path変数
path_game_html = "data/html/games"

# スクレイピング
ss = ScrapingSponavi(
    start_date=args.start,
    end_date=args.end)

# 実行
ss.exec_scraping(output_dir=path_game_html)

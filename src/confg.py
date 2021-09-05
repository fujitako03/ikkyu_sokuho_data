import os

from omegaconf import OmegaConf
from omegaconf.listconfig import ListConfig


class ConfigALL:
    def __init__(self) -> None:
        self.config_dir = "./config"

    def load_all_config(self) -> ListConfig:
        """全てのyamlファイルを読み込み、まとめてconfigとして出力する

        Returns:
            ListConfig: 結合したconfig
        """
        config_path_list = os.listdir(self.config_dir)
        conf_merge = OmegaConf.create()
        for config_path in config_path_list:
            conf = OmegaConf.load(os.path.join(self.config_dir, config_path))
            conf_merge = OmegaConf.merge(conf_merge, conf)
        
        return conf_merge


class ConfigTeam:
    def __init__(self, league, conf) -> None:
        self.conf = conf
        self.league = league
    
    def get_sponavi_team_id_list(self) -> list:
        """configからteam_idのリストを取得

        Returns:
            list(int): チームIDリスト
        """
        team_list = self.conf.team.get(self.league)
        return [t.sponavi_team_id for t in team_list]


if __name__=='__main__':
    config_all = ConfigALL()
    conf = config_all.load_all_config()

    config_team = ConfigTeam(
        league="npb",
        conf=conf)

    team_list = config_team.get_sponavi_team_id_list()
    print(team_list)




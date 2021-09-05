import os

from omegaconf import OmegaConf


class Team:
    def __init__(self, league, conf) -> None:
        self.conf = conf
        self.league = league

    def get_sponavi_team_id_list(self):
        """configからteam_idのリストを取得

        Returns:
            list(int): チームIDリスト
        """
        team_list = self.conf.npb
        return [t.sponavi_team_id for t in team_list]


if __name__=='__main__':
    league = "npb"
    conf_team = OmegaConf.load("./config/config_team.yaml")
    team = Team(
        league=league,
        conf=conf_team)

    team_list = team.get_sponavi_team_id_list()
    print(team_list)




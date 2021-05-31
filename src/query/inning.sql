with newest as (
    select
        game_id,
        max(exec_datetime) as exec_datetime
    from
        `baseball-analytics-311804.sponavi_lake.score` s
    group by
        1
)
,t1 as (
    select
        g.game_id,
        g.game_date,
        s.inning,
        s.top_buttom,
        case
            when s.top_buttom = 'top' then g.team_top_id
            when s.top_buttom = 'bottom' then g.team_bottom_id
            else null end as team_id,
        case
            when s.top_buttom = 'top' then g.team_top_name
            when s.top_buttom = 'bottom' then g.team_bottom_name
            else null end as team_name,
        coalesce(
            cast(
                regexp_extract(
                    REGEXP_EXTRACT(s.result_main, '＋[0-9]点'),
                    '[0-9]+'
            )
        as INT64),0) as score
from 
    `baseball-analytics-311804.sponavi_lake.score` as s
    inner join newest as n
        on s.game_id = n.game_id
        and s.exec_datetime = n.exec_datetime
    left join `baseball-analytics-311804.sponavi_lake.game` as g
        on s.game_id = g.game_id
)
select
    game_id,
    g.game_date,
    inning,
    top_buttom,
    team_id,
    team_name,
    sum(score) as score
from
    t1
group by
    1,2,3,4,5
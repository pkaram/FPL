--review team fitness and player availability
with teams_stats as (
  select *
  from teams
  where date_loaded in (select max(date_loaded) from teams)
),

player_stats as (
  SELECT second_name,
    team_code,
    influence,
    creativity,
    threat,
    ict_index,
    case when player_type.singular_name='Defender' then 1 else 0 end as defender,
    case when player_type.singular_name='Midfielder' then 1 else 0 end as midfielder,
    case when player_type.singular_name='Goalkeeper' then 1 else 0 end as goalkeeper,
    case when player_type.singular_name='Forward' then 1 else 0 end as forward
    from players
    join player_type on player_type.id=players.element_type
    where date_loaded in (select max(date_loaded) from players)
),

recent_news as (
    select
  news.team,
  sum(case when news.chance_of_playing_next_round=0 then 1 else 0 end) as players_out,
  sum(case when news.chance_of_playing_next_round!=0 then 1 else 0 end) as players_likely_out
  from news
  where news.date_loaded in (select max(date_loaded) from news)
  group by news.team
)

select
base_data.*,
recent_news.players_out,
recent_news.players_likely_out

from
(
select
date_loaded,
teams_stats.name as team_name,
base_data.
teams_stats.strength_overall_home,
teams_stats.strength_overall_away,
avg(player_stats.influence) as influence,
avg(player_stats.creativity) as creativity,
avg(player_stats.threat) as threat,
avg(player_stats.ict_index) as ict_index,
count(*) as number_of_players,
sum(player_stats.defender) as defender_cnt,
sum(player_stats.midfielder) as midfielder_cnt,
sum(player_stats.goalkeeper) as goalkeeper_cnt,
sum(player_stats.forward) as forward_cnt
from teams_stats
 left  join player_stats on player_stats.team_code=teams_stats.code
group by teams_stats.name,
points,
position,
form,
strength
) base_data
left join recent_news on recent_news.team=base_data.team_name
order by base_data.ict_index desc

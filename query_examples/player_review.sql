  --review player performance
  SELECT second_name,
    teams.name,
    influence,
    creativity,
    threat,
    ict_index,
	minutes,
	now_cost/10 as cost,
    player_type.singular_name as position
    from players
    join player_type on player_type.id=players.element_type
	join teams on teams.code=players.team_code
    where players.date_loaded in (select max(date_loaded) from players)
	and teams.date_loaded in (select max(date_loaded) from teams)
    order by cast(ict_index as float) desc
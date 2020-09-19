  --review player performance

  SELECT second_name,
    team_code,
    influence,
    creativity,
    threat,
    ict_index,
	minutes,
	now_cost/10 as cost,
    case when player_type.singular_name='Defender' then 1 else 0 end as defender,
    case when player_type.singular_name='Midfielder' then 1 else 0 end as midfielder,
    case when player_type.singular_name='Goalkeeper' then 1 else 0 end as goalkeeper,
    case when player_type.singular_name='Forward' then 1 else 0 end as forward
    from players
    join player_type on player_type.id=players.element_type
    where date_loaded in (select max(date_loaded) from players)
    order by cast(ict_index as float) desc
#cluster players based on performance metrics
import sqlite3
from sklearn.cluster import KMeans
import pandas as pd
from connection_utils import get_data_from_db
from process_utils import prepare_data_for_clustering, perform_kmeans, write_cluster_results

def main():
    query = """
    with teams_subset as (
    select distinct code,name as team_name from teams
    )
    
    select 
    players.*,
    player_type.singular_name,
    teams_subset.team_name as team_name
    from players
    join player_type on players.element_type=player_type.id
    join teams_subset on teams_subset.code=players.team_code
    where players.date_loaded in (select max(date_loaded) as max_date from players)  
    and player_type.singular_name !='Goalkeeper'              
    """
    players = get_data_from_db(query)
    players = prepare_data_for_clustering(players)
    clustering_cols = ['influence','creativity','threat','clean_sheets','points_per_game','minutes']
    players = perform_kmeans(players, clustering_cols)
    write_cluster_results(players)

if __name__ == '__main__':
    main()
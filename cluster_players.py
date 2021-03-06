#cluster players based on performance metrics
import sqlite3
from sklearn.cluster import KMeans
import pandas as pd

def main():
    #connect to the database
    cnx = sqlite3.connect('{add your path}/fpl_data.db')

    #load most recent info on players
    players=pd.read_sql("""
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
    """, cnx)

    #drop Goalkeepers out of clustering
    #players_clustering=players[players['singular_name']!='Goalkeeper']
    #drop out players that have not played (this part can be modified as minutes represent total minutes played)
    players_clustering=players[players['minutes']!=0]
    players_clustering=players_clustering.reset_index(drop=True)

    #change data structure for clustering
    players_clustering['influence']=[float(s) for s in players_clustering['influence']]
    players_clustering['creativity']=[float(s) for s in players_clustering['creativity']]
    players_clustering['threat']=[float(s) for s in players_clustering['threat']]
    players_clustering['points_per_game']=[float(s) for s in players_clustering['points_per_game']]

    #select data to perform kmeans clustering on
    data_kmeans=players_clustering[['influence','creativity','threat','clean_sheets','points_per_game','minutes']]

    #as an example 4 clusters will be used
    kmeans=KMeans(n_clusters=4).fit(data_kmeans)
    #add labels of clusters on the dataframe
    players_clustering['cluster_label']=kmeans.labels_
    players_clustering['now_cost']=players_clustering['now_cost'].astype(float)
    players_clustering['now_cost']=players_clustering['now_cost']/10
    players_clustering['ict_index']=players_clustering['ict_index'].astype(float)

    players_clustering=players_clustering[['singular_name','team_name','second_name','now_cost','points_per_game','minutes','cluster_label','ict_index']]

    players_clustering=players_clustering.sort_values(by='ict_index',ascending=False)
    players_clustering=players_clustering.reset_index(drop=True)
    #write dataframe as table to the database
    players_clustering.to_sql(name='cluster_results', con=cnx,index=False,if_exists='replace')
    cnx.close()

if __name__ == '__main__':
    main()
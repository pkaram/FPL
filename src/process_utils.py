
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from connection_utils import store_df_to_db, drop_data_from_table

def get_events_deadlines(r_json, execution_date):
    events = pd.DataFrame(r_json['events'])
    upcoming_event_deadline=events[['deadline_time','name']][events['is_next']==1]
    upcoming_event_deadline=upcoming_event_deadline.reset_index(drop=True)
    upcoming_event_name=upcoming_event_deadline.name[0]
    upcoming_event_deadline=upcoming_event_deadline.deadline_time[0]
    upcoming_event_deadline=upcoming_event_deadline.replace('T',' ').replace('Z', '')

    events['last_date_loaded'] = execution_date
    events = events.drop(columns=['chip_plays'])
    events['top_element_info'] = [str(s) for s in events['top_element_info']]
    return events, upcoming_event_name, upcoming_event_deadline
   
def add_teams_data(r_json, execution_date, upcoming_event_name):
    tn = 'teams'
    df = pd.DataFrame(r_json['teams'])    
    df['date_loaded'] = execution_date
    df['upcoming_event_name'] = upcoming_event_name
    drop_data_from_table(tn, execution_date)
    store_df_to_db(df,tn,strategy='append')
    return df

def add_player_type_data(r_json):
    df = pd.DataFrame(r_json['element_types'])
    df = df.drop(columns=['sub_positions_locked'])
    store_df_to_db(df,'player_type')


def add_player_data(r_json, upcoming_event_name, upcoming_event_deadline, execution_date):
    tn = 'players'
    df = pd.DataFrame(r_json['elements'])
    df['upcoming_event_name'] = upcoming_event_name
    df['upcoming_event_deadline'] = upcoming_event_deadline
    df['date_loaded'] = execution_date
    drop_data_from_table(tn, execution_date)
    store_df_to_db(df,tn,strategy='append')
    return df

def add_element_stats_vocab(r_json):
    df = pd.DataFrame(r_json['element_stats'])
    store_df_to_db(df,'element_stats_vocabulary')


def add_news_information(players, teams, execution_date):
    tn = 'news'
    df = players[['second_name','code','news','news_added','chance_of_playing_this_round','chance_of_playing_next_round','team']]
    df = df[df["news"]!= ""]
    df = df.merge(teams[['id','name']],how='left',left_on='team',right_on='id')    
    df = df.rename(columns = {'name': 'Team'}, inplace = False)
    df = df[['Team','second_name','code','news','news_added','chance_of_playing_this_round','chance_of_playing_next_round']]
    df['news_added'] = df.apply(lambda x: x.news_added.replace('T',' ').replace('Z',''), axis=1)
    df['date_loaded'] = execution_date
    drop_data_from_table(tn, execution_date)
    store_df_to_db(df,tn,strategy='append')


def prepare_data_for_clustering(df):
    df = df[df['minutes']!=0].reset_index(drop=True)
    for col in ['influence','creativity','threat','points_per_game']:
        df[col] = df[col].astype(float)
    return df

def write_cluster_results(df):
    df['now_cost'] = df['now_cost'].astype(float)
    df['now_cost'] = df['now_cost']/10
    df['ict_index'] = df['ict_index'].astype(float)
    df=df[['singular_name','team_name','second_name','now_cost','points_per_game','minutes','cluster','ict_index','PC_1','PC_2']]
    store_df_to_db(df,'cluster_results')

def perform_kmeans(df, cols):
    "performs kmeans on a dataframe"
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(df[cols])
    pca = PCA(n_components=2)
    pca_features = pca.fit_transform(scaled_data)
    kmeans = KMeans(n_clusters=6)
    kmeans.fit(scaled_data)
    df['cluster'] = list(kmeans.labels_)
    df['PC_1'] = pca_features[:, 0]
    df['PC_2'] = pca_features[:, 1]
    return df
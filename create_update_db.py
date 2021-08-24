#create database based on the infos provided from the api
#'https://fantasy.premierleague.com/api/bootstrap-static/'

import requests
import sqlite3
import pandas as pd
import datetime

def main():
    # Create your connection with the database, if not already existing it will be created
    cnx = sqlite3.connect('fpl_data.db')
    api_url= 'https://fantasy.premierleague.com/api/bootstrap-static/'
    #create today's date
    execution_date=str(datetime.date.today())
    execution_date=execution_date.replace('-','')

    #get request
    r = requests.get(api_url)
    #request to json
    json = r.json()

    ###############
    #load events
    events_df=pd.DataFrame(json['events'])
    #extract the upcoming event deadline and name
    upcoming_event_deadline=events_df[['deadline_time','name']][events_df['is_next']==1]
    upcoming_event_deadline=upcoming_event_deadline.reset_index(drop=True)
    upcoming_event_name=upcoming_event_deadline.name[0]
    upcoming_event_deadline=upcoming_event_deadline.deadline_time[0]
    upcoming_event_deadline=upcoming_event_deadline.replace('T',' ')
    upcoming_event_deadline=upcoming_event_deadline.replace('Z', '')

    #save the date loaded to check when the script run last
    events_df['last_date_loaded']=execution_date
    #drop nested element
    events_df=events_df.drop(columns=['chip_plays'])
    #replace the table since it will refresh its values and past values are not of importance
    events_df['top_element_info']=[str(s) for s in events_df['top_element_info']]
    events_df.to_sql(name='events', con=cnx,index=False,if_exists='replace')

    ###############
    #load teams
    teams_df = pd.DataFrame(json['teams'])
    #load date that data were loaded
    teams_df['date_loaded']=execution_date
    teams_df['upcoming_event_name']=upcoming_event_name
    #delete entries if already existing for the certain day
    table_already_exists=pd.read_sql("select * from sqlite_master where type='table' and name='teams'", cnx)
    if table_already_exists.shape[0]!=0:
        #if table already exists chech max date
        date_loaded_max=pd.read_sql("select max(date_loaded) as max_date from teams", cnx)
        date_loaded_max=date_loaded_max.max_date[0]
        #delete entries if they belong to the same day as the script currently
        if date_loaded_max==execution_date:
            cur=cnx.cursor()
            cur.execute("delete from teams where date_loaded="+date_loaded_max)
    #append entries
    teams_df.to_sql(name='teams', con=cnx,index=False,if_exists='append')

    ###############
    #load element types
    elements_types_df = pd.DataFrame(json['element_types'])
    elements_types_df=elements_types_df.drop(columns=['sub_positions_locked'])
    #element count can be changed and it could useful to have the most recent information
    elements_types_df.to_sql(name='player_type', con=cnx,index=False,if_exists='replace')

    ###############
    #load players
    elements_df=pd.DataFrame(json['elements'])
    elements_df['upcoming_event_name']=upcoming_event_name
    elements_df['upcoming_event_deadline']=upcoming_event_deadline
    elements_df['date_loaded']=execution_date
    #similar as for teams check if table exists so as to create it or delete if needed before appending data
    table_already_exists=pd.read_sql("select * from sqlite_master where type='table' and name='players'", cnx)
    if table_already_exists.shape[0]!=0:
        #if table already exists chech max date
        date_loaded_max=pd.read_sql("select max(date_loaded) as max_date from teams", cnx)
        date_loaded_max=date_loaded_max.max_date[0]
        #delete entries if they belong to the same day as the script currently
        if date_loaded_max==execution_date:
            cur=cnx.cursor()
            cur.execute("delete from players where date_loaded="+date_loaded_max)
    #load every time to get updated stats
    elements_df.to_sql(name='players', con=cnx,index=False,if_exists='append')

    ###############
    #load element stats vocabulary (should be loaded only once)
    element_stats=pd.DataFrame(json['element_stats'])
    #if table exists do nothing since the vocabulary will likely not change
    element_stats.to_sql(name='element_stats_vocabulary', con=cnx,index=False,if_exists='replace')

    ###############
    #table on news and dates that concern players
    news_df=elements_df[['second_name','code','news','news_added','chance_of_playing_this_round','chance_of_playing_next_round','team']]
    news_df=news_df[news_df["news"]!= ""]
    news_df=news_df.merge(teams_df[['id','name']],how='left',left_on='team',right_on='id')
    news_df=news_df.drop(columns=['id','team'])
    news_df=news_df.rename(columns = {'name': 'Team'}, inplace = False)
    news_df=news_df[['Team','second_name','code','news','news_added','chance_of_playing_this_round','chance_of_playing_next_round']]
    news_df['news_added']=[s.replace('T',' ') for s in news_df['news_added']]
    news_df['news_added']=[s.replace('Z','') for s in news_df['news_added']]
    news_df['date_loaded']=execution_date
    #similar as for teams check if table exists so as to create it or delete if needed before appending data
    table_already_exists=pd.read_sql("select * from sqlite_master where type='table' and name='news'", cnx)
    if table_already_exists.shape[0]!=0:
        #if table already exists chech max date
        date_loaded_max=pd.read_sql("select max(date_loaded) as max_date from news", cnx)
        date_loaded_max=date_loaded_max.max_date[0]
        #delete entries if they belong to the same day as the script currently
        if date_loaded_max==execution_date:
            cur=cnx.cursor()
            cur.execute("delete from news where date_loaded="+date_loaded_max)

    #append the data to have all the news
    news_df.to_sql(name='news', con=cnx,index=False,if_exists='append')

    #close connection
    cnx.close()


if __name__=='__main__':
    main()

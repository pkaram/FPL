"""
create database related to fpl
"""
import datetime
import logging
from connection_utils import make_request, store_df_to_db
from process_utils import get_events_deadlines, add_teams_data, add_player_type_data, add_player_data, add_element_stats_vocab, add_news_information

URL = 'https://fantasy.premierleague.com/api/bootstrap-static/'
logging.getLogger().setLevel(logging.INFO)

def main():
    execution_date = str(datetime.date.today())
    execution_date = execution_date.replace('-','')
    logging.info("making request")
    r_json = make_request(URL)
    events, upcoming_event_name, upcoming_event_deadline = get_events_deadlines(r_json, execution_date)
    logging.info("adding events info")
    store_df_to_db(events, 'events')
    logging.info("adding teams info")
    teams = add_teams_data(r_json, execution_date, upcoming_event_name)
    logging.info("adding player_type info")
    add_player_type_data(r_json)
    logging.info("adding players info")
    players = add_player_data(r_json, upcoming_event_name, upcoming_event_deadline, execution_date)
    logging.info("adding vocabulary info")
    add_element_stats_vocab(r_json)
    logging.info("adding news info")
    add_news_information(players, teams, execution_date)

if __name__=='__main__':
    main()

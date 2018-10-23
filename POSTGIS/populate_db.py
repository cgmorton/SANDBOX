import os
import time
from sqlalchemy import create_engine
import db_methods
import config
from sqlalchemy.orm import session as session_module

#######################################
# END OpenET database tables
#######################################

if __name__ == '__main__':
    # start_time = time.time()
    DB_USER = os.environ['DB_USER']
    DB_PASSWORD = os.environ['DB_PASSWORD']
    DB_PORT = os.environ['DB_PORT']
    DB_HOST = os.environ['DB_HOST']
    DB_NAME = os.environ['DB_NAME']

    db_string = "postgresql+psycopg2://" + DB_USER + ":" + DB_PASSWORD
    db_string += "@" + DB_HOST +  ":" + str(DB_PORT) + '/' + DB_NAME
    engine = create_engine(db_string, pool_size=20, max_overflow=0)

    '''
    db_methods.Base.metadata.drop_all(engine)
    db_methods.Base.metadata.create_all(engine)
    '''

    start_time = time.time()

    # Set up the db session
    Session = session_module.sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    # print(Base.metadata.sorted_tables)
    datasets = ['SSEBop']
    user_id = 0
    regions = ["US_states_west_500k", "US_counties_west_500k", "Mason", "CentralValley_15"]
    for rgn in regions[3:4]:
        print(rgn)
        geom_change_by_year = False
        if rgn in config.statics['regions_changing_by_year']:
            geom_change_by_year = True
        for ds in datasets:
            s_year = int(config.statics['all_year'][ds][0])
            e_year = int(config.statics['all_year'][ds][1])
            years = range(s_year, e_year)
            for year_int in years:
                year = str(year_int)
                DB_Util = db_methods.database_Util(rgn, ds, year, user_id, session, geom_change_by_year)
                DB_Util.add_data_to_db()
    session.close()
    print("--- %s seconds ---" % (str(time.time() - start_time)))
    print("--- %s minutes ---" % (str((time.time() - start_time) / 60.0)))
    print("--- %s hours ---" % (str((time.time() - start_time) / 3600.0)))



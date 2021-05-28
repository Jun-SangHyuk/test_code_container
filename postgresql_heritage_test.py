from metadata_test import RoutingSession
from sqlalchemy import text, create_engine
import time
import sqlalchemy
from datetime import date, timedelta
import pandas as pd
from fastapi.encoders import jsonable_encoder
# DB 연결 정보를 받아 내부 DB로 저장
class Inherits:
    def __init__(self, msg: str, schedule: int, preblock_yn: bool):
        self.engine = create_engine("{driver}://{user}:{password}@{host}:{port}/{db_name}".format(
                driver="postgresql",
                user="postgres",
                password="cslee",
                host="localhost",
                port="5432",
                db_name="postgres",
            ))
        self.schedule = schedule
        self.preblock = preblock_yn
        self.json = jsonable_encoder(self.engine.execute(msg).all())
        self.df = pd.DataFrame(self.json)

    def create_table(self, workflow_id: str, user_id: str, col_dict: str):
        self.df.loc[:, 'crt_dt'] = date.today().strftime("%Y-%m-%d %H:%M:%S")
        self.df.loc[:, 'mod_dt'] = (date.today() - timedelta(self.schedule)).strftime(
            "%Y-%m-%d %H:%M:%S")
        self.df.loc[:, 'pre_yn'] = self.preblock
        trigger_sql = """CREATE OR REPLACE FUNCTION insert_dynamic()
                                    RETURNS trigger
                                    LANGUAGE plpgsql
                                   AS $function$
                                   BEGIN
                                     execute format('insert into {wf_id}{user_id}_%s select $1.*',
                                        to_char(new.crt_dt, 'YYYYMMDD')) using new;
                                     RETURN null;
                                   END;
                                   $function$""".format(
            user_id=user_id,
            wf_id=workflow_id
        )

        set_trigger_sql = """CREATE TRIGGER tr_insert
                                    BEFORE INSERT ON {wf_id}{user_id}
                                    FOR EACH ROW EXECUTE PROCEDURE insert_dynamic();""".format(
            user_id=user_id,
            wf_id=workflow_id
        )

        is_success = True
        try:
            self.df.to_sql(f"{workflow_id}{user_id}", con=self.engine, index=False,
                           if_exists="fail", dtype= {'crt_dt': sqlalchemy.types.TIMESTAMP(),
                                                     'mod_dt': sqlalchemy.TIMESTAMP(),
                                                     'pre_yn': sqlalchemy.types.BOOLEAN,
                                                     f'{col_dict}': sqlalchemy.types.JSON()})
            print("create_table done")
            self.engine.execute(text(trigger_sql))
            print("trigger_sql done")
            self.engine.execute(text(set_trigger_sql))
            print("trigger create done")
        except:
            is_success = False

        return print(is_success)

    def create_chil_table(self, user_id, workflow_id, schedule: int, primary_key):
        chil_table_sql = """CREATE TABLE {wf_id}{user_id}_{time} (
                                    LIKE {wf_id}{user_id} INCLUDING ALL,
                                    PRIMARY KEY ("{key}"),                                                       
                                    CHECK ("crt_dt" >= '{time2}' 
                                    AND "crt_dt" < '{time3}') 
                                    ) INHERITS ({wf_id}{user_id});""".format(
            wf_id=workflow_id,
            user_id=user_id,
            time=date.today().strftime("%Y%m%d"),
            key=primary_key,
            time2=(date.today() - timedelta(schedule)).strftime(
                '%Y-%m-%d 00:00:00'),
            time3=date.today().strftime('%Y-%m-%d 00:00:00'))

        print(chil_table_sql)
        is_success = True

        try:
            self.engine.execute(text(chil_table_sql))
        except:
            is_success = False

        return print(is_success)







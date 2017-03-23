import psycopg2
import json

def main():
    with psycopg2.connect("host=localhost port=15432 "
            "dbname=mydb user=postgres password=postgres") as conn:

        #insertByObject(conn)
        #insertByText(conn)
        #select(conn)
        insertByAlchemy()

def select(conn):
    """ json列の取得 """
    with conn.cursor() as cur:
        # sqlの実行
        cur.execute("SELECT * FROM json_test")
        for row in cur.fetchall():
            print(row[0], row[1], type(row[0]))


def insertByObject(conn):
    """ libpqのJSONオブジェクトでのINSERT """
    from psycopg2.extras import Json

    with conn.cursor() as cur:
        #json objectで設定
        json_dict = {"a":30, "b": "extras.Json"}
        cur.execute("INSERT INTO json_test(info) VALUES(%s)", [Json(json_dict)])
        conn.commit();

def insertByText(conn):
    """ SQL文中でキャスト """
    with conn.cursor() as cur:
        #textで設定
        json_dict = {"a":40, "b": "json dumps"}
        cur.execute("INSERT INTO json_test(info) VALUES(jsonb(%s))", [json.dumps(json_dict)])
        conn.commit();

def insertByAlchemy():
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import create_engine, Column, Integer
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.orm import sessionmaker

    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:15432/mydb')
    Base = declarative_base()

    class JsonTest(Base):
        __tablename__ = 'json_test'
        id = Column(Integer, primary_key=True)
        info = Column(JSONB)

    session = sessionmaker(bind=engine)()

    jt = JsonTest()
    jt.info= {"a": 123, "b": "SQLAlchemy"}
    session.add(jt)
    session.commit()
    
    for row in session.query(JsonTest):
        print(row.info)

if __name__ == "__main__":
    main()

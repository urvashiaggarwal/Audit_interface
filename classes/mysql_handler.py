import mysql.connector
from pydantic import BaseModel
from typing import Union, List 
from dotenv import load_dotenv
import os, json
import pandas as pd
from classes.gemini_models import GeminiClient


class DataPointScore(BaseModel):
    data_point_name: str
    index: int
    score: int

class MySQLHandler:
    def __init__(self):
        #self.client = gemini_client 
        load_dotenv()  # Load variables from .env
        self.config = {
            "host": os.getenv("MYSQL_HOST"),
            "user": os.getenv("MYSQL_USER"),
            "password": os.getenv("MYSQL_PASSWORD"),
            "database": os.getenv("MYSQL_DATABASE"),
        }
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor(dictionary=True)
            print("✅ Connected to MySQL")
        except mysql.connector.Error as err:
            print(f"❌ Connection Error: {err}")

    def select(self, query, params=None):
        if not self.cursor:
            print("❌ No database connection.")
            return []
        try:
            self.cursor.execute(query, params or ())
            return self.cursor.fetchall()
        except mysql.connector.Error as err:
            print(f"❌ SELECT Error: {err}")
            return []

    def insert(self, query, params):
        if not self.connection or not self.cursor:
            print("❌ No database connection.")
            return
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
            print("✅ Data Inserted")
        except mysql.connector.Error as err:
            print(f"❌ INSERT Error: {err}")

    def update(self, query, params):
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
            print("✅ Data Updated")
        except mysql.connector.Error as err:
            print(f"❌ UPDATE Error: {err}")
    
    def fetch_unscored_rows(self, data_point_name):
        query = """
            SELECT * FROM bible_data
            WHERE data_point_name = %s AND is_scored = '0' AND updated_at IS NULL
        """
        
        self.cursor.execute(query, (data_point_name,))
        #print(self.cursor.statement)  # Debugging: print the executed query
        return self.cursor.fetchall()

    def update_scores(self, results: List[DataPointScore]):
        print(f"Updating scores for {len(results)} results")
        for r in results:
            update_query = """
                UPDATE bible_data
                SET score = %s,  is_scored = 1, updated_at = NOW()
                WHERE index_value = %s AND data_point_name = %s
            """
            self.cursor.execute(update_query, (
                r.score,
                r.index,
                r.data_point_name
            ))
            if self.cursor.rowcount == 0:
                print(f"❌ No row updated for index: {r.index}, data_point: {r.data_point_name}")
            else:
                print(f"✅ Updated index: {r.index}, data_point: {r.data_point_name}")
            self.conn.commit()
            

    def process_data_point(self, data_point, instruction):
        rows = self.fetch_unscored_rows(data_point)
         
        if not rows:
            return
        df = pd.DataFrame(rows)
        print(df.head())  # Debugging: print the first few rows of the DataFrame
        results = self.client.get_scores(df, data_point, instruction)
        print(f"Processing {data_point} with {len(results)} results {results}")
        self.update_scores(results)

    def populate_competition_oprns_audit_data(self):
        # Fetch all mappings from data_points
        mappings = self.select("SELECT * FROM data_points")
        for row in mappings:
            xid = row['xid']
            dp_name = row['data_point_name']
            col1 = row['c1']  # housing_table
            col2 = row['c2']  # magicbricks
            col3 = row['c3']  # squareyards

            def fetch_value(table, col, xid):
                if not col or col.strip() == '':
                    return None
                res = self.select(f"SELECT `{col}` FROM `{table}` WHERE xid = %s", (xid,))
                return res[0][col] if res and col in res[0] else None

            v1 = fetch_value("housing_table", col1, xid)
            v2 = fetch_value("magicbricks", col2, xid)
            v3 = fetch_value("squareyards", col3, xid)

            # Insert or update in competition_oprns_audit_data
            insert_query = """
                INSERT INTO competition_oprns_audit_data (
                    xid, data_point_name, value_99acres, value_magicbricks, value_squareyards
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    value_99acres=VALUES(value_99acres),
                    value_magicbricks=VALUES(value_magicbricks),
                    value_squareyards=VALUES(value_squareyards)
            """
            self.insert(insert_query, (xid, dp_name, v1, v2, v3))

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print(" Connection Closed")


# 🧪 Example usage:
# *if __name__ == "__main__":
#     client = GeminiClient()
#     processor = MySQLHandler(client)
#     instructions_path = r"C:\Users\abhishek.a3\Desktop\python\audit\P-S MS\prompt\bible_instructions.json"
    
#     with open(instructions_path, "r") as file:
#         instructions = json.load(file)

#     for dp, rule in instructions.items():
#         print(f"Processing: {dp}")
#         try:
#             processor.process_data_point(dp, rule)
#         except Exception as e:
#            print(f"Error processing {dp}: {e}")


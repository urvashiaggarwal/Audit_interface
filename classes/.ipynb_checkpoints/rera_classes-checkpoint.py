import pandas as pd
import json
from datetime import datetime

class RERAClasses:
    def __init__(self, db_handler):
        self.db = db_handler

    def insert_rera_data(self, df: pd.DataFrame) -> int:
        inserted = 0
        insert_query = """
            INSERT INTO rera (xid, rera_no, state, elements_data, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        for _, row in df.iterrows():
            if pd.isna(row["xid"]) or pd.isna(row["rera_no"]):
                continue

            # Build elements_data JSON by excluding known columns
            known_columns = {"xid", "rera_no", "state"}
            elements = {
                k: row[k] for k in df.columns
                if k not in known_columns and not pd.isna(row[k])
            }
            elements_json = json.dumps(elements, ensure_ascii=False)

            values = (
                row["xid"],
                row["rera_no"],
                row["state"] if not pd.isna(row["state"]) else None,
                elements_json,
                datetime.now(),
            )
            self.db.insert(insert_query, values)
            inserted += 1

        return inserted
    def insert_housing_data(self, df: pd.DataFrame) -> int:
        inserted = 0
        insert_query = """
            INSERT INTO c1_housing (xid, rera_no, housing_data, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        for _, row in df.iterrows():
            if pd.isna(row["xid"]) or pd.isna(row["rera_no"]):
                continue

            # Build elements_data JSON by excluding known columns
            known_columns = {"xid", "rera_no"}
            elements = {
                k: row[k] for k in df.columns
                if k not in known_columns and not pd.isna(row[k])
            }
            elements_json = json.dumps(elements, ensure_ascii=False)

            values = (
                row["xid"],
                row["rera_no"],
                #row["state"] if not pd.isna(row["state"]) else None,
                elements_json,
                datetime.now(),
            )
            self.db.insert(insert_query, values)
            inserted += 1

        return inserted


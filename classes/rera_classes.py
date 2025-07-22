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

        for _, row in df.iterrows():
            if pd.isna(row["XID"]) or pd.isna(row["RERA"]):
                continue

            # Known columns
            known_columns = {"XID", "RERA"}
            data_columns = {
                k.replace(" - ", "_").replace(" ", "_").replace("-","_"): row[k]
                #k.replace("-", "_"): row[k]                 
                for k in df.columns if k not in known_columns and not pd.isna(row[k])
            }

            # Prepare final insert dictionary
            insert_data = {
                "xid": row["XID"],
                "rera_no": row["RERA"],
                **data_columns
            }

            columns = ", ".join(insert_data.keys())
            placeholders = ", ".join(["%s"] * len(insert_data))
            values = list(insert_data.values())

            insert_query = f"""
                INSERT INTO housing_table ({columns})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {', '.join([f"{col}=VALUES({col})" for col in insert_data if col != 'xid'])}
            """
            print(insert_query) 
            self.db.insert(insert_query, values)
            inserted += 1

        return inserted
    def insert_magicbrics_data(self, df: pd.DataFrame) -> int:
        inserted = 0

        for _, row in df.iterrows():
            if pd.isna(row["XID"]) or pd.isna(row["RERA Number"]):
                continue

            # Known columns
            known_columns = {"XID", "RERA Number"}
            data_columns = {
                k.replace(" ", "_"): row[k]
                for k in df.columns if k not in known_columns and not pd.isna(row[k])
            }

            # Prepare final insert dictionary
            insert_data = {
                "xid": row["XID"],
                "rera_no": row["RERA Number"],
                **data_columns
            }

            columns = ", ".join(insert_data.keys())
            placeholders = ", ".join(["%s"] * len(insert_data))
            values = list(insert_data.values())

            insert_query = f"""
                INSERT INTO magic_bricks_table ({columns})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {', '.join([f"{col}=VALUES({col})" for col in insert_data if col != 'xid'])}
            """

            self.db.insert(insert_query, values)
            inserted += 1

        return inserted
    def insert_squareyards_data(self, df: pd.DataFrame) -> int:
        inserted = 0

        for _, row in df.iterrows():
            if pd.isna(row["XID"]) or pd.isna(row["RERA Number"]):
                continue

            # Known columns
            known_columns = {"XID", "RERA Number"}
            data_columns = {
                k.replace(" ", "_"): row[k]
                for k in df.columns if k not in known_columns and not pd.isna(row[k])
            }

            # Prepare final insert dictionary
            insert_data = {
                "xid": row["XID"],
                "rera_no": row["RERA Number"],
                **data_columns
            }

            columns = ", ".join(insert_data.keys())
            placeholders = ", ".join(["%s"] * len(insert_data))
            values = list(insert_data.values())

            insert_query = f"""
                INSERT INTO square_yards_table ({columns})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {', '.join([f"{col}=VALUES({col})" for col in insert_data if col != 'xid'])}
            """

            self.db.insert(insert_query, values)
            inserted += 1

        return inserted
    def ninetynineacres_data(self, df: pd.DataFrame) -> int:
        inserted = 0

        for _, row in df.iterrows():
            if pd.isna(row["xid"]) or pd.isna(row["registrationNumber"]):
                continue

            # Known columns
            known_columns = {"xid", "registrationNumber"}
            data_columns = {
                #k.replace(" ", "_"): row[k]
                k.replace(" - ", "_").replace(" ", "_"): row[k]
                for k in df.columns if k not in known_columns and not pd.isna(row[k])
            }

            # Prepare final insert dictionary
            insert_data = {
                "xid": row["xid"],
                "rera_no": row["registrationNumber"],
                **data_columns
            }

            columns = ", ".join(insert_data.keys())
            placeholders = ", ".join(["%s"] * len(insert_data))
            values = list(insert_data.values())

            insert_query = f"""
                INSERT INTO 99acres_table ({columns})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {', '.join([f"{col}=VALUES({col})" for col in insert_data if col != 'xid'])}
            """
             
            self.db.insert(insert_query, values)
            
            inserted += 1

        return inserted
    def insert_brochure_data(self, df: pd.DataFrame) -> int:
        inserted = 0
        insert_query = """
            INSERT INTO brochure (xid, rera_no, sy_data)
            VALUES (%s, %s, %s)
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
                elements_json
            )
            self.db.insert(insert_query, values)
            inserted += 1

        return inserted
# Function of Floor and Tower Tables :
    def insert_mb_floor_data(self, df: pd.DataFrame) -> int:
        inserted = 0
        insert_query = """
            INSERT INTO mb_floor_table (xid, unit_type, unit_size, area_type, price, possession_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        for _, row in df.iterrows():
            if pd.isna(row["XID"]):
                continue

            # Build elements_data JSON by excluding known columns
            '''known_columns = {"xid"}
            elements = {
                k: row[k] for k in df.columns
                if k not in known_columns and not pd.isna(row[k])
            }
            elements_json = json.dumps(elements, ensure_ascii=False)'''

            values = (
                row["XID"],
                row["Unit Type"],
                row['Unit Size'], 
                row['Area Type'], 
                row['Price'], 
                row['Possession Date']
            )
            self.db.insert(insert_query, values)
            inserted += 1

        return inserted
    def insert_housing_floor_data(self, df: pd.DataFrame) -> int:
        inserted = 0
        insert_query = """
            INSERT INTO hosuing_floor_table (xid, Project_Name, Configuration, List_Item, Price)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        for _, row in df.iterrows():
            if pd.isna(row["XID"]):
                continue
 

            values = (
                row["XID"],
                row["Project Name"],
                row['Configuration'], 
                row['List Item'], 
                row['Price'] 
            )
            self.db.insert(insert_query, values)
            inserted += 1

        return inserted 
    def insert_squareyards_floor_data(self, df: pd.DataFrame) -> int:
        inserted = 0
        insert_query = """
            INSERT INTO sy_floor_table (xid, Project_Name, Unit_Type, Area, price, Area_Type)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        for _, row in df.iterrows():
            if pd.isna(row["XID"]):
                continue

            values = (
                row["XID"],
                row["Project Name"],
                row["Unit Type"], 
                row['Area'], 
                row['Price'], 
                row['Area Type']
            )
            self.db.insert(insert_query, values)
            inserted += 1

        return inserted
    def fetch_all_data_by_xid(self, xid):
        result = {}

        result['99acres'] = self.db.select("SELECT * FROM `99acres_table` WHERE xid = %s", (xid,))
        result['housing'] = self.db.select("""
            SELECT h.*, p.xid, p.Project_Name, p.Configuration, p.List_Item, p.Price
            FROM housing_table h
            LEFT JOIN hosuing_floor_table p ON h.xid = p.xid
            WHERE h.xid = %s
        """, (xid,))
        result['magic_bricks'] = self.db.select("""
            SELECT mb.*, f.unit_type, f.unit_size, f.area_type, f.price, f.possession_date
            FROM magic_bricks_table mb
            LEFT JOIN mb_floor_table f ON mb.xid = f.xid
            WHERE mb.xid = %s
        """, (xid,))
        result['square_yards'] = self.db.select("""
            SELECT sy.*, f.Project_Name, f.Unit_Type, f.Area, f.Price, f.Area_Type
            FROM square_yards_table sy
            LEFT JOIN sy_floor_table f ON sy.xid = f.xid
            WHERE sy.xid = %s
        """, (xid,))
        

        return result

    

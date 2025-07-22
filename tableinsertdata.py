import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "audit_test"
}
PROJECT_KEY = "xid" 

def update_matching_score():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    # Get all data points (columns) except id, website, created_at
    cursor.execute("SHOW COLUMNS FROM data_points")
    columns = [row['Field'] for row in cursor.fetchall() if row['Field'] not in ('id', 'website', 'created_at')]
    print("Columns:", columns)

    cursor.execute("SELECT * FROM data_points")
    data_points_rows = cursor.fetchall()

    # For each project in your main list (e.g., from 99acres or a master list)
    print(PROJECT_KEY)
    cursor.execute(f"SELECT {PROJECT_KEY} FROM 99acres_table")  # or your master list
    print("Fetched project IDs from 99acres_table:")

    projects = [r[PROJECT_KEY] for r in cursor.fetchall()]
    print(projects)
    for project_id in projects:
        for data_point in columns:
            # Get column names for each source from mapping table
            col_99acres = data_points_rows[0][data_point]
            col_magicbricks = data_points_rows[1][data_point]
            col_housing = data_points_rows[2][data_point]
            col_squareyards = data_points_rows[3][data_point]

            # Debug: Print column mappings
            print(f"Data Point: {data_point}")
            print(f"  99acres column: {col_99acres}")
            print(f"  magicbricks column: {col_magicbricks}")
            print(f"  housing column: {col_housing}")
            print(f"  squareyards column: {col_squareyards}")

            # Fetch values from each table
            def get_value(table, col, pid):
                if not col:
                    print(f"  [SKIP] No column mapping for {table} on data point.")
                    return None
                print(f"  [QUERY] SELECT `{col}` FROM `{table}` WHERE `{PROJECT_KEY}` = {pid}")
                cursor.execute(f"SELECT `{col}` FROM `{table}` WHERE `{PROJECT_KEY}` = %s", (pid,))
                res = cursor.fetchone()
                print(f"  [RESULT] {{res}}")
                return res[col] if res and col in res else None

            if(col_99acres=='Project Locality + Project Region'):
                # Access 'Project Locality' and 'Project Region' columns for 99acres, concatenate them
                col_locality = "Project_Locality"
                col_region = "Project_Region"
                val_locality = get_value("99acres_table", col_locality, project_id)
                val_region = get_value("99acres_table", col_region, project_id)
                v_99acres = ""
                if val_locality and val_region:
                    v_99acres = f"{val_locality} {val_region}"
                elif val_locality:
                    v_99acres = val_locality
                elif val_region:
                    v_99acres = val_region
                else:
                    v_99acres = None
            else:
                v_99acres = get_value("99acres_table", col_99acres, project_id)

            v_magicbricks = get_value("magic_bricks_table", col_magicbricks, project_id)
            v_housing = get_value("housing_table", col_housing, project_id)
            v_squareyards = get_value("square_yards_table", col_squareyards, project_id)

            # Debug: Print final values before insert
            print(f"Inserting for project {project_id}, data_point {data_point}:")
            print(f"  v_99acres: {v_99acres}")
            print(f"  v_magicbricks: {v_magicbricks}")
            print(f"  v_housing: {v_housing}")
            print(f"  v_squareyards: {v_squareyards}")

            # Insert or update in competition_oprns
            insert_query = """
                INSERT INTO competition_oprns_audit_data (
                    index_value, data_point_name, value_99acres, c1, c2, c3
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    value_99acres=VALUES(value_99acres),
                    c1=VALUES(c1),
                    c2=VALUES(c2),
                    c3=VALUES(c3)
            """
            cursor.execute(insert_query, (project_id, data_point, v_99acres, v_magicbricks, v_housing, v_squareyards))
            res = cursor.fetchone()
            print(res)
            conn.commit()

    print(" Data mapping and insertion complete.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    update_matching_score()
import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "audit_test"
}
PROJECT_KEY = "xid" 

def update_matching_score(xids):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    # Get all data points (columns) except id, website, created_at
    cursor.execute("SHOW COLUMNS FROM data_points")
    columns = [row['Field'] for row in cursor.fetchall() if row['Field'] not in ('id', 'website', 'created_at')]
    print("Columns:", columns)

    cursor.execute("SELECT * FROM data_points")
    data_points_rows = cursor.fetchall()

    print(PROJECT_KEY)
    print("Fetched project IDs from uploaded CSV:")
    projects = xids
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

            if data_point.lower() == 'amenities_list':
                # Create amenities table if not exists
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS competition_amenities (
                        `Index` INT PRIMARY KEY,
                        `99acres` TEXT,
                        `C1` TEXT,
                        `C2` TEXT,
                        `C3` TEXT,
                        `missing_amenities` TEXT
                    )
                ''')
                # Insert or update amenities row only for input XIDs
                if project_id in projects:
                    insert_query = '''
                        INSERT INTO competition_amenities (`Index`, `99acres`, `C1`, `C2`, `C3`)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            `99acres`=VALUES(`99acres`),
                            `C1`=VALUES(`C1`),
                            `C2`=VALUES(`C2`),
                            `C3`=VALUES(`C3`)
                    '''
                    cursor.execute(insert_query, (project_id, v_99acres, v_magicbricks, v_housing, v_squareyards))
                    conn.commit()
            else:
                # Insert or update in competition_oprns_audit_data
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
                conn.commit()

    # Process amenities for only the given XIDs
    import pandas as pd, re
    def extract_amenities_99acres(amenities_str):
        if pd.isna(amenities_str):
            return []
        items = re.split(r',\s*', amenities_str)
        return list(set([re.sub(r'^\d+:\s*', '', item).strip().lower() for item in items]))
    def extract_amenities_other(text):
        if pd.isna(text):
            return []
        items = re.split(r',|;|/|&', text)
        return list(set([item.strip().lower() for item in items if item.strip()]))
    # Fetch amenities for these XIDs
    if projects:
        cursor.execute(f"SELECT * FROM competition_amenities WHERE `Index` IN ({','.join(['%s']*len(projects))})", tuple(projects))
        amenities_rows = cursor.fetchall()
        if amenities_rows:
            df = pd.DataFrame(amenities_rows)
            df['99acres_clean'] = df['99acres'].apply(extract_amenities_99acres)
            df['C1_clean'] = df['C1'].apply(extract_amenities_other)
            df['C2_clean'] = df['C2'].apply(extract_amenities_other)
            df['C3_clean'] = df['C3'].apply(extract_amenities_other)
            df['combined_C'] = df.apply(lambda row: list(set(row['C1_clean'] + row['C2_clean'] + row['C3_clean'])), axis=1)
            df['missing_amenities'] = df.apply(
                lambda row: sorted(set(map(str.lower, row['99acres_clean'])) - set(map(str.lower, row['combined_C']))),
                axis=1
            )
            for idx, row in df.iterrows():
                cell_value = ', '.join(row['missing_amenities'])
                update_query = '''
                    UPDATE competition_amenities
                    SET missing_amenities = %s
                    WHERE `Index` = %s
                '''
                cursor.execute(update_query, (cell_value, row["Index"]))
            conn.commit()

    print(" Data mapping and insertion complete.")
    cursor.close()
    conn.close()

# Update main guard for new signature
if __name__ == "__main__":
    # Example: update_matching_score([123, 456])
    pass
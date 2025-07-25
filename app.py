from flask import Flask, render_template, request, redirect, send_file, session
import pandas as pd
import os, sys, json
from dotenv import load_dotenv
from io import BytesIO
import mysql
# Load environment
load_dotenv()

# Add path to import MySQLHandler
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from classes.mysql_handler import MySQLHandler
from classes.gemini_models import GeminiClient
from classes.rera_classes import RERAClasses
from tableinsertdata import update_matching_score
from competition_operations import run_competition_operations

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')
app.secret_key = 'your_secret_key'  # Add a secret key for session management

@app.route('/csv')
def csv():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    upload_type = request.form.get('type')  # "rera" or "brouchure"

    if not file or not file.filename.endswith('.csv'):
        return "Invalid file"

    filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    file.save(filepath)

    df = pd.read_csv(filepath)
    db = MySQLHandler()
    uploader = RERAClasses(db)

    try:
        if upload_type == "rera":
            inserted = uploader.insert_rera_data(df)
            return f"Inserted {inserted} new rows into rera table."
        elif upload_type == "brouchure":
            inserted = uploader.insert_brochure_data(df)
            return f"Inserted {inserted} new rows into brochure data table."
        elif upload_type == "99acres":
            inserted = uploader.ninetynineacres_data(df)
            return f"Inserted {inserted} new rows into 99acres data table."
        elif upload_type == "housing":
            inserted = uploader.insert_housing_data(df)
            return f"Inserted {inserted} new rows into housing table."
        elif upload_type == "magicbrics":
            inserted = uploader.insert_magicbrics_data(df)
            return f"Inserted {inserted} new rows into magicbrics table."
        elif upload_type == "squareyards":
            inserted = uploader.insert_squareyards_data(df)
            return f"Inserted {inserted} new rows into squareyards table."  
        elif upload_type == "magicbrics-floor":
            inserted = uploader.insert_mb_floor_data(df)
            return f"Inserted {inserted} new rows into magicbricks floor data table."   
        elif upload_type == "housing-floor":
            inserted = uploader.insert_housing_floor_data(df)
            return f"Inserted {inserted} new rows into housing data table."
        elif upload_type == "squareyards-floor":
            inserted = uploader.insert_squareyards_floor_data(df)
            return f"Inserted {inserted} new rows into squareyards data table."
        else:
            return "❌ Invalid upload type."
    finally:
        db.close()

@app.route('/updated_score')
def  updated_score():
    
    try:
        client = GeminiClient()
        processor = MySQLHandler(client)
        instructions_path = os.path.join(os.path.dirname(__file__), "prompt", "bible_instructions.json")

        with open(instructions_path, "r", encoding="utf-8") as file:
            instructions = json.load(file)

        results = []
        for dp, rule in instructions.items():
            try:
                processor.process_data_point(dp, rule)
                results.append(f"✅ Processed: {dp}")
            except Exception as e:
                results.append(f"❌ Error processing {dp}: {e}")

        return "<br>".join(results)

    except Exception as e:
        return f"🔥 Critical failure: {e}"

'''@app.route('/', methods=['GET', 'POST'])
def index():
    db = MySQLHandler()
    fetcher = RERAClasses(db)
    if request.method == 'POST':
        xid = request.form.get('xid')
        if not xid:
            return render_template('index.html', error="Please enter an XID.")
        
        data = fetcher.fetch_all_data_by_xid(xid)
        return render_template('results.html', xid=xid, data=data)
    
    return render_template('index.html')'''


@app.route('/', methods=['GET', 'POST'])
def index():
    db = MySQLHandler()
    fetcher = RERAClasses(db)

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'update_score':
            try:
                file = request.files['file']
                if not file or not file.filename.endswith('.csv'):
                    return render_template('index.html', error="Please upload a valid CSV with 'xid' column")
                df = pd.read_csv(file)
                if 'xid' not in df.columns:
                    return render_template('index.html', error="CSV must contain an 'xid' column")
                xids = [int(x) for x in df['xid'].dropna().unique()]
                session['xids'] = xids  # Store xids in session
                update_matching_score(xids)
                run_competition_operations()
                return render_template('index.html', message="✅ Matching score updated and scoring complete!")
            except Exception as e:
                return render_template('index.html', error=f"❌ Error: {e}")

    return render_template('index.html')     

@app.route('/populate_audit_data')
def populate_audit_data():
    db = MySQLHandler()
    try:
        db.populate_competition_oprns_audit_data()
        return "✅ Data populated in competition_oprns_audit_data."
    except Exception as e:
        return f"❌ Error: {e}"
    finally:
        db.close()

@app.route('/get_output_csv', methods=['POST'])
def get_output_csv():
    import mysql.connector
    import pandas as pd
    import os
    from dotenv import load_dotenv
    from io import BytesIO
    load_dotenv()
    try:
        xids = session.get('xids', None)
        if not xids:
            return render_template('index.html', error="No XIDs found for export. Please upload a file first.")
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
            connection_timeout=5,
            use_pure=True
        )
        placeholders = ','.join(['%s'] * len(xids))
        # Exclude amenities_list from main output
        query = f"SELECT * FROM competition_oprns_audit_data WHERE index_value IN ({placeholders}) AND data_point_name != 'amenities_list'"
        df = pd.read_sql(query, conn, params=tuple(xids))
        output = BytesIO()
        df.to_csv(output, index=False)
        output.seek(0)
        conn.close()
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name='output_results.csv'
        )
    except Exception as e:
        return render_template('index.html', error=f"❌ Error exporting output CSV: {e}")

@app.route('/get_amenities_csv', methods=['POST'])
def get_amenities_csv():
    import mysql.connector
    import pandas as pd
    import os
    from dotenv import load_dotenv
    from io import BytesIO
    load_dotenv()
    try:
        xids = session.get('xids', None)
        if not xids:
            return render_template('index.html', error="No XIDs found for export. Please upload a file first.")
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
            connection_timeout=5,
            use_pure=True
        )
        placeholders = ','.join(['%s'] * len(xids))
        query = f"SELECT * FROM competition_amenities WHERE `Index` IN ({placeholders})"
        df = pd.read_sql(query, conn, params=tuple(xids))
        output = BytesIO()
        df.to_csv(output, index=False)
        output.seek(0)
        conn.close()
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name='amenities_results.csv'
        )
    except Exception as e:
        return render_template('index.html', error=f"❌ Error exporting amenities CSV: {e}")

if __name__ == '__main__':
    app.run(debug=True)



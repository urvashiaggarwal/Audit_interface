from flask import Flask, render_template, request, redirect, send_file
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
        if action == 'upload':
            file = request.files['file']
            if not file or not file.filename.endswith('.csv'):
                return "Please upload a valid CSV with 'xid' column"

            df = pd.read_csv(file)
            if 'xid' not in df.columns:
                return "CSV must contain an 'xid' column"

            sheet_data = {}  # key: sheet name, value: list of records

            for xid in df['xid'].dropna().unique():
                xid = int(xid)
                xid_data = fetcher.fetch_all_data_by_xid(xid)
                for source, records in xid_data.items():
                    for record in records:
                        record['xid'] = xid
                        record['source'] = source
                    sheet_data.setdefault(source, []).extend(records)

            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                for sheet_name, records in sheet_data.items():
                    if records:  # Avoid writing empty sheets
                        pd.DataFrame(records).to_excel(writer, sheet_name=sheet_name[:31], index=False)

            output.seek(0)
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name='xid_data_export.xlsx'
            )
        elif action == 'update_score':
            try:
                update_matching_score()
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
    load_dotenv()
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
            connection_timeout=5,
            use_pure=True
        )
        df = pd.read_sql("SELECT * FROM bible_oprns_data", conn)
        output = BytesIO()
        df.to_csv(output, index=False)
        output.seek(0)
        conn.close()
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name='audit_results.csv'
        )
    except Exception as e:
        return render_template('index.html', error=f"❌ Error exporting CSV: {e}")

if __name__ == '__main__':
    app.run(debug=True)



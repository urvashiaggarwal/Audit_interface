from flask import Flask, render_template, request, redirect
import pandas as pd
import os, sys, json
from dotenv import load_dotenv


# Load environment
load_dotenv()

# Add path to import MySQLHandler
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from classes.mysql_handler import MySQLHandler
from classes.gemini_models import GeminiClient
from classes.rera_classes import RERAClasses

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')

@app.route('/')
def index():
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
            inserted = uploader.insert_brouchure_data(df)
            return f"Inserted {inserted} new rows into bible_data table."
        elif upload_type == "housing":
            inserted = uploader.insert_housing_data(df)
            return f"Inserted {inserted} new rows into housing table."
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

     
if __name__ == '__main__':
    app.run(debug=True)

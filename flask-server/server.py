from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
import numpy as np

app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

global_df = None

@app.route("/upload_csv", methods=["POST"])
def upload_csv():
    global global_df

    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if file:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(file_path)

        global_df = pd.read_csv(file_path)
        global_df = global_df.replace({np.nan: None})
        data = global_df.to_dict(orient="records")

        global_nans = global_df.isnull().sum().reset_index()
        global_nans.columns=["type", "number of nans"]
        global_nans = global_nans.replace({np.nan: None})
        nans = global_nans.to_dict(orient="records")

        duplicates = int(global_df.duplicated().sum())
        
        global_desc = global_df.describe().round(2)
        description = global_desc.to_dict(orient="records")

        return jsonify({
            "data": data,
            "shape": global_df.shape,
            "nans": nans,
            "duplicates": duplicates,
            "description": description
        })

@app.route("/group_by", methods=["POST"])
def group_by():
    global global_df

    if global_df is None:
        return jsonify({"error": "No data available. Please upload a CSV file first."}), 400

    request_data = request.get_json()
    column_name = request_data.get("column_name")
    value_column = request_data.get("value_column")
    aggregation_function = request_data.get("aggregation_function")

    if column_name is None or column_name not in global_df.columns:
        return jsonify({"error": "Invalid column name"}), 400

    if value_column is None or value_column not in global_df.columns:
        return jsonify({"error": "Invalid value column"}), 400

    if aggregation_function not in ["mean", "sum", "count", "max", "min"]:
        return jsonify({"error": "Invalid aggregation function"}), 400

    try:
        grouped_df = global_df.groupby(column_name).agg({value_column: aggregation_function}).reset_index().round(2)
        grouped_data = grouped_df.to_dict(orient="records")
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "grouped_data": grouped_data
    })

if __name__ == "__main__":  
    app.run(debug=True)

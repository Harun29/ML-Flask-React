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

        return jsonify({"data": data, "shape": global_df.shape})

@app.route("/get_nans", methods=['GET'])
def get_nans():
    if global_df is None:
        return jsonify({"error": "No data available. Please upload a CSV file first."}), 400
    
    global_nans = global_df.isnull().sum().reset_index()
    global_nans.columns=["type", "number of nans"]
    global_nans = global_nans.replace({np.nan: None})
    nans = global_nans.to_dict(orient="records")
    
    return jsonify({"nans": nans})

if __name__ == "__main__":
    app.run(debug=True)

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

@app.route("/get_csv", methods=["GET"])
def get_csv():
  global global_df
  if global_df is not None:
    data = global_df.to_dict(orient="records")

    global_nans = global_df.isnull().sum().reset_index()
    global_nans.columns=["type", "number of nans"]
    global_nans = global_nans.replace({np.nan: None})
    nans = global_nans.to_dict(orient="records")

    duplicates = int(global_df.duplicated().sum())
    
    global_desc = global_df.describe().round(2)
    description = global_desc.to_dict(orient="records")

    columns = global_df.columns.tolist()

  return jsonify({
    "data": data,
    "shape": global_df.shape,
    "nans": nans,
    "duplicates": duplicates,
    "description": description,
    "columns": columns
  })

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

    columns = global_df.columns.tolist()

    return jsonify({
      "data": data,
      "shape": global_df.shape,
      "nans": nans,
      "duplicates": duplicates,
      "description": description,
      "columns": columns
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

@app.route("/fillna", methods=["POST"])
def fillna():
  global global_df

  if global_df is None:
    return jsonify({"error": "No data available. Please upload a CSV file first."}), 400

  request_data = request.get_json()
  fill_value = request_data.get("fill_value")
  method = request_data.get("method")
  column_name = request_data.get("column_name")

  if fill_value is None and method is None:
    return jsonify({"error": "Either fill_value or method is required"}), 400

  if fill_value is not None and method is not None:
    return jsonify({"error": "Cannot specify both fill_value and method"}), 400

  if column_name and column_name not in global_df.columns:
    return jsonify({"error": f"Invalid column name: {column_name}"}), 400

  try:
    if column_name:
      if fill_value is not None:
        global_df[column_name].fillna(fill_value, inplace=True)
      else:
        global_df[column_name].fillna(method=method, inplace=True)
    else:
      if fill_value is not None:
        global_df.fillna(fill_value, inplace=True)
      else:
        global_df.fillna(method=method, inplace=True)
    data = global_df.to_dict(orient="records")
  except Exception as e:
    return jsonify({"error": str(e)}), 500

  return jsonify({
    "data": data,
    "shape": global_df.shape
  })


@app.route("/dropna", methods=["POST"])
def dropna():
  global global_df

  if global_df is None:
    return jsonify({"error": "No data available. Please upload a CSV file first."}), 400

  request_data = request.get_json()
  axis = request_data.get("axis", 0)  # 0 for rows, 1 for columns
  how = request_data.get("how", "any")  # 'any' or 'all'

  if axis not in [0, 1]:
    return jsonify({"error": "Invalid axis value"}), 400

  if how not in ["any", "all"]:
    return jsonify({"error": "Invalid how value"}), 400

  try:
    global_df.dropna(axis=axis, how=how, inplace=True)
    data = global_df.to_dict(orient="records")
  except Exception as e:
    return jsonify({"error": str(e)}), 500

  return jsonify({
    "data": data,
    "shape": global_df.shape
  })

if __name__ == "__main__":  
  app.run(debug=True)

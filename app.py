from flask import Flask, request, jsonify

import mysql.connector

from dags.utils import *

app = Flask(__name__)

load_dotenv()

os.path.abspath(__file__)

db_config_obj = DBConfig(load_db_configs_in_dict())
s3_config_obj = S3Config(load_s3_configs_in_dict())

s3_client = get_s3_instance()


@app.route("/execute_query", methods=["POST"])
def execute_query():
    try:
        query = request.json.get("query")
        save_as = request.json.get("save_as")

        if not query:
            return jsonify({"error": "No SQL query provided"}), 400

        mysql_conn = mysql.connector.connect(
            host=db_config_obj.db_host,
            port=db_config_obj.db_port,
            user=db_config_obj.db_user,
            password=db_config_obj.db_password,
            database=db_config_obj.db_name
        )
        cursor = mysql_conn.cursor()

        cursor.execute(query)

        result = cursor.fetchall()

        columns = [col[0] for col in cursor.description]
        data = [dict(zip(columns, row)) for row in result]

        cursor.close()
        mysql_conn.close()

        # TODO: async
        if save_as:
            result_json = jsonify(data).get_json()
            s3_client.put_object(
                Bucket=s3_config_obj.s3_bucket_name,
                Key=save_as,
                Body=str(result_json)
            )
            print("query successfully saved to s3")
        return jsonify(data), 200

    except Exception as e:
        print("something went wrong:", e)
        return jsonify({"error": "query cannot be processed at this time, please try again later"}), 500


@app.route("/retrieve_saved_query", methods=["POST"])
def retrieve_saved_query():
    try:
        save_name = request.json.get("save_name")
        if not save_name:
            return jsonify({"error": "Save name is required"}), 400

        response = s3_client.get_object(
            Bucket=s3_config_obj.s3_bucket_name,
            Key=save_name
        )

        result_json = response["Body"].read().decode()

        return jsonify(result_json), 200

    except Exception as e:
        print("something went wrong:", e)
        return jsonify({"error": "saved query cannot be retrieved at this time, please try again later"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6000, debug=True)

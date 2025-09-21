from flask import Flask, request, jsonify
from flask_cors import CORS


from input_parser import RawArgs
from main import run_algorithm

app = Flask(__name__)
CORS(app)

@app.route("/algorithm", methods=['POST'])
def run_algorithm_route():
    try:
        request_args = RawArgs(**request.json)
        _, subset_df, removed_df = run_algorithm(initial_args=request_args)
        return jsonify({
            'subset_df': subset_df.to_dict('records'),
            'removed_df': removed_df.to_dict('records')
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
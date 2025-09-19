from flask import Flask, request, jsonify
from flask_cors import CORS

from input_parser import RawArgs
from algorithm import run_algorithm, run_algorithm_background

app = Flask(__name__)
CORS(app)

@app.route("/algorithm", methods=['POST'])
def run_algorithm_route():
    try:
        background = request.args.get('background', False)
        if not background:
            request_args = RawArgs(**request.json)
            _, subset_df, removed_df = run_algorithm(initial_args=request_args)
            return jsonify({
                'subset_df': subset_df.to_dict('records'),
                'removed_df': removed_df.to_dict('records')
            }), 200
        else:
            task = run_algorithm_background.delay(request.json)
            return jsonify({
                'task_id': task.id,
            }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
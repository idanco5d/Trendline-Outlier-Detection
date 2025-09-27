from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from class_models import AlgoResponse, RawArgs
from main import run_algorithm
from input_parser import RawArgs
from algorithm import run_algorithm, run_algorithm_background

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # TODO: add production frontend as well
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.post('/', response_model=AlgoResponse)
def algorithm(body: RawArgs):
    _, subset_df, removed_df = run_algorithm(initial_args=body)
    return AlgoResponse(subset_df=subset_df.to_dict('records'),
                        removed_df=removed_df.to_dict('records'))
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
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from class_models import AlgoResponse, RawArgs
from main import run_algorithm

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
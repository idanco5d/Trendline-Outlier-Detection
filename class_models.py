from pydantic import BaseModel


class RawArgs(BaseModel):
    aggregation_function: str
    aggregation_column: str
    grouping_columns: list[str]
    output_folder: str
    dataset_file_name: str | None = None
    data: list[dict] | None = None
    prune_aggpack_by_greedy: int | None = None
    prune_dp_by_greedy: int | None = None
    prune_h: bool | None = None
    mem_opt: bool | None = None
    agg_pack_opt: bool | None = None
    cutoff_seconds: int | None = None


class AlgoResponse(BaseModel):
    subset_df: list[dict]
    removed_df: list[dict]
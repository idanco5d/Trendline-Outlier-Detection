import pandas as pd


def calculate_removed_tuples(original_data_frame: pd.DataFrame, contained_data_frame: pd.DataFrame) -> pd.DataFrame:
    merged_data_frame = original_data_frame.merge(contained_data_frame, how='outer', indicator=True)
    difference_df = merged_data_frame[merged_data_frame['_merge'] == 'left_only']

    return difference_df.drop(columns=['_merge'])

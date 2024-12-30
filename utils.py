from typing import List, Hashable, DefaultDict

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from binary_search_default_dict import BinarySearchDefaultDict


def empty_data_frame(base_df_columns) -> pd.DataFrame:
    return pd.DataFrame(columns=base_df_columns)


def list_of_empty_dictionaries(output_list_length: int, column_base) -> List[DefaultDict[float, pd.DataFrame]]:
    return [BinarySearchDefaultDict(column_base) for _ in range(output_list_length)]


def get_group_by_key(grouped_rows: DataFrameGroupBy, key: Hashable) -> pd.DataFrame:
    firstKey = next(iter(grouped_rows.groups))
    if isinstance(firstKey, tuple):
        return grouped_rows.get_group(key)
    return grouped_rows.get_group((key,))


def data_frames_union(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([df1, df2])


def calculate_removed_tuples(original_data_frame: pd.DataFrame, contained_data_frame: pd.DataFrame) -> pd.DataFrame:
    merged_data_frame = original_data_frame.merge(contained_data_frame, how='outer', indicator=True)
    difference_df = merged_data_frame[merged_data_frame['_merge'] == 'left_only']

    return difference_df.drop(columns=['_merge'])

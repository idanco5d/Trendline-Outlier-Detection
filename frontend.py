from typing import List

import pandas as pd
import streamlit as st
import time

from aggregations_mem import SumAggregationOpt
from optimal_subset_with_constraint_unified import get_optimal_subset_F_first

uploaded_file = st.file_uploader("Choose a file")

agg_col = st.text_input("Aggregation column")

grouping_cols = st.text_input("Grouping columns")

# TODO: make these configurable

algorithm = get_optimal_subset_F_first

AggCls = SumAggregationOpt

grouping_cols_isnt_empty = grouping_cols is not None and grouping_cols != ''

agg_col_isnt_empty = agg_col is not None and agg_col != ''

df_grouping_cols = None

if grouping_cols_isnt_empty:
    df_grouping_cols = grouping_cols.split(',')

agg_typename = AggCls.get_name()

GROUP_NAME = 'Group'

def get_grouped_df(_df: pd.DataFrame, cols: List[str], _agg_col: str, agg_name: str) -> pd.DataFrame:
    _grouped = _df.groupby(cols, as_index=False)[_agg_col].agg([agg_name])
    _grouped.rename(columns={agg_name: _agg_col}, inplace=True)
    _grouped[GROUP_NAME] = _grouped[cols].astype(str).agg('_'.join, axis=1)
    return _grouped

df = None

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)

all_input_entered = uploaded_file is not None and grouping_cols_isnt_empty and agg_col_isnt_empty

bar_chart_placeholder = st.empty()

if all_input_entered:
    grouped = get_grouped_df(_df=df, cols=df_grouping_cols, _agg_col=agg_col, agg_name=agg_typename)
    bar_chart_placeholder.bar_chart(grouped, x=GROUP_NAME, y=agg_col, stack=False)
    with st.spinner("Running algorithm...", show_time=True):
        subset_df, _ = algorithm(
            df=df,
            group_cols=df_grouping_cols,
            agg_col=agg_col,
            Agg=AggCls,
        )
        time.sleep(5)  # simulate "long" algorithm run
    subset_grouped = get_grouped_df(subset_df, df_grouping_cols, agg_col, agg_typename)
    bar_chart_placeholder.bar_chart(subset_grouped, x=GROUP_NAME, y=agg_col, stack=False)
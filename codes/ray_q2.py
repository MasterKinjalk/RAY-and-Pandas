"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-10 21:45:33
 	 FilePath: /codes/pandas_q2.py
 	 Description:
"""
import pandas as pd
import ray
import typing
import util.judge_df_equal
import tempfile
import numpy as np
from ray.util.queue import Queue




@ray.remote
def compute_summary_chunk(chunk):
    
    # Perform aggregation similar to the SQL query
    result = chunk.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=('l_quantity', 'sum'),
        sum_base_price=('l_extendedprice', 'sum'),
        sum_disc_price=('l_extendedprice', lambda x: (x * (1 - chunk.loc[x.index, 'l_discount'])).sum()),
        sum_charge=('l_extendedprice', lambda x: (x * (1 - chunk.loc[x.index, 'l_discount']) * (1 + chunk.loc[x.index, 'l_tax'])).sum()),
        avg_qty=('l_quantity', 'mean'),
        avg_price=('l_extendedprice', 'mean'),
        avg_disc=('l_discount', 'mean'),
        count_order=('l_quantity', 'count')
    ).reset_index()
    return result



def ray_q2(timediff: int, lineitem: pd.DataFrame) -> pd.DataFrame:
    ray.init(num_cpus=4, ignore_reinit_error=True)

    lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
    cutoff_date = pd.Timestamp('1998-12-01') - pd.Timedelta(days=timediff)
    filtered_df = lineitem[lineitem['l_shipdate'] <= cutoff_date]

    data_chunks = np.array_split(filtered_df, 8)
    tasks = [compute_summary_chunk.remote(chunk) for chunk in data_chunks]
    results = ray.get(tasks)

    combined_result = pd.concat(results)

    # To calculate weighted averages, first sum the relevant quantities and then divide
    final_result = combined_result.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=('sum_qty', 'sum'),
        sum_base_price=('sum_base_price', 'sum'),
        sum_disc_price=('sum_disc_price', 'sum'),
        sum_charge=('sum_charge', 'sum'),
        total_qty=('sum_qty', 'sum'),  # Total quantity for calculating avg_qty
        total_base_price=('sum_base_price', 'sum'),  # Total base price for calculating avg_price
        avg_disc=('avg_disc', 'mean'),  # Average discount can be directly averaged
        count_order=('count_order', 'sum')
    ).reset_index()

    # Now calculate the weighted averages for avg_qty and avg_price
    final_result['avg_qty'] = final_result['sum_qty'] / final_result['count_order']
    final_result['avg_price'] = final_result['sum_base_price'] / final_result['count_order']

    # Drop the intermediate sum columns if they are not needed in the final output
    final_result.drop(columns=['total_qty', 'total_base_price'], inplace=True)
    # Reorder the DataFrame columns as specified
    final_columns_order = ['l_returnflag', 'l_linestatus', 'sum_qty', 'sum_base_price', 'sum_disc_price',
                           'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order']
    final_result = final_result[final_columns_order]

    final_result = final_result.sort_values(by=['l_returnflag', 'l_linestatus'])

    ray.shutdown()
    return final_result


if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']

    # run the test
    result = ray_q2(90, lineitem)
    # result.to_csv("correct_results/pandas_q2.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q2.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")



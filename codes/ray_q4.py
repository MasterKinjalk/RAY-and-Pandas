"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-10 21:45:33
 	 FilePath: /codes/pandas_q2.py
 	 Description:
"""
import tempfile

import pandas as pd
import ray
import typing
import util.judge_df_equal
import numpy as np



@ray.remote
def process_orders_chunk(orders_chunk, filtered_lineitem):
    # Orders have already been filtered by date before chunking, so just join with lineitem
    valid_orders = pd.merge(orders_chunk, filtered_lineitem, left_on='o_orderkey', right_on='l_orderkey', how='inner').drop_duplicates('o_orderkey')
    # Return counts by o_orderpriority for the chunk
    return valid_orders.groupby('o_orderpriority').size().reset_index(name='order_count')

def ray_q4(time: str, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    ray.init()
    time_as_datetime = pd.to_datetime(time)
    end_time = time_as_datetime + pd.DateOffset(months=3)
    # Ensure datetime conversion is done outside the remote function
    orders['o_orderdate'] = pd.to_datetime(orders['o_orderdate'])
    lineitem['l_commitdate'] = pd.to_datetime(lineitem['l_commitdate'])
    lineitem['l_receiptdate'] = pd.to_datetime(lineitem['l_receiptdate'])

    # Pre-filter lineitem DataFrame
    filtered_lineitem = lineitem[lineitem['l_commitdate'] < lineitem['l_receiptdate']]
    # Filter orders DataFrame by date range before splitting into chunks
    filtered_orders = orders[(orders['o_orderdate'] >= time_as_datetime) & (orders['o_orderdate'] < end_time)]
    
    orders_chunks = np.array_split(filtered_orders, 6)  # Splitting after filtering
    
    tasks = [process_orders_chunk.remote(chunk, filtered_lineitem) for chunk in orders_chunks]
    
    results = ray.get(tasks)
    combined_result = pd.concat(results)
    
    final_result = combined_result.groupby('o_orderpriority').agg(order_count=('order_count', 'sum')).reset_index().sort_values(by='o_orderpriority')
    
    ray.shutdown()
    return final_result



if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    orders = pd.read_csv("tables/orders.csv", header=None, delimiter="|")
    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']

    orders.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']

    # run the test
    result = ray_q4("1993-7-01",orders,lineitem)
    # result.to_csv("correct_results/pandas_q4.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q4.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")

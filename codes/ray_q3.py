"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-11 10:03:13
 	 FilePath: /codes/pandas_q3.py
 	 Description:
"""
import tempfile

import pandas as pd
import ray
import typing

import util.judge_df_equal


import ray
import pandas as pd
import numpy as np

# Assuming Ray has been initialized elsewhere as needed
# ray.init()

@ray.remote
def process_chunk(customer_df, orders_chunk, lineitem_chunk):
    # Filter orders and lineitem DataFrames based on the date conditions
    orders_filtered = orders_chunk[orders_chunk['o_orderdate'] < pd.Timestamp('1995-03-15')]
    lineitem_filtered = lineitem_chunk[lineitem_chunk['l_shipdate'] > pd.Timestamp('1995-03-15')]
    
    # Join filtered customer DataFrame with orders, then with lineitem
    merged_df = pd.merge(customer_df, orders_filtered, how='inner', left_on='c_custkey', right_on='o_custkey')
    merged_df = pd.merge(merged_df, lineitem_filtered, how='inner', left_on='o_orderkey', right_on='l_orderkey')
    
    # Perform group by aggregation to calculate revenue
    result = merged_df.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']).agg(
        revenue=('l_extendedprice', lambda x: (x * (1 - merged_df.loc[x.index, 'l_discount'])).sum())
    ).reset_index()
    
    # Sort the results within each chunk
    result = result.sort_values(by=['revenue', 'o_orderdate'], ascending=[False, True])
    return result

def ray_q3(segment: str, customer: pd.DataFrame, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    ray.init(num_cpus = 4)
    # Filter customer DataFrame based on segment
    filtered_customers = customer[customer['c_mktsegment'] == segment]
    
    # Convert dates to datetime format for filtering
    orders['o_orderdate'] = pd.to_datetime(orders['o_orderdate'])
    lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
    
    # Split orders and lineitem DataFrames into chunks for parallel processing
    orders_chunks = np.array_split(orders, 4)  # Adjust number of chunks as needed
    lineitem_chunks = np.array_split(lineitem, 4)
    
    # Dispatch tasks to Ray for parallel processing
    tasks = [process_chunk.remote(filtered_customers, orders_chunk, lineitem_chunk) 
             for orders_chunk, lineitem_chunk in zip(orders_chunks, lineitem_chunks)]
    
    # Retrieve results from Ray tasks
    results = ray.get(tasks)
    
    # Combine results from all chunks
    combined_result = pd.concat(results)
    
    # Perform final aggregation and sorting on the combined result
    final_result = combined_result.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']).agg(
        revenue=('revenue', 'sum')
    ).reset_index().sort_values(by=['revenue', 'o_orderdate'], ascending=[False, True]).head(10)
    ray.shutdown()
    return final_result




if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    orders = pd.read_csv("tables/orders.csv", header=None, delimiter="|")
    customer = pd.read_csv("tables/customer.csv", header=None, delimiter="|")


    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    customer.columns = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
                        'c_comment']
    orders.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']

    # run the test
    result = ray_q3('BUILDING', customer, orders, lineitem)
    # result.to_csv("correct_results/pandas_q3.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q3.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")

"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:47
 	 LastEditTime: 2023-09-11 10:37:24
 	 FilePath: /codes/pandas_q1.py
 	 Description:
"""
import pandas as pd
import ray
import typing
import numpy as np




@ray.remote
def calculate_revenue_chunk(chunk, time_as_datetime):
    # Ensure 'l_shipdate' is a datetime format within the function (if not already converted)
    chunk['l_shipdate'] = pd.to_datetime(chunk['l_shipdate'])
    
    # Perform the filtering
    filtered_chunk = chunk[
        (chunk['l_shipdate'] >= time_as_datetime) &
        (chunk['l_shipdate'] < time_as_datetime + pd.DateOffset(years=1)) &
        (chunk['l_discount'].between(0.06 - 0.01, 0.06 + 0.010001)) &
        (chunk['l_quantity'] < 24)
    ]
    
    # Calculate and return the sum of 'l_extendedprice' * 'l_discount'
    return (filtered_chunk['l_extendedprice'] * filtered_chunk['l_discount']).sum()


def ray_q1(time: str, lineitem: pd.DataFrame) -> float:
    time_as_datetime = pd.to_datetime(time)
    data_chunks = np.array_split(lineitem, 8)  # Adjust the number of chunks based on your dataset size and available resources
    ray.init()
    # Dispatch tasks
    tasks = [calculate_revenue_chunk.remote(chunk, time_as_datetime) for chunk in data_chunks]

    # Retrieve and sum up the results
    results = ray.get(tasks)
    total_revenue = sum(results)
    ray.shutdown()
    return total_revenue




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
    result = ray_q1("1994-01-01", lineitem)

    try:
        assert abs(result - 123141078.2283) < 0.01
        print("*******************pass**********************")
    except Exception as e:
        logger.error("Exception Occurred:" + str(e))
        print(f"*******************failed, your incorrect result is {result}**************")

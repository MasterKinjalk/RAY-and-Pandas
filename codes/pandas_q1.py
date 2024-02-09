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


def pandas_q1(time: str, lineitem:pd.DataFrame) -> float:
    # TODO: your codes begin
        # Convert the 'time' parameter to a datetime object
    time_as_datetime = pd.to_datetime(time)
    
    # Ensure the 'l_shipdate' column is in datetime format
    lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
    

    # Perform the filtering with 'l_shipdate' as datetime objects
    filtered_df = lineitem[
        (lineitem['l_shipdate'] >= time_as_datetime) &
        (lineitem['l_shipdate'] < time_as_datetime + pd.DateOffset(years=1)) &
        (lineitem['l_discount'].between(0.06 - 0.01, 0.06 + 0.010001)) &
        (lineitem['l_quantity'] < 24)
    ]
    
    # Calculate the sum of 'l_extendedprice' * 'l_discount' for the filtered rows
    revenue_increase = (filtered_df['l_extendedprice'] * filtered_df['l_discount']).sum()
    
    return revenue_increase




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
    result = pandas_q1("1994-01-01", lineitem)
    try:
        assert abs(result - 123141078.2283) < 0.01
        print("*******************pass**********************")
    except Exception as e:
        logger.error("Exception Occurred:" + str(e))
        print(f"*******************failed, your incorrect result is {result}**************")

import os
import time

# TPC-H Dataset Path
path = os.environ['TPCH_DATASET_PATH']

#### MODIN Dask Configuration #############################
import modin.config as modin_cfg
modin_cfg.Engine.put("dask")

from dask.distributed import Client
client = Client(processes=False)

import modin.pandas as pd

#####################################################################################

def q1(li):
    df = li[(li.l_shipdate<='1998-09-02')]
    df = df[['l_shipdate', 'l_returnflag', 'l_linestatus', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax']]
    df['a'] = ((df.l_extendedprice) * (1 - (df.l_discount)))
    df['b'] = (((df.l_extendedprice) * (1 - (df.l_discount))) * (1 + (df.l_tax)))
    df = df \
        .groupby(['l_returnflag', 'l_linestatus']) \
        .agg(
            sum_qty=("l_quantity", "sum"),
            sum_base_price=("l_extendedprice", "sum"),
            sum_disc_price=("a", "sum"),
            sum_charge=("b", "sum"),
            avg_qty=("l_quantity", "mean"),
            avg_price=("l_extendedprice", "mean"),
            avg_disc=("l_discount", "mean"),
            count_order=("l_returnflag", "count"),
        ).reset_index()
    df = df.sort_values(by=['l_returnflag', 'l_linestatus'], ascending=[True, True])
    return df


###########################################

def q6(li):
    df = li[
        (li.l_shipdate>='1994-01-01') & 
        (li.l_shipdate<'1995-01-01') & 
        (li.l_discount>=0.050) & 
        (li.l_discount<=0.070) & 
        (li.l_quantity<24)
    ] 
    df = df[['l_shipdate', 'l_discount', 'l_quantity', 'l_extendedprice']]
    df['l_extendedpricel_discount'] = ((df.l_extendedprice) * (df.l_discount))
    res = (df.l_extendedpricel_discount).sum()
    return res

#####################################################################################

def main():

    l_columnnames = [
        'l_orderkey',
        'l_partkey',
        'l_suppkey',
        'l_linenumber',
        'l_quantity',
        'l_extendedprice',
        'l_discount',
        'l_tax',
        'l_returnflag',
        'l_linestatus',
        'l_shipdate',
        'l_commitdate',
        'l_receiptdate',
        'l_shipinstruct',
        'l_shipmode',
        'l_comment'
    ]

    l_data_types = {
        'l_orderkey': int,
        'l_partkey': int,
        'l_suppkey': int,
        'l_linenumber': int,
        'l_quantity': float,
        'l_extendedprice': float,
        'l_discount': float,
        'l_tax': float,
        'l_returnflag': str,
        'l_linestatus': str,
        'l_shipinstruct': str,
        'l_shipmode': str,
        'l_comment': str
    }

    l_parse_dates = [
        'l_shipdate',
        'l_commitdate',
        'l_receiptdate'
    ]


    print("##############################################")

    start = time.time()
    li = pd.read_table(path + "lineitem.tbl", sep="|", names=l_columnnames, dtype=l_data_types, parse_dates=l_parse_dates, index_col=False)
    end = time.time()
    print(">>> Read CSV Time: ", 1000 * (end - start), " ms")

    print("##############################################")

    print(">>> Q1 Results:")
    times = []
    for _ in range(5):
        start = time.time()
        res = q1(li)
        end = time.time()
        times.append(1000 * (end - start))
    print(res)
    print("Time: ", sum(times) / len(times), " ms")

    print("##############################################")

    print(">>> Q6 Results:")
    times = []
    for _ in range(5):
        start = time.time()
        res = q6(li)
        end = time.time()
        times.append(1000 * (end - start))
    print(res)
    print("Time: ", sum(times) / len(times), " ms")

    print("##############################################")

##########################################################

if __name__ == "__main__":
    main()
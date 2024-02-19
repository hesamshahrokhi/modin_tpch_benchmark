# modin_tpch_benchmark
A repository for comparing Pandas and Modin (Dask/Ray) performance on simple TPC-H Queries (Q1 and Q6) written in Pandas.

**How to execute the benchmark?**
* Prepare the TPC-H dataset (https://github.com/electrum/tpch-dbgen) and declare your path inside the __run.py__ file.
* Execute __run.py__ file
```
python3 run.py
```

**Sample Results**

Results of running the benchmark on the TPC-H Dataset with the Scale Factor of 1 already exist in __Results_TPCH_SF1.txt__.

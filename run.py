import os

##### TPC-H Dataset Path ######################
path = "[PUT YOUR PATH HERE]"
###############################################

os.environ["TPCH_DATASET_PATH"] = path 

print("#### Panadas Benchmark ####")
os.system("python3 tpch_pandas.py")
print("\n\n")

print("#### Modin [Ray] Benchmark ####")
os.system("python3 tpch_ray.py")
print("\n\n")

print("#### Modin [Dask] Benchmark ####")
os.system("python3 tpch_dask.py")
print("\n\n")

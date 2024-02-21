import os

##############################################################
# USING Modin v0.27.0 (https://pypi.org/project/modin/0.27.0/)
# 16 GB RAM
# 4 Physical Cores
# 2 Threads per Core
# Dataset: TPC-H SF-1 (1GB)
##############################################################


##### TPC-H Dataset Path ######################
# path = "[PUT YOUR PATH HERE]"
path = "/home/hesam/Desktop/datasets/SF-1/"
###############################################

os.environ["TPCH_DATASET_PATH"] = path 

print("#### Panadas Benchmark ####")
os.system("python3.10 tpch_pandas.py")
print("\n\n")

print("#### Modin [Ray] Benchmark ####")
os.system("python3.10 tpch_ray.py")
print("\n\n")

print("#### Modin [Dask] Benchmark ####")
os.system("python3.10 tpch_dask.py")
print("\n\n")

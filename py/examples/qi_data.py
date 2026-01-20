# %%
import os
import jpk_reader as jpk
import time

DATA_PATH = "../../data/qi_data/qi_data-2_0-lg.jpk-qi-data"
# %%
s = time.perf_counter()
reader = jpk.QIMapReader(DATA_PATH)
print(time.perf_counter() - s)
# %%
s = time.perf_counter()
data = reader.all_data()
print(time.perf_counter() - s)
# %%

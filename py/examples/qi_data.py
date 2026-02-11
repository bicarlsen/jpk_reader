# %%
import jpk_reader_rs as jpk

DATA_PATH = "../../data/scope/time-current-deflection.out"
# %%
reader = jpk.qi_map.QIMapReader(DATA_PATH)
# %%
metadata = reader.all_metadata()
# %%
data = reader.all_data()
# %%

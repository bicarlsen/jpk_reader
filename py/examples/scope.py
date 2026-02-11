# %%
import jpk_reader_rs as jpk

DATA_PATH = "../../data/scope/time-current-deflection.out"
# %%
data = jpk.scope.load_data(DATA_PATH)
# %%

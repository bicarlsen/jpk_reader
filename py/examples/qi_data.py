# %%
import jpk_reader_rs as jpk

DATA_PATH = "../../data/qi_data/qi_data-2_0-lg.jpk-qi-data"
# %%
reader = jpk.QIMapReader(DATA_PATH)
data = reader.all_data()

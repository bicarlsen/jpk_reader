# %%
import jpk_reader_rs as jpk

# DATA_PATH = "../../data/qi_data/qi_data-2_0-lg.jpk-qi-data"
DATA_PATH = "s:\\_öffentlich_TAUSCHordner\\Mitarbeitende\\carlsen_brian\\degradation\\00-preliminary\\01/carlsen-asfaw-postdegradation-data-2026.01.08-16.35.28.552.jpk-qi-data"
# %%
reader = jpk.QIMapReader(DATA_PATH)
# %%
metadata = reader.all_metadata()
# %%
data = reader.all_data()
# %%

# %%
import os
import jpk_reader_rs as jpk

# %%
DATA_PATH = "../../data/voltage-spectroscopy"
DATA_FILE = "voltage-spectroscopy.jpk-voltage-ramp"
COLLECTION_DIR = "collection"
# %%
file_data = jpk.voltage_spectroscopy.load_file(os.path.join(DATA_PATH, DATA_FILE))

# %%
collection_data = jpk.voltage_spectroscopy.load_dir(
    os.path.join(DATA_PATH, COLLECTION_DIR)
)

# %%

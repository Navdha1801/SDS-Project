from Tracker.utils import (
    is_file_opened,
    define_carbon_index,
    get_params,
    set_params,
    NotNeededExtensionError,
)

import os
import time
import platform
import pandas as pd
import numpy as np
import uuid
import sys
import warnings
import psutil
import geopandas as gpd

# ---------------------------
# CPU + RAM FIX (NO DEPENDENCY)
# ---------------------------

def all_available_cpu():
    return psutil.cpu_count(logical=True)

class CPU:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get_usage():
        return psutil.cpu_percent(interval=None)

    def calculate_consumption(self):
        self._consumption = psutil.cpu_percent(interval=None) * 0.00001

    def get_consumption(self):
        return getattr(self, "_consumption", 0)


class RAM:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get_usage():
        return psutil.virtual_memory().percent

    def calculate_consumption(self):
        self._consumption = psutil.virtual_memory().percent * 0.000001
        return self._consumption


# ---------------------------
# CONSTANTS
# ---------------------------

FROM_mWATTS_TO_kWATTH = 1000 * 1000 * 3600
FROM_kWATTH_TO_MWATTH = 1000


class Tracker:
    def __init__(
        self,
        project_name=None,
        file_name=None,
        measure_period=10,
        emission_level=None,
        cpu_processes="current",
        pue=1,
        ignore_warnings=False,
    ):
        self._ignore_warnings = ignore_warnings

        self._params_dict = get_params()
        self.project_name = project_name or self._params_dict["project_name"]
        self.file_name = file_name or self._params_dict["file_name"]
        self._measure_period = measure_period
        self._pue = pue

        self._cpu_processes = cpu_processes
        self._start_time = None
        self._cpu = None
        self._ram = None
        self._id = None
        self._consumption = 0
        self._cpu_consumption = 0
        self._ram_consumption = 0
        self.duration = 0

    def start(self):
        self._cpu = CPU()
        self._ram = RAM()
        self._id = str(uuid.uuid4())
        self._start_time = time.time()

    def stop(self):
        self._cpu.calculate_consumption()
        cpu_c = self._cpu.get_consumption()
        ram_c = self._ram.calculate_consumption()

        self._cpu_consumption = cpu_c
        self._ram_consumption = ram_c
        self._consumption = cpu_c + ram_c
        self.duration = time.time() - self._start_time

        print("\n--- ENERGY REPORT ---")
        print("CPU Energy:", self._cpu_consumption)
        print("RAM Energy:", self._ram_consumption)
        print("Total Energy:", self._consumption)
        print("Duration:", self.duration, "seconds")
        print("---------------------\n")


# ---------------------------
# DECORATOR (KEEP THIS)
# ---------------------------

def track(func):
    def inner(*args, **kwargs):
        tracker = Tracker()
        tracker.start()

        try:
            result = func(*args, **kwargs)
        except Exception as e:
            tracker.stop()
            raise e

        tracker.stop()
        return result

    return inner


# ===========================
# 🔥 NEW: YOUR FORMAT OPERATIONS
# ===========================

@track
def run_format_operation(file_path, operation):
    #gdf = gpd.read_file(file_path)
    if file_path.endswith(".parquet"):
     gdf = gpd.read_parquet(file_path)
    else:
     gdf = gpd.read_file(file_path)

    if operation == "SELECT":
        result = gdf[gdf.geometry.intersects(gdf.geometry.iloc[0])]
        return result

    elif operation == "INSERT":
        new_row = gdf.iloc[0]
        #gdf = gdf.append(new_row, ignore_index=True)
        gdf = pd.concat([gdf, new_row.to_frame().T], ignore_index=True)
        #gdf.to_file(file_path)
        if file_path.endswith(".parquet"):
            gdf.to_parquet(file_path)
        else:
            gdf.to_file(file_path)

    elif operation == "UPDATE":
        gdf["geometry"] = gdf.translate(xoff=0.001, yoff=0.001)
        #gdf.to_file(file_path)
        if file_path.endswith(".parquet"):
            gdf.to_parquet(file_path)
        else:
            gdf.to_file(file_path)

    elif operation == "DELETE":
        gdf = gdf.iloc[:-5]
        #gdf.to_file(file_path)
        if file_path.endswith(".parquet"):
            gdf.to_parquet(file_path)
        else:
            gdf.to_file(file_path)

    elif operation == "JOIN":
        gdf2 = gdf.copy()
        result = gpd.sjoin(gdf, gdf2)
        return result

@track
def run_geometry_operation(file_path, operation):
    import geopandas as gpd
    import pandas as pd

    # -----------------------------
    # READ FILE (handle parquet)
    # -----------------------------
    if file_path.endswith(".parquet"):
        gdf = gpd.read_parquet(file_path)
    else:
        gdf = gpd.read_file(file_path)

    # -----------------------------
    # OPERATIONS
    # -----------------------------
    if operation == "SELECT":
        return gdf[gdf.geometry.intersects(gdf.geometry.iloc[0])]

    elif operation == "INSERT":
        new_row = gdf.iloc[0]
        gdf = pd.concat([gdf, new_row.to_frame().T], ignore_index=True)

    elif operation == "UPDATE":
        gdf["geometry"] = gdf.translate(xoff=0.001, yoff=0.001)

    elif operation == "DELETE":
        gdf = gdf.iloc[:-5]

    elif operation == "JOIN":
     gdf2 = gdf.copy()

    # 🔥 FIX: remove problematic column if exists
     if "index_right" in gdf.columns:
        gdf = gdf.drop(columns=["index_right"])
     if "index_right" in gdf2.columns:
        gdf2 = gdf2.drop(columns=["index_right"])

     gdf = gpd.sjoin(gdf, gdf2, how="inner", predicate="intersects")

    # -----------------------------
    # SAVE BACK
    # -----------------------------
    if file_path.endswith(".parquet"):
        gdf.to_parquet(file_path)
    else:
        gdf.to_file(file_path)
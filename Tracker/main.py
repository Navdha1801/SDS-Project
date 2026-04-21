from Tracker.utils import (
    is_file_opened,
    define_carbon_index,
    get_params,
    set_params,
    NotNeededExtensionError,
)

import os
import shutil
import time
import platform
import pandas as pd
import numpy as np
import uuid
import sys
import warnings
import psutil
import geopandas as gpd
from shapely.geometry import box


# ---------------------------
# CPU + RAM
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


# ---------------------------
# TRACKER
# ---------------------------


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
        print("CPU Energy:  ", self._cpu_consumption)
        print("RAM Energy:  ", self._ram_consumption)
        print("Total Energy:", self._consumption)
        print("Duration:    ", self.duration, "seconds")
        print("---------------------\n")


# ---------------------------
# DECORATOR
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
        # Always returns (duration, total_energy, function_result)
        # function_result is None for INSERT/UPDATE/DELETE (they write back)
        # function_result is a GeoDataFrame for SELECT and JOIN
        return tracker.duration, tracker._consumption, result

    return inner


# ---------------------------
# GEOMETRY OPERATION (defined before run_experiment so it can be called by it)
# ---------------------------


@track
def run_geometry_operation(file_path, operation):

    # READ
    if file_path.endswith(".parquet"):
        gdf = gpd.read_parquet(file_path)
    else:
        gdf = gpd.read_file(file_path)

    geom_col = gdf.geometry.name

    # OPERATIONS
    if operation == "SELECT":
        minx, miny, maxx, maxy = gdf.total_bounds
        cx, cy = (minx + maxx) / 2, (miny + maxy) / 2
        half_w, half_h = (maxx - minx) / 4, (maxy - miny) / 4
        query_box = box(cx - half_w, cy - half_h, cx + half_w, cy + half_h)
        result = gdf[gdf.geometry.intersects(query_box)]
        return result

    elif operation == "INSERT":
        new_row = gdf.iloc[[0]].copy()
        gdf = pd.concat([gdf, new_row], ignore_index=True)

    elif operation == "UPDATE":
        gdf[geom_col] = gdf.geometry.translate(xoff=0.001, yoff=0.001)

    elif operation == "DELETE":
        gdf = gdf.iloc[: -min(5, len(gdf))].copy()

    elif operation == "JOIN":
        for col in ["index_right", "index_left"]:
            if col in gdf.columns:
                gdf = gdf.drop(columns=[col])

        gdf2 = gdf.copy()

        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        if gdf2.crs is None:
            gdf2 = gdf2.set_crs("EPSG:4326")

        gdf_proj = gdf.to_crs("EPSG:3857")
        gdf2_proj = gdf2.to_crs("EPSG:3857")

        sample = gdf_proj.sample(n=min(500, len(gdf_proj)), random_state=42)

        for col in ["index_right", "index_left"]:
            if col in sample.columns:
                sample = sample.drop(columns=[col])

        result = gpd.sjoin_nearest(sample, gdf2_proj, how="inner", max_distance=5000)
        return result

    # SAVE BACK — only INSERT, UPDATE, DELETE reach here
    if file_path.endswith(".parquet"):
        gdf.to_parquet(file_path)
    elif file_path.endswith(".shp"):
        gdf.to_file(file_path)
    elif file_path.endswith(".gpkg"):
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        gdf.to_file(file_path, driver="GPKG")
    elif file_path.endswith(".fgb"):
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        gdf.to_file(file_path, driver="FlatGeobuf")
    else:
        gdf.to_file(file_path)

    return None


# ---------------------------
# EXPERIMENT RUNNER
# ---------------------------

# NOTE: run_experiment is NOT wrapped with @track
# It is the outer loop that calls run_geometry_operation 20 times
# and collects the (duration, energy) tuples returned by @track


def run_experiment(file_path, operation, runs=20):
    """
    Runs run_geometry_operation `runs` times for a given file and operation.
    Restores the original file before each run for INSERT, UPDATE, DELETE
    so every run operates on identical data — same principle as DBJoules
    resetting database state between runs.
    Returns mean and std of duration and energy across all runs.
    """
    backup_path = file_path + ".bak"
    shutil.copy2(file_path, backup_path)

    times, energies = [], []

    for i in range(runs):
        if operation in ("INSERT", "UPDATE", "DELETE"):
            shutil.copy2(backup_path, file_path)

        duration, energy, _ = run_geometry_operation(file_path, operation)
        times.append(duration)
        energies.append(energy)

    shutil.copy2(backup_path, file_path)
    os.remove(backup_path)

    return {
        "operation": operation,
        "file": file_path,
        "mean_time": np.mean(times),
        "std_time": np.std(times),
        "mean_energy": np.mean(energies),
        "std_energy": np.std(energies),
    }


# ---------------------------
# OLD FORMAT OPERATION (kept for Option 1 compatibility)
# ---------------------------


@track
def run_format_operation(file_path, operation):
    if file_path.endswith(".parquet"):
        gdf = gpd.read_parquet(file_path)
    else:
        gdf = gpd.read_file(file_path)

    if operation == "SELECT":
        result = gdf[gdf.geometry.intersects(gdf.geometry.iloc[0])]
        return result

    elif operation == "INSERT":
        new_row = gdf.iloc[0]
        gdf = pd.concat([gdf, new_row.to_frame().T], ignore_index=True)
        if file_path.endswith(".parquet"):
            gdf.to_parquet(file_path)
        else:
            gdf.to_file(file_path)

    elif operation == "UPDATE":
        gdf["geometry"] = gdf.translate(xoff=0.001, yoff=0.001)
        if file_path.endswith(".parquet"):
            gdf.to_parquet(file_path)
        else:
            gdf.to_file(file_path)

    elif operation == "DELETE":
        gdf = gdf.iloc[:-5]
        if file_path.endswith(".parquet"):
            gdf.to_parquet(file_path)
        else:
            gdf.to_file(file_path)

    elif operation == "JOIN":
        gdf2 = gdf.copy()
        result = gpd.sjoin(gdf, gdf2)
        return result

    return None


@track
def run_index_operation(file_path, operation, use_index=False):
    import geopandas as gpd
    import pandas as pd

    # -----------------------------
    # READ FILE
    # -----------------------------
    if file_path.endswith(".parquet"):
        gdf = gpd.read_parquet(file_path)
    else:
        gdf = gpd.read_file(file_path)

    query_geom = gdf.geometry.iloc[0]

    # -----------------------------
    # SELECT / JOIN (INDEX MATTERS)
    # -----------------------------
    if operation == "SELECT":

        if use_index:
            # ✅ WITH INDEX
            sindex = gdf.sindex
            possible = list(sindex.intersection(query_geom.bounds))
            result = gdf.iloc[possible]
            result = result[result.geometry.intersects(query_geom)]
        else:
            # ❌ WITHOUT INDEX
            result = gdf[gdf.geometry.intersects(query_geom)]

    elif operation == "JOIN":

        gdf2 = gdf.copy()

        if use_index:
            # indexed join (approximate simulation)
            sindex = gdf2.sindex
            matches = []

            for geom in gdf.geometry:
                possible = list(sindex.intersection(geom.bounds))
                matches.append(len(possible))
        else:
            # full scan join
            _ = gpd.sjoin(gdf, gdf2, how="inner", predicate="intersects")

    # -----------------------------
    # OTHER OPERATIONS (same)
    # -----------------------------
    elif operation == "INSERT":
        new_row = gdf.iloc[0]
        gdf = pd.concat([gdf, new_row.to_frame().T], ignore_index=True)

    elif operation == "UPDATE":
        gdf["geometry"] = gdf.translate(xoff=0.001, yoff=0.001)

    elif operation == "DELETE":
        gdf = gdf.iloc[:-5]

    return True

@track
def run_compression_operation(file_path, operation):

    import geopandas as gpd
    import pandas as pd
    import gzip

    # -----------------------------
    # READ FILE (handle compression)
    # -----------------------------
    if file_path.endswith(".parquet"):
        gdf = gpd.read_parquet(file_path)

    elif file_path.endswith(".gz"):
        with gzip.open(file_path, 'rb') as f:
            gdf = gpd.read_file(f)

    else:
        gdf = gpd.read_file(file_path)

    query_geom = gdf.geometry.iloc[0]

    # -----------------------------
    # OPERATIONS
    # -----------------------------
    if operation == "SELECT":
        _ = gdf[gdf.geometry.intersects(query_geom)]

    elif operation == "JOIN":
        _ = gpd.sjoin(gdf, gdf.copy(), how="inner", predicate="intersects")

    elif operation == "INSERT":
        new_row = gdf.iloc[0]
        gdf = pd.concat([gdf, new_row.to_frame().T], ignore_index=True)

    elif operation == "UPDATE":
        gdf["geometry"] = gdf.translate(xoff=0.001, yoff=0.001)

    elif operation == "DELETE":
        gdf = gdf.iloc[:-5]

    return True
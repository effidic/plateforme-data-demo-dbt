import os
import hashlib
from typing import List
import platform
import logging


CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
DBT_BASE_PATH = os.path.join(CURRENT_PATH, os.pardir, os.pardir)
DBT_MODELS_PATH = os.path.join(DBT_BASE_PATH, "models")
DBT_SNAPSHOTS_PATH = os.path.join(DBT_BASE_PATH, "snapshots")
DBT_MACROS_PATH = os.path.join(DBT_BASE_PATH, "macros")
DBT_SOURCES_PATH = os.path.join(DBT_MODELS_PATH, "0_sources")
DBT_TARGET_PATH = os.path.join(DBT_BASE_PATH, "target")
DBT_ARCHIVES_PATH = "0_archives"
DBT_PROJECT_NAME = "plateforme_data_demo"

DBT_FLAG_PATH = os.path.join(DBT_BASE_PATH, "tests", "flag")
DBT_FLAG_DOC = os.path.join(DBT_FLAG_PATH, "flag_doc")
DBT_FLAG_BUILD = os.path.join(DBT_FLAG_PATH, "flag_build")
DBT_FLAG_GENERATE_DOC = os.path.join(DBT_FLAG_PATH, "flag_generate_doc")

VENV = "venv"
if platform.system() == "Windows":
    DBT_PATH_VENV = os.path.join(DBT_BASE_PATH, VENV, "Scripts", "dbt")
    SQLFLUFF_PATH_VENV = os.path.join(DBT_BASE_PATH, VENV, "Scripts", "sqlfluff")
else:
    DBT_PATH_VENV = os.path.join(DBT_BASE_PATH, VENV, "bin", "dbt")
    SQLFLUFF_PATH_VENV = os.path.join(DBT_BASE_PATH, VENV, "Scripts", "sqlfluff")

# Logger

# create logger
logger = logging.getLogger("dbt_toolbox")
logger.setLevel(os.environ.get('LOGGER_LEVEL', logging.INFO))
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter

def logger_set_debug():
    formatter = logging.Formatter(
        "[%(asctime)s][%(levelname)s][%(filename)s::%(funcName)s::%(lineno)s] %(message)s"
    )
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
    logger.setLevel(logging.DEBUG)

def logger_set_info():
    formatter = logging.Formatter("%(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
    logger.setLevel(logging.INFO)

# Global util function (can be used for different python projects)


def merge_dict(d1: dict, d2: dict) -> dict:
    return {**d1, **d2}


def get_relative_path(abs_path: str) -> str:
    return os.path.relpath(abs_path, DBT_BASE_PATH)


def get_abs_path(relative_path: str) -> str:
    return os.path.join(DBT_BASE_PATH, relative_path)


def checksum_md5_file(path_file: str):
    hash_md5 = hashlib.md5()
    with open(path_file, "r", encoding='utf-8') as f:
        hash_md5.update(f.read().encode('utf-8'))
    return hash_md5.hexdigest()


def checksum_md5_folder(path_folder: str, file_ext=[".sql", ".yml"]):
    hash_md5 = hashlib.md5()
    for f_path in list_files(path_folder, file_ext):
        with open(f_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
    return hash_md5.hexdigest()


def checksum_md5_files(path_folder: str, file_ext=[".sql", ".yml"]) -> dict:
    checksum_md5_files = {}
    for f_path in list_files(path_folder, file_ext):
        checksum_md5_files[f_path] = checksum_md5_file(f_path)
    return checksum_md5_files


def path_to_file_name(path_file: str) -> str:
    path_file = os.path.normpath(path_file)
    sub_paths = path_file.split(os.sep)
    return "_".join(sub_paths)


def list_files(path_folder: str, file_ext=[".sql", ".yml"]) -> List[str]:
    list_files = []
    for root, dirs, files in os.walk(path_folder):
        for f_name in files:
            f_path = os.path.join(root, f_name)
            ext = os.path.splitext(f_name)[1]  # Get file extension
            if ext.lower() not in file_ext:
                continue
            list_files.append(f_path)
    return list_files

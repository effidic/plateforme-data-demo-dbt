import os
import subprocess
import json
import re
from typing import List, Tuple
from functools import lru_cache



from global_util import (
    DBT_FLAG_DOC,
    DBT_FLAG_BUILD,
    DBT_MODELS_PATH,
    DBT_SNAPSHOTS_PATH,
    DBT_FLAG_GENERATE_DOC,
    DBT_SOURCES_PATH,
    DBT_ARCHIVES_PATH,
    DBT_MACROS_PATH,
    DBT_PATH_VENV,
)
from global_util import (
    checksum_md5_file,
    get_abs_path,
    get_relative_path,
    checksum_md5_files,
    path_to_file_name,
    merge_dict,
)
from global_util import logger


class DBTUtil:
    """Dbt util"""

    @staticmethod
    @lru_cache
    def get_metadata_models(model_names: Tuple[str]) -> dict:
        metadata_models = {}
        model_names_str = ' '.join(model_names)
        cmd = f"{DBT_PATH_VENV} ls --select {model_names_str} --output json"
        logger.debug({"cmd": cmd})
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error during runing dbt command:\n{result}")
        #logger.debug({"result.stdout": result.stdout})
        try:
            list_json_str = re.findall(r"\{.*\}", result.stdout)
            for json_str in list_json_str:
                resource_type_json = json.loads(json_str)
                resource_type = resource_type_json['resource_type']
                # on exclut les tests listés puisque ce ne sont pas des modeles
                if resource_type !='test':
                    metadata_model = json.loads(json_str)
                    metadata_models[metadata_model['name']] = metadata_model
        except Exception as e:
            raise NotImplementedError(
                f"check_model_exist - case not handle: \n{result.stdout}\n{e}"
            )
        return metadata_models

    @staticmethod
    def get_metadata_model(model_name: str) -> dict:
        cmd = f"{DBT_PATH_VENV} ls --select {model_name} --output json"
        logger.debug({"cmd": cmd})
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error during runing dbt command:\n{result}")
        logger.debug({"result.stdout": result.stdout})
        try:
            json_str = re.findall(r"\{.*\}", result.stdout)[0]
            metadata_model = json.loads(json_str)
        except Exception as e:
            raise NotImplementedError(
                f"check_model_exist - case not handle: \n{result.stdout}\n{e}"
            )
        return metadata_model

    @staticmethod
    def check_model_exist(model_name: str):
        cmd = f"{DBT_PATH_VENV} ls --select {model_name} --output json"
        logger.debug({"cmd": cmd})
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error during runing dbt command:\n{result}")
        logger.debug({"result.stdout": result.stdout})
        if "does not match" in result.stdout:
            return False
        else:
            return True

    @staticmethod
    def flag_build_model_cmd(model_sql_rel_path: str):
        """Save checksum of the .sql model, when the command "invoke build-model" is executed"""
        # Save checksum .sql file
        md5_checksum = checksum_md5_file(get_abs_path(model_sql_rel_path))
        model_file = os.path.basename(model_sql_rel_path)
        checksum_file = f"{model_file}.md5"
        f_path = os.path.join(DBT_FLAG_BUILD, checksum_file)
        logger.debug({"f_path": f_path, "md5_checksum": md5_checksum})
        with open(f_path, "w") as f:
            f.write(md5_checksum)


    @staticmethod
    def flag_doc_model_cmd(model_yml_rel_path: str, model_sql_rel_path: str):
        """Save checksum of the model (.yml & .sql), when the command "invoke doc-model" is executed"""
        # Save checksum .yml file
        md5_checksum = checksum_md5_file(get_abs_path(model_yml_rel_path))
        model_file = os.path.basename(model_yml_rel_path)
        checksum_file = f"{model_file}.md5"
        f_path = os.path.join(DBT_FLAG_DOC, checksum_file)
        logger.debug({"f_path": f_path, "md5_checksum": md5_checksum})
        with open(f_path, "w") as f:
            f.write(md5_checksum)
        # Save checksum .sql file
        md5_checksum = checksum_md5_file(get_abs_path(model_sql_rel_path))
        model_file = os.path.basename(model_sql_rel_path)
        checksum_file = f"{model_file}.md5"
        f_path = os.path.join(DBT_FLAG_DOC, checksum_file)
        logger.debug({"f_path": f_path, "md5_checksum": md5_checksum})
        with open(f_path, "w") as f:
            f.write(md5_checksum)

    @staticmethod
    def flag_generate_doc_cmd():
        """Save checksums of all .yml files, when the command "invoke generate-doc" is executed"""
        expected_checksum_yml_models = checksum_md5_files(DBT_MODELS_PATH, [".yml"])
        expected_checksum_yml_macros = checksum_md5_files(DBT_MACROS_PATH, [".yml"])
        expected_checksum_yml = merge_dict(
            expected_checksum_yml_models, expected_checksum_yml_macros
        )
        logger.debug({"expected_checksum_yml": expected_checksum_yml})
        for abs_path, checksum in expected_checksum_yml.items():
            rel_path = get_relative_path(abs_path)
            checksum_file = path_to_file_name(rel_path) + ".md5"
            f_path = os.path.join(DBT_FLAG_GENERATE_DOC, checksum_file)
            with open(f_path, "w") as f:
                f.write(checksum)

    @staticmethod
    def get_missing_build_models() -> List[str]:
        expected_checksum_sql_models = {**DBTUtil.get_expected_checksum_models(DBT_SNAPSHOTS_PATH, [".sql"]) 
        , **DBTUtil.get_expected_checksum_models( DBT_MODELS_PATH, [".sql"] ) }
        actual_checksum_sql_models = DBTUtil.get_actual_checksum_models(DBT_FLAG_BUILD, [".sql"] )
        logger.debug({"expected_checksum_sql_models": expected_checksum_sql_models})
        logger.debug({"actual_checksum_sql_models": actual_checksum_sql_models})
        missing_run_models = []
        for model_name, expected_checksum in expected_checksum_sql_models.items():
            if (
                model_name not in actual_checksum_sql_models
                or actual_checksum_sql_models[model_name] != expected_checksum
            ):
                missing_run_models.append(model_name)
        return missing_run_models

    @staticmethod
    def get_missing_doc_models() -> List[str]: 
        missing_doc_models = []
        for ext in [".sql", ".yml"]:
            expected_checksum_models = {**DBTUtil.get_expected_checksum_models(DBT_SNAPSHOTS_PATH, [ext]) 
            , **DBTUtil.get_expected_checksum_models( DBT_MODELS_PATH, [ext] ) }
            actual_checksum_models = DBTUtil.get_actual_checksum_models(DBT_FLAG_DOC, [ext] )
            logger.debug(
                {
                    "expected_checksum_models": expected_checksum_models,
                    "actual_checksum_models": actual_checksum_models,
                }
            )
            for model_name, expected_checksum in expected_checksum_models.items():
                if (
                    model_name not in actual_checksum_models
                    or actual_checksum_models[model_name] != expected_checksum
                ):
                    missing_doc_models.append(model_name)
        missing_doc_models = list(set(missing_doc_models))  # Remove duplicate
        return missing_doc_models

    @staticmethod
    def clean_flag_files():
        expected_checksum_sql_models = {**DBTUtil.get_expected_checksum_models(
            DBT_MODELS_PATH, [".sql"]
        ) , **DBTUtil.get_expected_checksum_models(
            DBT_SNAPSHOTS_PATH, [".sql"]
        ) }
        for f_name in os.listdir(DBT_FLAG_BUILD):
            f_path = os.path.join(DBT_FLAG_BUILD, f_name)
            model_name = f_name.split(".")[0]
            if model_name not in expected_checksum_sql_models:
                os.remove(f_path)

        expected_checksum_yml_models = {**DBTUtil.get_expected_checksum_models(
            DBT_MODELS_PATH, [".yml"]
        ) , **DBTUtil.get_expected_checksum_models(
            DBT_SNAPSHOTS_PATH, [".yml"]
        ) }
        for f_name in os.listdir(DBT_FLAG_DOC):
            f_path = os.path.join(DBT_FLAG_DOC, f_name)
            model_name = f_name.split(".")[0]
            if model_name not in expected_checksum_yml_models:
                os.remove(f_path)

        # generate_doc
        expected_checksum_yml_models = checksum_md5_files(DBT_MODELS_PATH, [".yml"])
        expected_checksum_yml_macros = checksum_md5_files(DBT_MACROS_PATH, [".yml"])
        expected_checksum_yml = {}
        for abs_path, checksum in merge_dict(
            expected_checksum_yml_models, expected_checksum_yml_macros
        ).items():
            rel_path = get_relative_path(abs_path)
            checksum_file = path_to_file_name(rel_path) + ".md5"
            expected_checksum_yml[checksum_file] = checksum
        for f_name in os.listdir(DBT_FLAG_GENERATE_DOC):
            f_path = os.path.join(DBT_FLAG_GENERATE_DOC, f_name)
            if f_name not in expected_checksum_yml:
                os.remove(f_path)

    @staticmethod
    def get_expected_checksum_models(path_folder: str, file_ext: List[str]) -> dict:
        checksum_sql_files = checksum_md5_files(path_folder, file_ext=file_ext)
        # Transform abs path key to relative path
        expected_checksum_models = {}
        for abs_path, checksum in checksum_sql_files.items():
            model_name = os.path.basename(abs_path).split(".")[0]
            if DBT_SOURCES_PATH in abs_path or DBT_ARCHIVES_PATH in abs_path:  # Ignore
                continue
            expected_checksum_models[model_name] = checksum
        return expected_checksum_models

    @staticmethod
    def get_actual_checksum_models(path_folder: str, file_ext: List[str]) -> dict:
        actual_checksum_models = {}
        for f_name in os.listdir(path_folder):
            model_name, ext = os.path.splitext(f_name.replace(".md5", ""))
            if ext not in file_ext:
                continue
            f_path = os.path.join(path_folder, f_name)
            with open(f_path, "r") as f:
                checksum = f.read()
            actual_checksum_models[model_name] = checksum
        return actual_checksum_models

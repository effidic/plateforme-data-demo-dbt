import yaml
import os
from typing import List

from global_util import get_relative_path, get_abs_path
from global_util import logger


class YMLModelUtil:
    """YML model util"""

    @staticmethod
    def get_default_yml_model(db_table: str, name_columns: List[str], resource_type: str) -> dict:
        yml_columns = []
        for name_column in name_columns:
            yml_column = {}
            yml_column["name"] = name_column
            yml_column["description"] = ""
            yml_columns.append(yml_column)

        yml_model = {}
        yml_model["name"] = db_table
        yml_model["description"] = ""
        yml_model["columns"] = yml_columns

        yml_models = {}
        yml_models["version"] = 2
        if resource_type == 'snapshot':
            yml_models["snapshots"] = [yml_model]
        else:
            yml_models["models"] = [yml_model]
        logger.debug({"yml_models": yml_models})
        return yml_models

    @staticmethod
    def merge_yml_model(yml_models: str, name_columns: List[str], resource_type: str) -> dict:
        logger.debug(f"yml_models: {yml_models}")
        logger.debug(f"name_columns: {name_columns}")
        logger.debug(f"resource_type: {resource_type}")
        if resource_type == 'snapshot':
            if "columns" in yml_models["snapshots"][0]:
                actual_yml_columns = yml_models["snapshots"][0]["columns"]
            else:
                actual_yml_columns = []
        else:
            if "columns" in yml_models["models"][0]:
                actual_yml_columns = yml_models["models"][0]["columns"]
            else:
                actual_yml_columns = []
        logger.debug(f"actual_yml_columns: {actual_yml_columns}")
        actual_columns = {c["name"]: c for c in actual_yml_columns}
        yml_columns = []
        for name_column in name_columns:
            yml_column = {}
            if name_column in actual_columns:  # Existing value
                yml_column = actual_columns[name_column]
            else:  # Default value
                yml_column["name"] = name_column
                yml_column["description"] = ""
            yml_columns.append(yml_column)
        if resource_type == 'snapshot':
            yml_models["snapshots"][0]["columns"] = yml_columns
        else:
            yml_models["models"][0]["columns"] = yml_columns
        logger.debug({"yml_models": yml_models})
        return yml_models

    @staticmethod
    def write_yml_model(
        model_name: str, model_yml_rel_path: str, name_columns: List[str], resource_type: str
    ):
        yml_models = {}
        path_yml_model = get_abs_path(model_yml_rel_path)

        if os.path.exists(path_yml_model):  # The .yml file for the model exist
            with open(path_yml_model, "r") as f:
                yml_models = yaml.safe_load(f.read())
            yml_models = YMLModelUtil.merge_yml_model(yml_models, name_columns, resource_type=resource_type)
        else:  # The .yml file for the source doesn't exist
            yml_models = YMLModelUtil.get_default_yml_model(
                db_table=model_name, name_columns=name_columns, resource_type=resource_type
            )

        # Write the .yml source content
        parent_dir = os.path.dirname(path_yml_model)
        os.makedirs(parent_dir, exist_ok=True)
        with open(path_yml_model, "w") as f:
            f.write(yaml.dump(yml_models, allow_unicode=True, sort_keys=False))

        return get_relative_path(path_yml_model)

    @staticmethod
    def is_empty_description_in_model(yml_model: dict) -> bool:
        empty_description = False
        model = yml_model["models"][0]
        if "description" not in model or model["description"] in ("", " ", None):
            empty_description = True
        if "columns" in model:
          for col in model["columns"]:
              if "description" not in col or col["description"] in ("", " ", None):
                  empty_description = True
        else:
            empty_description = True
        return empty_description

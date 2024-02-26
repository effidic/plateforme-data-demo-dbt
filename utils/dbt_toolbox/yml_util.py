import yaml
import os
from typing import List

from global_util import DBT_MODELS_PATH, DBT_SOURCES_PATH, DBT_ARCHIVES_PATH, list_files, get_relative_path
from yml_source_util import YMLSourceUtil
from yml_model_util import YMLModelUtil
from global_util import logger



class YMLUtil:
    @staticmethod
    def load_yml_file(path_file: str) -> dict:
        with open(path_file, "r") as f:
            yml_content = yaml.safe_load(f.read())
        return yml_content

    @staticmethod
    def write_yml_file(path_file: str, yml_content: dict):
        with open(path_file, "w") as f:
            f.write(yaml.dump(yml_content, sort_keys=False))

    @staticmethod
    def get_yml_files_with_empty_description() -> List[str]:
        empty_description_yml_files = []
        yml_files = list_files(DBT_MODELS_PATH, [".yml"])
        for f_path in yml_files:
            logger.debug({"yml_files": f_path})
            yml_content = YMLUtil.load_yml_file(f_path)
            try:
                if DBT_SOURCES_PATH in f_path:  # sources
                    logger.debug(f"we don't check source yml files...: {f_path}")
                    #if YMLSourceUtil.is_empty_description_in_source(yl_content):
                    #    empty_description_yml_files.append(get_relative_path(f_path))
                elif DBT_ARCHIVES_PATH in f_path:  # archives
                    logger.debug(f"we don't check archives yml files...: {f_path}")
                else:  # models
                    if YMLModelUtil.is_empty_description_in_model(yml_content):
                        empty_description_yml_files.append(get_relative_path(f_path))
            except Exception as e:
                raise Exception(
                    f"The format of your .yml file ({f_path}) is not good.\n Technical error: {e}"
                )
        logger.debug({"empty_description_yml_files": empty_description_yml_files})
#        return empty_description_yml_files
        return []

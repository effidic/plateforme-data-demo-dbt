import yaml
import os
from typing import List

from global_util import DBT_BASE_PATH, DBT_SOURCES_PATH, get_relative_path
from global_util import logger


class YMLSourceUtil:
    @staticmethod
    def get_path_yml_source(
        db_database: str, db_schema: str, db_table: str
    ) -> str:
        file_name = f"src_{db_schema.lower()}__{db_table.lower()}.yml"
        path = os.path.join(DBT_SOURCES_PATH, db_schema, file_name)
        logger.debug({"path": path})
        return path

    @staticmethod
    def get_default_yml_source(
        db_database: str,
        db_schema: str,
        db_table: str,
        name_columns: List[str],
    ) -> dict:

        yml_columns = []
        for name_column in name_columns:
            yml_column = {}
            yml_column["name"] = name_column
            yml_column["description"] = ""
            yml_columns.append(yml_column)

        yml_quoting = {}
        yml_quoting["database"] = False
        yml_quoting["schema"] = True
        yml_quoting["identifier"] = True

        yml_table = {}
        yml_table["name"] = db_table
        yml_table["description"] = ""
        yml_table["columns"] = yml_columns
        
        yml_source = {}
        yml_source["name"] = db_schema
        yml_source["schema"] = db_schema
        yml_source["database"] = "\"{{ env_var('KEYFILE_PROJECT_ID')}}\""
        yml_source["quoting"] = yml_quoting
        yml_source["tables"] = [yml_table]

        yml_sources = {}
        yml_sources["version"] = 2
        yml_sources["sources"] = [yml_source]
        logger.debug({"yml_sources": yml_sources})
        return yml_sources

    @staticmethod
    def merge_yml_source(yml_sources: str, name_columns: List[str]) -> dict:
        actual_yml_columns = yml_sources["sources"][0]["tables"][0]["columns"]
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
        yml_sources["sources"][0]["tables"][0]["columns"] = yml_columns
        logger.debug({"yml_sources": yml_sources})
        return yml_sources

    @staticmethod
    def write_yml_source(
        db_database: str,
        db_schema: str,
        db_table: str,
        name_columns: List[str],
    ) -> str:
        yml_sources = {}
        path_yml_source = YMLSourceUtil.get_path_yml_source(
            db_database, db_schema, db_table
        )
        if os.path.exists(path_yml_source):  # The .yml file for the source exist
            with open(path_yml_source, "r") as f:
                yml_sources = yaml.safe_load(f.read())
            yml_sources = YMLSourceUtil.merge_yml_source(yml_sources, name_columns)
        else:  # The .yml file for the source doesn't exist
            yml_sources = YMLSourceUtil.get_default_yml_source(
                db_database,
                db_schema,
                db_table,
                name_columns,
            )

        # Write the .yml source content
        parent_dir = os.path.dirname(path_yml_source)
        os.makedirs(parent_dir, exist_ok=True)
        with open(path_yml_source, "w") as f:
            f.write(yaml.dump(yml_sources, sort_keys=False))

        return get_relative_path(path_yml_source)

    @staticmethod
    def is_empty_description_in_source(yml_source: dict) -> bool:
        empty_description = False
        table = yml_source["sources"][0]["tables"][0]
        if "description" not in table or table["description"] in ("", " ", None):
            empty_description = True
        for col in table["columns"]:
            if "description" not in col or col["description"] in ("", " ", None):
                empty_description = True
        return empty_description

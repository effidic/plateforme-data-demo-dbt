"""
Utily commands for dbt:
- tasks.py
- utils/dbt_toolbox/tasks.py
"""

from invoke import task, Collection
import shutil
import os
import platform
import sys
import pyperclip
from dotenv import load_dotenv
import sys


# Import dbt_toolbox project
from database_util import DatabaseUtil
from dbt_util import DBTUtil
from yml_model_util import YMLModelUtil
from yml_source_util import YMLSourceUtil
from yml_util import YMLUtil
from global_util import logger, DBT_BASE_PATH, DBT_TARGET_PATH, DBT_PROJECT_NAME, get_abs_path, logger_set_debug, logger_set_info


# Global variables
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
PYTHON_VERSION = 3.8
VENV = "venv"
if platform.system() == "Windows":
    DBT_PATH_VENV = os.path.join(DBT_BASE_PATH, VENV, "Scripts", "dbt")
    SQLFLUFF_PATH_VENV = os.path.join(DBT_BASE_PATH, VENV, "Scripts", "sqlfluff")
    SQLFLUFF_CONFIG_PATH = os.path.join(DBT_BASE_PATH, ".sqlfluff")
else:
    DBT_PATH_VENV = os.path.join(DBT_BASE_PATH, VENV, "bin", "dbt")
    SQLFLUFF_PATH_VENV = os.path.join(DBT_BASE_PATH, VENV, "bin", "sqlfluff")
    SQLFLUFF_CONFIG_PATH = os.path.join(DBT_BASE_PATH, ".sqlfluff")


# Set environment variables
os.environ["DBT_PROFILES_DIR"] = DBT_BASE_PATH
load_dotenv(os.path.join(DBT_BASE_PATH, '.env'), override=True)  # Load environment variables from .env file


@task
def debug(c):
    """Technical task - to activate log if the debug option is specified"""
    command_args = sys.argv[1:]
    if "-d" in command_args or "--debug" in command_args:
        logger_set_debug()
        logger.debug('Activate debug mode')
    else:
        logger_set_info()
        logger.setLevel("INFO")


@task(pre=[debug])
def compile_all(c):
    """
    Compile all dbt models to SQL files, available in target/compiled directory

    Example:
            invoke compile-all
    """
    compiled_path = os.path.join("target", "compiled")
    shutil.rmtree(compiled_path, ignore_errors=True)
    c.run(f'"{DBT_PATH_VENV}" compile')
    logger.info(f"✅ Success compile-all")


@task(pre=[debug], help={"model": "dbt model name"})
def compile_model(c, model):
    """
    Transform a dbt model to SQL, print the compiled SQL in the terminal and copy the content of the SQL
    (after that, you can paste the code in your SQL client with `Ctrl+v` or `⌘+V`)

    Example:
            invoke compile-model acq_henner_apporteur
    """
    if not DBTUtil.check_model_exist(model):
        logger.info(f'❌ Error compile-model - The model "{model}" does not exist')
        exit(1)
    c.run(f'"{DBT_PATH_VENV}" compile --select {model}', hide="out")
    metadata_model = DBTUtil.get_metadata_model(model)
    model_path = metadata_model["original_file_path"]

    compiled_model_path = os.path.join(
        "target", "compiled", DBT_PROJECT_NAME, model_path
    )
    with open(get_abs_path(compiled_model_path), "r") as f:
        content_sql = f.read()
    logger.info(f"\n{content_sql}\n")
    pyperclip.copy(content_sql)  # Copy to the clipboard



@task(pre=[debug], help={"model": "dbt model name"})
def ls_model(c, model):
        cmd = f'"{DBT_PATH_VENV}" ls --select {model} --output json'
        logger.info(cmd)
        logger.debug(cmd)
        logger.info("*********************"+cmd)
        c.run(cmd)

@task(pre=[debug], help={"models": "dbt model names separated by space"})
def lint_model(c, models):
    models = models.split(",")
    logger.info('Start lint model(s)')
    metadata_models = DBTUtil.get_metadata_models(tuple(models))
    logger.info("************"+' '.join(metadata_models))
    abs_path_models = []       
    for model_name, metadata_model in metadata_models.items():
            model_sql_rel_path = metadata_model["original_file_path"]
            abs_path_model = get_abs_path(model_sql_rel_path)
            abs_path_models.append(abs_path_model)

    abs_path_models_str = ' '.join(abs_path_models)
    cmd_lint = f'"{SQLFLUFF_PATH_VENV}" fix {abs_path_models_str} -f --ignore parsing --config {SQLFLUFF_CONFIG_PATH}'
    logger.debug(cmd_lint)
    result = c.run(cmd_lint,hide=True,warn=True,)
    logger.debug(result.stdout)
    logger.info('Success lint model(s)')

@task(
    pre=[debug],
    help={
        "models": "dbt model name(s)",
        "nb_children": "number children degrees to run (https://docs.getdbt.com/reference/node-selection/graph-operators)"
    },
)
def build_doc_model(c, models, nb_children=1, bl_test = True):
    """
    Run a dbt model, and all the models that depend on this model (+1).
    It will also Lint the SQL file.
    Finally it documents the yml file

    Example:
            invoke build-doc-model dwh_f_facture
    """
    build_model(c, models)
    doc_model(c, models, bl_build = False)

@task(
    pre=[debug],
    help={
        "models": "dbt model name(s)",
        "nb_children": "number children degrees to run (https://docs.getdbt.com/reference/node-selection/graph-operators)"
    },
)
def build_model(c, models, nb_children=1, bl_test = True):
    """
    Run a dbt model, and all the models that depend on this model (+1).
    It will also Lint the SQL file.

    Example:
            invoke run-model acq_henner_apporteur
    """
    try:
        #shutil.rmtree(DBT_TARGET_PATH, ignore_errors=True)  # Handle strange behaviour 
        models = models.split(",")
        logger.info("**************"+' '.join(models))
        run_args = [f"{model}+{nb_children}" for model in models]
        run_args_str = " ".join(run_args)
        run_test = [f"{model}" for model in models] # Test shall not be launched for child models
        run_test_str = " ".join(run_test)
        try:
            if bl_test:
                cmd = f'"{DBT_PATH_VENV}" --cache-selected-only --fail-fast build --select {run_args_str} --resource-type model snapshot && "{DBT_PATH_VENV}" test --select {run_test_str}'
            else:
                cmd = f'"{DBT_PATH_VENV}" --cache-selected-only --fail-fast build --select {run_args_str} --resource-type model snapshot'
            logger.info(cmd)
            logger.debug(cmd)
            logger.info("*********************"+cmd)
            c.run(cmd)
        except Exception as e:
            raise Exception(f'"Error during build model"')
        
        metadata_models = DBTUtil.get_metadata_models(tuple(models))
        logger.info("************"+' '.join(metadata_models))
        abs_path_models = []       
        for model_name, metadata_model in metadata_models.items():
            model_sql_rel_path = metadata_model["original_file_path"]
            abs_path_model = get_abs_path(model_sql_rel_path)
            abs_path_models.append(abs_path_model)

        
        # Only lint and flag model run if childrens models are also tested
        if nb_children >= 1: 
            # Lint SQL files
            logger.info('Start lint model(s)')
            abs_path_models_str = ' '.join(abs_path_models)
            cmd_lint = f'"{SQLFLUFF_PATH_VENV}" fix {abs_path_models_str} -f --ignore parsing --config {SQLFLUFF_CONFIG_PATH}'
            logger.debug(cmd_lint)
            result = c.run(cmd_lint,hide=True,warn=True,)
            logger.debug(result.stdout)
            logger.info('Success lint model(s)')

            for model_name, metadata_model in metadata_models.items():
                model_sql_rel_path = metadata_model["original_file_path"]
                DBTUtil.flag_build_model_cmd(model_sql_rel_path)
        logger.info(f"✅ Success build-model")
        
    except Exception as e:
        logger.info(f"❌ Error build-model - {e}")
        
        exit(1)


@task(
    pre=[debug],
    help={
        "models": "dbt model name(s)",
        "nb_children": "number children degrees to run (https://docs.getdbt.com/reference/node-selection/graph-operators)"
    },
)
def fake_build_model(c, models, nb_children=1):
    """
    Fake, cause does not run model (to save cloud warehouse cost)
    Run a dbt model, and all the models that depend on this model (+1).
    It will also Lint the SQL file.

    Example:
            invoke run-model acq_henner_apporteur
    """
    try:
        shutil.rmtree(DBT_TARGET_PATH, ignore_errors=True)  # Handle strange behaviour 
        models = models.split(",")
        logger.info("**************"+' '.join(models))
        run_args = [f"{model}+{nb_children}" for model in models]
        run_args_str = " ".join(run_args)
        try:
            cmd = f'"{DBT_PATH_VENV}" build --select {run_args_str}'
            logger.info(cmd)
            logger.debug(cmd)
            logger.info("*********************"+cmd)
            #c.run(cmd)
        except Exception as e:
            raise Exception(f'"Error during build model"')
        
        metadata_models = DBTUtil.get_metadata_models(tuple(models))
        logger.info("************"+' '.join(metadata_models))
        abs_path_models = []       
        for model_name, metadata_model in metadata_models.items():
            model_sql_rel_path = metadata_model["original_file_path"]
            abs_path_model = get_abs_path(model_sql_rel_path)
            abs_path_models.append(abs_path_model)

        
        # Only lint and flag model run if childrens models are also tested
        if nb_children >= 1: 
            # Lint SQL files
            logger.info('Start lint model(s)')
            abs_path_models_str = ' '.join(abs_path_models)
            cmd_lint = f'"{SQLFLUFF_PATH_VENV}" fix {abs_path_models_str} -f --ignore parsing --config {SQLFLUFF_CONFIG_PATH}'
            logger.debug(cmd_lint)
            result = c.run(cmd_lint,hide=True,warn=True,)
            logger.debug(result.stdout)
            logger.info('Success lint model(s)')

            for model_name, metadata_model in metadata_models.items():
                model_sql_rel_path = metadata_model["original_file_path"]
                DBTUtil.flag_build_model_cmd(model_sql_rel_path)
        logger.info(f"✅ Success build-model")
        
    except Exception as e:
        logger.info(f"❌ Error build-model - {e}")
        
        exit(1)


@ task(pre=[debug], help={"models": "dbt model name(s)"})
def doc_model(c, models, bl_build = True):
    """
    Generate yml content for a model, create or update the .yml file
    If the file already exist, merge existing values with current table structure
    This command will also run the model.

    Example:
            invoke doc-model acq_henner_apporteur
    """
    if bl_build:
        build_model(c, models, nb_children=0, bl_test=False)
    models = models.split(",")
    metadata_models = DBTUtil.get_metadata_models(tuple(models))
    ephemeral_models     = [ metadata_model for model,metadata_model in metadata_models.items() if metadata_model['config']['materialized'] == 'ephemeral' ]
    non_ephemeral_models = [ metadata_model for model,metadata_model in metadata_models.items() if metadata_model['config']['materialized'] != 'ephemeral' ]
    type_list = [ metadata_model['config']['materialized'] for model,metadata_model in metadata_models.items() ]
    logger.debug(f"type list : {type_list}")
    metadata_models = []
    ephemeral_models_temp = []
    non_ephemeral_models_temp = []
    if ephemeral_models:
        ephemeral_models_temp = DatabaseUtil.bulk_columns_ephemeral( ephemeral_models )
    if non_ephemeral_models:
        non_ephemeral_models_temp = DatabaseUtil.bulk_get_table_name_columns( non_ephemeral_models  )
    logger.info(f"{len(ephemeral_models_temp)} ephemeral model / {len(non_ephemeral_models_temp)} non ephemeral model")
    metadata_models = ephemeral_models_temp + non_ephemeral_models_temp
    for metadata_model in metadata_models:
        logger.debug(f"model to do : {metadata_model}")
        resource_type = metadata_model['resource_type']
        model_sql_rel_path = metadata_model["original_file_path"]
        model_yml_rel_path = model_sql_rel_path.replace(".sql", ".yml")            
        yml_path = YMLModelUtil.write_yml_model(
            model_name=metadata_model['name'],
            model_yml_rel_path=model_yml_rel_path,
            name_columns=metadata_model['retrieved_columns'] ,
            resource_type = resource_type
        )
        DBTUtil.flag_doc_model_cmd(model_yml_rel_path, model_sql_rel_path)
        logger.info(f"File created/updated: {yml_path}")
    logger.info(f"✅ Success doc-model")

@ task(pre=[debug], help={"models": "dbt model name(s)"})
def fake_doc_model(c, models):
    """
    Fake, cause does not run model before doc (to save cloud warehouse cost)
    Generate yml content for a model, create or update the .yml file
    If the file already exist, merge existing values with current table structure
    This command will also run the model.

    Example:
            invoke fake-doc-model acq_henner_apporteur
    """
    fake_build_model(c, models, nb_children=0)
    models = models.split(",")
    metadata_models = DBTUtil.get_metadata_models(tuple(models))
    ephemeral_models     = [ metadata_model for model,metadata_model in metadata_models.items() if metadata_model['config']['materialized'] == 'ephemeral' ]
    non_ephemeral_models = [ metadata_model for model,metadata_model in metadata_models.items() if metadata_model['config']['materialized'] != 'ephemeral' ]
    type_list = [ metadata_model['config']['materialized'] for model,metadata_model in metadata_models.items() ]
    logger.debug(f"type list : {type_list}")
    metadata_models = []
    ephemeral_models_temp = []
    non_ephemeral_models_temp = []
    if ephemeral_models:
        ephemeral_models_temp = DatabaseUtil.bulk_columns_ephemeral( ephemeral_models )
    if non_ephemeral_models:
        non_ephemeral_models_temp = DatabaseUtil.bulk_get_table_name_columns( non_ephemeral_models  )
    logger.info(f"{len(ephemeral_models_temp)} ephemeral model / {len(non_ephemeral_models_temp)} non ephemeral model")
    metadata_models = ephemeral_models_temp + non_ephemeral_models_temp
    for metadata_model in metadata_models:
        logger.debug(f"model to do : {metadata_model}")
        resource_type = metadata_model['resource_type']
        model_sql_rel_path = metadata_model["original_file_path"]
        model_yml_rel_path = model_sql_rel_path.replace(".sql", ".yml")            
        yml_path = YMLModelUtil.write_yml_model(
            model_name=metadata_model['name'],
            model_yml_rel_path=model_yml_rel_path,
            name_columns=metadata_model['retrieved_columns'] ,
            resource_type = resource_type
        )
        DBTUtil.flag_doc_model_cmd(model_yml_rel_path, model_sql_rel_path)
        logger.info(f"File created/updated: {yml_path}")
    logger.info(f"✅ Success doc-model")

@task(
    pre=[debug],
    help={"table_id": "Snowflake table id (database.schema.table)"},
)
def doc_source(c, table_id):
    """
    Generate yml content for a source, create or update the .yml file
    If the file already exist, merge existing values with current table structure

    Example:
            invoke doc-source PLATFORM.ACCOUNTBALANCEHISTORY
    """
    args = table_id.split(".")
    if len(args) != 2:
        logger.info(
            f'❌ Error - The table id need to have the format "<schema>.<table>", \
				ex: "PLATFORM.ACCOUNTBALANCEHISTORY"'
        )
        exit(1)
    db_schema, db_table = args
    db_database= os.environ.get('KEYFILE_PROJECT_ID')
    name_columns = DatabaseUtil.get_table_name_columns(
        db_database, db_schema, db_table
    )
    yml_path = YMLSourceUtil.write_yml_source(
        db_database, db_schema, db_table, name_columns
    )
    logger.info(f"✅ Success doc-source, file created/updated: {yml_path}")


@task(pre=[debug])
def check_all(c):
    """Check if you need to perform some actions locally, before pushing your code on Gitlab"""
    DBTUtil.clean_flag_files()
    cicd_check_all(c)
    generate_doc(c)
    logger.info(f"✅ Success check_all")


@task(pre=[debug])
def cicd_check_all(c):
    """
    Technical task
    Part of check-all function, command executed locally and during CICD
    """
    #empty_description_yml_files = YMLUtil.get_yml_files_with_empty_description()
    # TODO - uncomment
    # if len(empty_description_yml_files) > 0:
    # 	yml_files_str = '\n'.join([f'- {yml_file}' for yml_file in empty_description_yml_files])
    # 	logger.info(f'❌ Error - You need to fill the description of those .yml files:\n{yml_files_str}')
    # 	logger.info('Please re-run "invoke check-all" after that')
    # 	exit(1)
    missing_run_models = DBTUtil.get_missing_build_models()
    if len(missing_run_models) > 0:
        models_str = ",".join(missing_run_models)
        logger.info(f"❌ Error - You need to run the command (either build-model or build-doc-model):\ninvoke build-doc-model {models_str}")
        logger.info('Please re-run "invoke check-all" after that')
        exit(1)
    missing_doc_models = DBTUtil.get_missing_doc_models()
    if len(missing_doc_models) > 0:
        models_str = ",".join(missing_doc_models)
        logger.info(f"❌ Error - You need to run the command:\ninvoke doc-model {models_str}")
        logger.info('Please re-run "invoke check-all" after that')
        exit(1)

@task(pre=[debug])
def preview_doc(c):
    """
    Generate the documentation and run a webserver in local at the address 127.0.0.1:8080.
    You can stop it in the terminal with `Ctrl+c` or `Option+c`.

    Example:
        invoke preview-doc
    """
    c.run(f'"{DBT_PATH_VENV}" docs generate')
    c.run(f'"{DBT_PATH_VENV}" docs serve')


@task(pre=[debug])
def generate_doc(c):
    """Generate doc dbt, it will check the format of all .yml files"""
    #c.run(f'"{DBT_PATH_VENV}" docs generate') ## to speed up checkall
    DBTUtil.flag_generate_doc_cmd()
    logger.info(f"✅ Success generate_doc")


@task(pre=[debug], help={"model": "dbt model name"})
def test_model(c, model):
    """
    Run all tests for a model

    Example:
        invoke preview-doc
    """
    c.run(f'"{DBT_PATH_VENV}" test --select {model}')


@task
def deploy_docker(c):
    """Deploy dbt docker image, to run on Airflow in local (Only for DE)

    Example:
        invoke deploy-docker
    """
    os.chdir(DBT_BASE_PATH)
    c.run(
        "docker build --platform=linux/amd64 --no-cache -t xxxxx -f utils/docker/actualdwh ."
    )
    c.run("docker push xxxxx/actualdwh:LOCAL")


@task
def deploy_docker_cicd(c):
    """Deploy dbt docker image, for CICD

    Example:
        invoke deploy-docker-cicd
    """
    os.chdir(DBT_BASE_PATH)
    c.run(
        "docker build --platform=linux/amd64 --no-cache -t ghcr.io/effidic/plateforme-data-demo-dbt/plateforme_data_demo_dbt_cicd:CICD -f utils/docker/plateforme_data_demo_dbt_cicd ."
    )
    c.run("docker login ghcr.io/ -u sylvain.tacquet@effidic.fr -p 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") # jeton révoqué
    c.run("docker push ghcr.io/effidic/plateforme-data-demo-dbt/plateforme_data_demo_dbt_cicd:CICD")





@task(pre=[debug])
def run_seed(c):
    """
    Run a dbt model, and all the models that depend on this model (+1).
    It will also Lint the SQL file.

    Example:
            invoke run-seed
    """
    try:
        shutil.rmtree(DBT_TARGET_PATH, ignore_errors=True)  # Handle strange behaviour 
        try:
            cmd = f'"{DBT_PATH_VENV}" seed'
            c.run(cmd)
        except Exception as e:
            raise Exception("Error during seed")
    except Exception as e:
        logger.info(f"❌ Error seed - {e}")      
        exit(1)
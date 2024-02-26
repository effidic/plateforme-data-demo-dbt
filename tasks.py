"""
Utily commands for dbt:
- tasks.py
- utils/dbt_toolbox/tasks.py

Main objectives:
- Help to configure/install dbt in local, and easely handle dependancies change in the future (python libs + dbt packages)
- Help dbt developers to process several actions in one command, also help them to fill .yml files (mainly used for documentation)
- Limit/control the scope of dbt commands, to prevend high data consumption
- Increase project quality, by controlling during CICD that some actions are performed in local (ex: run changed models)


Technical choices:
- conda (miniconda) to handle python interpreters and librairies dependancies
	- If we only install one python intepreter on the system, it's quite easy to have dependancies conflict. 
	  Also, in the future, we may upgrade python version. So conda it's good for this, isolate python environments and handle several versions
	- We can also use Docker in place of Conda. Why I choosed conda?
		- To install Docker, you need admin access on the computer. No everyone has this access. It's not the case for minoconda
		- That can impact performance, and I'm more confortable with conda.
- use "invoke" python library as command tools:
	- Before, we were using "makefile" file (with make commands). But for Windows users, it can be painfull to configure/install 
	- This library it's quite stable and popular
	- it's important to first run "invoke install", before running other commands
"""

from invoke import task
import shutil
import os
import platform
import sys


# Global variables
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
DBT_TOOLBOX_PATH = os.path.join(CURRENT_PATH, "utils", "dbt_toolbox")
PYTHON_VERSION = 3.9
VENV = "venv"
if platform.system() == "Windows":
    PYTHON_PATH_CONDA = os.path.join(CURRENT_PATH, VENV, "python")
    DBT_PATH_VENV = os.path.join(CURRENT_PATH, VENV, "Scripts", "dbt")
else:
    PYTHON_PATH_CONDA = os.path.join(CURRENT_PATH, VENV, "bin", "python")
    DBT_PATH_VENV = os.path.join(CURRENT_PATH, VENV, "bin", "dbt")

# Set environment variables
# Handle Windows error - UnicodeEncodeError: 'charmap' codec can't encode character '\u274c'
os.environ['PYTHONIOENCODING'] = 'utf8' 


@task
def install(c):
    """Install project dependancies, create conda environment inside a project (similar to venv)"""
    if not os.path.exists(".env"):
        print(
            f'/!\ Error - You need to create the file ".env". See the instruction in the README.md'
        )
        exit(1)
    shutil.rmtree(VENV, ignore_errors=True)
    # cffi=1.15.1=*_3 -> https://github.com/snowflakedb/snowflake-connector-python/issues/1205
    c.run(
        f'conda create --prefix {VENV} -y -q --no-default-package python={PYTHON_VERSION} "cffi=1.15.1=*_3"'
    )
    c.run(f'"{PYTHON_PATH_CONDA}" -m pip install -U -q pip')
    c.run(f'"{PYTHON_PATH_CONDA}" -m pip install -U -q -r utils/requirements-dev.txt')
    c.run(f'"{DBT_PATH_VENV}" deps')
    print(f"Success install")


def run_command_with_venv(c):
    """Activate venv and invoke command - Functions are stored in utils/dbt_toolbox/tasks.py"""
    os.chdir(DBT_TOOLBOX_PATH)
    command_args_str = " ".join(sys.argv[1:])
    # We restart the same command with different python interpreter
    c.run(f'"{PYTHON_PATH_CONDA}" -m invoke {command_args_str}')


@task(aliases=["compile_model", "ls_model", "lint_model", "build_model",  "build_doc_model", "fake_build_model", "doc_model", "fake_doc_model", "doc_source", "test_model"])
def parse_command_args(c, args):
    """Parse commands with 1 arguments, that need to have venv activated"""
    run_command_with_venv(c)


@task(
    aliases=[
        "compile_all",
        "check_all",
        "preview_doc",
        "generate_doc",
        "deploy_docker",
        "deploy_docker_cicd",
        "run_seed"
    ]
)
def parse_command_no_args(c):
    """Parse commands with no argument, that need to have venv activated"""
    run_command_with_venv(c)

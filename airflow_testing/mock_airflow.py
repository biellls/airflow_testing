import contextlib
import os
import tempfile
import jinja2


@contextlib.contextmanager
def set_env(**environ):
    """
    Temporarily set the process environment variables.

    :type environ: dict[str, unicode]
    :param environ: Environment variables to set
    """
    old_environ = dict(os.environ)
    os.environ.update(environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


@contextlib.contextmanager
def mock_airflow():
    with tempfile.TemporaryDirectory() as temp_dir:
        cfg_path = os.path.join(temp_dir, 'airflow.cfg')
        db_path = os.path.join(temp_dir, 'airflow.db')

        airflow_config = render_config(db_path, airflow_home=temp_dir)
        with open(cfg_path, 'w') as f:
            f.write(airflow_config)

        with set_env(AIRFLOW_HOME=temp_dir):
            print(f'Initialising temporary airflow db in {db_path}')
            from airflow.utils.db import initdb
            initdb()

            yield


def render_config(db_path: str, airflow_home: str) -> str:
    template_loader = jinja2.FileSystemLoader(searchpath=os.path.dirname(__file__))
    template_env = jinja2.Environment(loader=template_loader)

    template = template_env.get_template('./airflow.cfg.j2')
    cfg_contents = template.render({'db_path': db_path, 'airflow_home': airflow_home})

    return cfg_contents


def set_connection(conn: dict):
    from airflow import models
    from airflow.utils.db import merge_conn

    merge_conn(models.Connection(**conn))

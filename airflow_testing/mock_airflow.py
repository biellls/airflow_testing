import contextlib
import os
import tempfile
from unittest.mock import patch

import jinja2
from airflow import configuration, settings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session


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


def make_fake_conf_get(airflow_home, sql_alchemy_conn):
    def fake_conf_get(section, key, **kwargs):
        if section == 'core' and key == 'AIRFLOW_HOME':
            return airflow_home
        elif section == 'core' and key == 'SQL_ALCHEMY_CONN':
            return sql_alchemy_conn
        else:
            return configuration.get(section, key, **kwargs)

    return fake_conf_get


@contextlib.contextmanager
def mock_airflow():
    with tempfile.TemporaryDirectory() as temp_dir:
        cfg_path = os.path.join(temp_dir, 'airflow.cfg')
        db_path = os.path.join(temp_dir, 'airflow.db')

        airflow_config = render_config(db_path, airflow_home=temp_dir)
        with open(cfg_path, 'w') as f:
            f.write(airflow_config)

        # with set_env(AIRFLOW_HOME=temp_dir), patch('airflow.configuration.get') as conf_get_mock:
        with set_env(AIRFLOW_HOME=temp_dir):
            # conf_get_mock.side_effect = make_fake_conf_get(
            #     airflow_home=temp_dir,
            #     sql_alchemy_conn=f'sqlite:///{db_path}')
            print(f'Initialising temporary airflow db in {db_path}')
            from airflow.utils.db import initdb
            settings.AIRFLOW_HOME = temp_dir
            settings.SQL_ALCHEMY_CONN = f'sqlite:///{db_path}'

            engine_args = {
                # 'pool_size': configuration.getint('core', 'SQL_ALCHEMY_POOL_SIZE'),
                # 'pool_recycle': configuration.getint('core',
                #                                      'SQL_ALCHEMY_POOL_RECYCLE')
            }

            engine = create_engine(settings.SQL_ALCHEMY_CONN, **engine_args)
            settings.Session = scoped_session(
                sessionmaker(autocommit=False, autoflush=False, bind=engine))
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

# if __name__ == '__main__':
#     with mock_airflow():
#         Variable.set('var_a', 'value_a')
#         set_connection({'conn_id': 'my_test_connection', 'login': 'aaa', 'password': 'pwd'})
#         # dag = DAG(dag_id='example_dag')
#         conn = BaseHook.get_connection('my_test_connection')
#         pass

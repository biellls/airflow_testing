import pytest
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

from airflow_testing.mock_airflow import mock_airflow_db


def test_mock_airflow():
    with mock_airflow_db() as airflow_conn:
        with pytest.raises(KeyError):
            Variable.get('aa')

        airflow_conn.set_variable('aa', 'b')
        assert Variable.get('aa') == 'b'

    with pytest.raises(KeyError):
        Variable.get('aa')

    with mock_airflow_db() as airflow_conn:
        with pytest.raises(KeyError):
            Variable.get('aa')

        airflow_conn.set_connection(conn_id='foo_conn', conn_type='test', extra={'foo': 'bar'})
        conn = BaseHook.get_connection('foo_conn')
        assert conn.conn_id == 'foo_conn'
        assert conn.extra_dejson == {'foo': 'bar'}

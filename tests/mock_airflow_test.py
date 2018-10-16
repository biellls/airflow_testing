import pytest

from airflow.models import Variable
from airflow_testing.mock_airflow import mock_airflow


def test_mock_airflow():
    with mock_airflow():

        with pytest.raises(KeyError):
            Variable.get('var_a')

        Variable.set('var_a', 'value_a')
        assert Variable.get('var_a') == 'value_a'

        Variable.set('var_b', '2')
        assert Variable.get('var_b') == '2'

    with mock_airflow():
        with pytest.raises(KeyError):
            Variable.get('var_a')

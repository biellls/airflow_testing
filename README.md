# Airflow Testing

This library simplifies airflow integration testing by providing a temporary sqlite database. This is provided through a context manager and gets cleaned up once the test is over (and the original connection is restored).

In addition, the context manager provides an AirflowDb object with convenience methods to set variables and connections.

### Example
```python
with mock_airflow_db() as airflow_conn:
    with pytest.raises(KeyError):
        Variable.get('aa')

    airflow_conn.set_variable('aa', 'b')
    assert Variable.get('aa') == 'b'

with pytest.raises(KeyError):
    Variable.get('aa')

with mock_airflow_db() as airflow_conn:
    airflow_conn.set_connection(conn_id='foo_conn', conn_type='test', extra={'foo': 'bar'})
    conn = BaseHook.get_connection('foo_conn')
    assert conn.conn_id == 'foo_conn'
    assert conn.extra_dejson == {'foo': 'bar'}
```

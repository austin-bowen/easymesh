from rosy.types import Endpoint


def test_Endpoint_str():
    endpoint = Endpoint(host='localhost', port=8080)
    assert str(endpoint) == 'localhost:8080'

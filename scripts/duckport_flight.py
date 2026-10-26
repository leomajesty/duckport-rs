"""Minimal duckport Flight client shared by the scripts in this directory.

Only needs pyarrow, so it runs from any ingestor venv. Loopback servers need no token;
otherwise the Bearer token comes from DUCKPORT_TOKEN.
"""

import json
import os

import pyarrow.flight as flight


class _Bearer(flight.ClientMiddleware):
    def __init__(self, token):
        self._value = f"Bearer {token}".encode()

    def sending_headers(self):
        return {b"authorization": self._value}


class _BearerFactory(flight.ClientMiddlewareFactory):
    def __init__(self, token):
        self._token = token

    def start_call(self, info):
        return _Bearer(self._token)


class Duckport:
    def __init__(self, addr):
        token = os.getenv("DUCKPORT_TOKEN", "")
        kwargs = {"middleware": [_BearerFactory(token)]} if token else {}
        self.client = flight.FlightClient(f"grpc://{addr}", **kwargs)

    def action(self, kind, body=None):
        payload = json.dumps(body).encode() if body is not None else b""
        res = list(self.client.do_action(flight.Action(kind, payload)))
        return json.loads(res[0].body.to_pybytes())

    def execute(self, sql):
        return self.action("duckport.execute", {"sql": sql})["rows_affected"]

    def transaction(self, statements):
        return self.action("duckport.execute_transaction", {"statements": statements})

    def query(self, sql):
        """Rows as dicts. duckport.query raises on an empty result; that becomes []."""
        ticket = flight.Ticket(json.dumps({"type": "duckport.query", "sql": sql}).encode())
        try:
            return self.client.do_get(ticket).read_all().to_pylist()
        except flight.FlightInternalError as e:
            if "never sent a data message" in str(e):
                return []
            raise

    def scalar(self, sql):
        rows = self.query(sql)
        return list(rows[0].values())[0] if rows else None


def q(name):
    """Quote an identifier."""
    return '"' + name.replace('"', '""') + '"'


def lit(value):
    """Quote a string literal."""
    return "'" + str(value).replace("'", "''") + "'"

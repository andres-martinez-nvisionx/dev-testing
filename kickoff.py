#!/usr/bin/env python3
"""
PostgreSQL Connector - Job Kickoff for in-cluster testing.

Steps:
  1. Creates PostgreSQL credentials in CredentialManager via gRPC
  2. Creates pipeline in JobEngine via HTTP
  3. Creates job in JobEngine via HTTP

Usage:
  python kickoff.py
  python kickoff.py --pg-host datasource-postgres --pg-port 5432
  python kickoff.py --je-url http://jobengine:8081 --credmgr-addr credential-manager:9090
"""

import argparse
import json
import os
import sys
from typing import Any, Dict

import grpc
import requests
from google.protobuf import any_pb2

from proto_py import credential_requests_pb2
from proto_py import credential_requests_pb2_grpc
from proto_py import postgresql_pb2


# Defaults — override via flags or env vars.
DEFAULT_JOBENGINE_URL = "http://localhost:18081"
DEFAULT_CREDENTIAL_MANAGER_ADDR = "localhost:19090"
DEFAULT_ARCHIVER_INPUT_SUBJECT = "je.archiver.v1.in"
DEFAULT_CONNECTOR_INPUT_SUBJECT = "je.postgresql-scanner.v1.in"

# PostgreSQL defaults
DEFAULT_PG_HOST = "postgres-datasource"
DEFAULT_PG_PORT = 5432
DEFAULT_PG_USER = "connector_user"
DEFAULT_PG_PASS = "connector_pass"
DEFAULT_PG_DB = ""
DEFAULT_PG_SSL = "disable"
DEFAULT_PG_SAMPLE_SIZE = 5


def create_credentials(credmgr_addr: str, datasource_id: str,
                       datasource_access_id: str, pg_host: str,
                       pg_port: int, pg_user: str, pg_pass: str,
                       pg_db: str, pg_ssl: str,
                       pg_sample_size: int) -> bool:
    """Create PostgreSQL credentials in CredentialManager via gRPC."""
    print("Step 1: Create PostgreSQL Credentials")
    print("--------------------------------------")
    print(f"CredentialManager: {credmgr_addr}")
    print(f"PostgreSQL target: {pg_host}:{pg_port} user={pg_user} ssl={pg_ssl}")

    try:
        channel = grpc.insecure_channel(credmgr_addr)
        stub = credential_requests_pb2_grpc.CredentialServiceStub(channel)

        creds = postgresql_pb2.PostgreSQLCredentials(
            host=pg_host,
            port=pg_port,
            username=pg_user,
            password=pg_pass,
            database=pg_db,
            ssl_mode=pg_ssl,
            sample_size=pg_sample_size,
        )

        any_creds = any_pb2.Any()
        any_creds.Pack(creds)

        request = credential_requests_pb2.CreateConnectorCredentialRequest(
            datasource_id=datasource_id,
            datasource_access_id=datasource_access_id,
            credentials=any_creds,
        )

        print(f"datasource_id:        {datasource_id}")
        print(f"datasource_access_id: {datasource_access_id}")

        response = stub.CreateConnectorCredential(request, timeout=10.0)

        if response.success:
            print(f"OK: {response.message}\n")
            return True
        else:
            print(f"FAIL: {response.message}", file=sys.stderr)
            return False

    except grpc.RpcError as e:
        print(f"gRPC error: {e.code()}: {e.details()}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return False
    finally:
        channel.close()


def create_pipeline(je_url: str, pipeline_id: str,
                    connector_input_subject: str,
                    archiver_input_subject: str) -> bool:
    """Create pipeline in JobEngine via HTTP PUT."""
    print("Step 2: Create Pipeline")
    print("-----------------------")

    pipeline = {
        "name": pipeline_id,
        "description": "PostgreSQL Scan to Archive Pipeline",
        "steps": [
            {
                "id": "postgresql-scan",
                "service": "postgresql-connector",
                "input_subject": connector_input_subject,
                "output_subject": "je.ctrl.stepresult.postgresql-scanner",
                "timeout_seconds": 300,
                "message_type": "ScanStart",
            },
            {
                "id": "archive",
                "service": "archiver",
                "input_subject": archiver_input_subject,
                "output_subject": "je.ctrl.stepresult.archiver",
                "timeout_seconds": 60,
            },
        ],
    }

    url = f"{je_url}/api/v1/pipelines/{pipeline_id}"
    print(f"PUT {url}")
    print(f"Payload:\n{json.dumps(pipeline, indent=2)}\n")

    try:
        resp = requests.put(url, json=pipeline,
                            headers={"Content-Type": "application/json"},
                            timeout=30)
        print(f"Status: {resp.status_code}")
        print(f"Body:   {resp.text}\n")
        if 200 <= resp.status_code < 300:
            print("OK: Pipeline created\n")
            return True
        print("FAIL: Pipeline creation failed\n", file=sys.stderr)
        return False
    except requests.RequestException as e:
        print(f"Error: {e}", file=sys.stderr)
        return False


def create_job(je_url: str, pipeline_id: str, datasource_id: str,
               datasource_access_id: str) -> bool:
    """Create job in JobEngine via HTTP POST."""
    print("Step 3: Create Job")
    print("-------------------")

    job_request = {
        "pipeline_id": pipeline_id,
        "payload": {
            "datasource_id": datasource_id,
            "datasource_access_id": datasource_access_id,
        },
    }

    url = f"{je_url}/api/v1/jobs"
    print(f"POST {url}")
    print(f"Payload:\n{json.dumps(job_request, indent=2)}\n")

    try:
        resp = requests.post(url, json=job_request,
                             headers={"Content-Type": "application/json"},
                             timeout=30)
        print(f"Status: {resp.status_code}")
        print(f"Body:   {resp.text}\n")
        if 200 <= resp.status_code < 300:
            print("OK: Job created\n")
            return True
        print("FAIL: Job creation failed\n", file=sys.stderr)
        return False
    except requests.RequestException as e:
        print(f"Error: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="PostgreSQL Connector - Job Kickoff (in-cluster)")
    parser.add_argument("--je-url", default="",
                        help=f"JobEngine URL (default: {DEFAULT_JOBENGINE_URL})")
    parser.add_argument("--credmgr-addr", default="",
                        help=f"CredentialManager gRPC addr (default: {DEFAULT_CREDENTIAL_MANAGER_ADDR})")
    parser.add_argument("--connector-subject", default=DEFAULT_CONNECTOR_INPUT_SUBJECT,
                        help=f"Connector NATS input subject (default: {DEFAULT_CONNECTOR_INPUT_SUBJECT})")
    parser.add_argument("--archiver-subject", default=DEFAULT_ARCHIVER_INPUT_SUBJECT,
                        help=f"Archiver NATS input subject (default: {DEFAULT_ARCHIVER_INPUT_SUBJECT})")
    parser.add_argument("--index-suffix", default="001",
                        help="Datasource ID suffix (default: 001)")

    # PostgreSQL connection
    parser.add_argument("--pg-host", default=DEFAULT_PG_HOST,
                        help=f"PostgreSQL host (default: {DEFAULT_PG_HOST})")
    parser.add_argument("--pg-port", type=int, default=DEFAULT_PG_PORT,
                        help=f"PostgreSQL port (default: {DEFAULT_PG_PORT})")
    parser.add_argument("--pg-user", default=DEFAULT_PG_USER,
                        help=f"PostgreSQL user (default: {DEFAULT_PG_USER})")
    parser.add_argument("--pg-pass", default=DEFAULT_PG_PASS,
                        help=f"PostgreSQL password (default: {DEFAULT_PG_PASS})")
    parser.add_argument("--pg-db", default=DEFAULT_PG_DB,
                        help="PostgreSQL database (default: empty = scan all)")
    parser.add_argument("--pg-ssl", default=DEFAULT_PG_SSL,
                        help=f"PostgreSQL SSL mode (default: {DEFAULT_PG_SSL})")
    parser.add_argument("--pg-sample-size", type=int,
                        default=DEFAULT_PG_SAMPLE_SIZE,
                        help=f"Sample size per column (default: {DEFAULT_PG_SAMPLE_SIZE})")

    args = parser.parse_args()

    je_url = args.je_url or os.environ.get("JOBENGINE_URL", "") or DEFAULT_JOBENGINE_URL
    credmgr_addr = args.credmgr_addr or os.environ.get("CREDENTIAL_MANAGER_ADDR", "") or DEFAULT_CREDENTIAL_MANAGER_ADDR

    datasource_id = f"postgresql-dsid-{args.index_suffix}"
    datasource_access_id = f"postgresql-access-{args.index_suffix}"
    pipeline_id = "postgresql-scan-archive"

    print("=" * 50)
    print("PostgreSQL Connector - Job Kickoff")
    print("=" * 50)
    print(f"JobEngine:          {je_url}")
    print(f"CredentialManager:  {credmgr_addr}")
    print(f"Connector subject:  {args.connector_subject}")
    print(f"PostgreSQL target:  {args.pg_host}:{args.pg_port}")
    print(f"Datasource ID:      {datasource_id}")
    print(f"Access ID:          {datasource_access_id}")
    print()

    if not create_credentials(credmgr_addr, datasource_id,
                              datasource_access_id, args.pg_host,
                              args.pg_port, args.pg_user, args.pg_pass,
                              args.pg_db, args.pg_ssl, args.pg_sample_size):
        sys.exit(1)

    if not create_pipeline(je_url, pipeline_id, args.connector_subject,
                           args.archiver_subject):
        sys.exit(1)

    if not create_job(je_url, pipeline_id, datasource_id,
                      datasource_access_id):
        sys.exit(1)

    print("=" * 50)
    print("PostgreSQL job kickoff completed successfully!")
    print("=" * 50)


if __name__ == "__main__":
    main()

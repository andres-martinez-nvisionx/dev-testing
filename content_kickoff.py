#!/usr/bin/env python3
"""
content_kickoff.py — E2E driver for the PostgreSQL connector in content mode.

Flow:
  1. Seed a credential in Credential Manager (same gRPC as kickoff.py).
  2. Publish a ContentFetchRequest to CONTENT_FETCH JetStream on
     subject content.fetch.postgresql, with an ephemeral inbox as reply_subject.
  3. Wait for the ContentFetchResponse reply (connector samples the column,
     streams bytes to Content Service, and replies success/error).
  4. Verify the upload via gRPC ContentTransferService.Download.

Usage:
  python content_kickoff.py \\
      --urn 'nx:postgresql:v1?access=postgresql-access-001&pqn=sales.public.customers.email'

  # Re-run without re-seeding
  python content_kickoff.py --urn '...' --skip-seed

  # Override cluster endpoints
  python content_kickoff.py --urn '...' \\
      --nats-url nats://nats:4222 \\
      --credmgr-addr credential-manager:9090 \\
      --content-transfer-addr content-service:9090

Prerequisites (in-cluster):
  - Content Service deployed and healthy (kubectl get pods -l app.kubernetes.io/name=content-service)
  - Connector running with CONNECTOR_MODE=content and consumer bound to CONTENT_FETCH stream
  - NATS stream CONTENT_FETCH provisioned (created on demand if missing)
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import sys
import uuid
from urllib.parse import parse_qs

import grpc
from google.protobuf import any_pb2
from google.protobuf.timestamp_pb2 import Timestamp

from proto_py import credential_requests_pb2
from proto_py import credential_requests_pb2_grpc
from proto_py import postgresql_pb2
from proto_py import content_fetch_pb2 as fetchpb
from proto_py import content_transfer_pb2 as transferpb
from proto_py import content_transfer_pb2_grpc as transfer_grpc


DEFAULT_NATS_URL              = "nats://nats:4222"
DEFAULT_CREDMGR_ADDR          = "credential-manager:9090"
DEFAULT_CONTENT_TRANSFER_ADDR = "content-service:9090"
DEFAULT_SUBJECT               = "content.fetch.postgresql"
DEFAULT_STREAM                = "CONTENT_FETCH"


def parse_access_from_urn(urn: str) -> str:
    if "?" not in urn:
        raise ValueError(f"URN missing query string: {urn!r}")
    _, query = urn.split("?", 1)
    params = parse_qs(query)
    access = params.get("access", [""])[0]
    if not access:
        raise ValueError(f"URN missing required 'access' parameter: {urn!r}")
    return access


def seed_credential(
    credmgr_addr: str,
    access_id: str,
    pg_host: str,
    pg_port: int,
    pg_user: str,
    pg_password: str,
    pg_database: str,
    pg_sslmode: str,
) -> None:
    print(f"[1/3] Seeding credential at {credmgr_addr} for access={access_id!r}")
    channel = grpc.insecure_channel(credmgr_addr)
    try:
        stub = credential_requests_pb2_grpc.CredentialServiceStub(channel)

        creds = postgresql_pb2.PostgreSQLCredentials(
            host=pg_host,
            port=pg_port,
            username=pg_user,
            password=pg_password,
            database=pg_database,
            ssl_mode=pg_sslmode,
            sample_size=10,
        )
        any_creds = any_pb2.Any()
        any_creds.Pack(creds)

        req = credential_requests_pb2.CreateConnectorCredentialRequest(
            datasource_id=access_id,
            datasource_access_id=access_id,
            credentials=any_creds,
        )
        resp = stub.CreateConnectorCredential(req, timeout=10.0)
        if not resp.success:
            msg = (resp.message or "").lower()
            if "exists" in msg or "duplicate" in msg:
                print(f"      credential already exists — reusing ({resp.message})")
                return
            raise RuntimeError(f"credential seed failed: {resp.message}")
        print(f"      OK — {resp.message}")
    finally:
        channel.close()


async def ensure_content_stream(js, stream_name: str, subject_pattern: str) -> None:
    from nats.js.api import StreamConfig
    from nats.js.errors import NotFoundError

    try:
        info = await js.stream_info(stream_name)
        covered = any(
            subject_pattern == s or (s.endswith(".>") and subject_pattern.startswith(s[:-2]))
            for s in info.config.subjects or []
        )
        if not covered:
            print(f"      WARN: stream {stream_name!r} exists but doesn't cover "
                  f"subject {subject_pattern!r} (has {info.config.subjects})")
        else:
            print(f"      stream {stream_name!r} already exists — reusing")
        return
    except NotFoundError:
        pass

    await js.add_stream(config=StreamConfig(
        name=stream_name,
        subjects=["content.fetch.>"],
    ))
    print(f"      created JetStream stream {stream_name!r} → content.fetch.>")


async def publish_and_wait(
    nats_url: str,
    subject: str,
    stream: str,
    urn: str,
    timeout: float,
) -> fetchpb.ContentFetchResponse:
    import nats

    print(f"[2/3] Publishing ContentFetchRequest to {subject!r} via {nats_url}")
    nc = await nats.connect(nats_url)
    try:
        js = nc.jetstream()
        await ensure_content_stream(js, stream, subject)

        inbox = nc.new_inbox()
        fut: asyncio.Future = asyncio.Future()

        async def on_msg(m):
            resp = fetchpb.ContentFetchResponse()
            resp.ParseFromString(m.data)
            status_name = fetchpb.FetchResponseStatus.Name(resp.status)
            if resp.status == fetchpb.FETCH_STATUS_PROCESSING:
                print(f"      ← PROCESSING (request_id={resp.request_id})")
                return
            if not fut.done():
                print(f"      ← {status_name} (request_id={resp.request_id})")
                fut.set_result(resp)

        sub = await nc.subscribe(inbox, cb=on_msg)
        await nc.flush()

        req = fetchpb.ContentFetchRequest(
            request_id=f"kickoff-{uuid.uuid4().hex[:12]}",
            urn=urn,
            reply_subject=inbox,
        )
        ts = Timestamp()
        ts.GetCurrentTime()
        req.timestamp.CopyFrom(ts)

        print(f"      request_id={req.request_id}")
        print(f"      urn={urn}")

        ack = await js.publish(subject, req.SerializeToString())
        print(f"      JetStream ACK: stream={ack.stream} seq={ack.seq}")

        try:
            resp = await asyncio.wait_for(fut, timeout=timeout)
            return resp
        finally:
            await sub.unsubscribe()
    finally:
        await nc.drain()


def verify_via_grpc_download(addr: str, urn: str, expected_size: int) -> None:
    """Verify the upload by downloading via gRPC ContentTransferService.Download.

    Protocol: first message is DownloadResponse{info}, subsequent messages are
    DownloadResponse{chunk}. We reassemble, compute SHA-256, and compare size.
    """
    print(f"[3/3] Verifying upload via gRPC Download from {addr}")
    channel = grpc.insecure_channel(addr)
    try:
        stub = transfer_grpc.ContentTransferServiceStub(channel)
        req = transferpb.DownloadRequest(
            urn=urn,
            request_id=f"kickoff-verify-{uuid.uuid4().hex[:8]}",
        )

        info = None
        data = bytearray()

        try:
            for msg in stub.Download(req, timeout=30.0):
                which = msg.WhichOneof("data")
                if which == "info":
                    info = msg.info
                    print(f"      info: size={info.size} bytes, content_type={info.content_type!r}")
                elif which == "chunk":
                    data.extend(msg.chunk)
        except grpc.RpcError as e:
            print(f"      Download gRPC error: {e.code()}: {e.details()}")
            return

        sha256 = hashlib.sha256(bytes(data)).hexdigest()
        print(f"      Downloaded {len(data)} bytes, sha256={sha256}")

        if expected_size > 0 and len(data) != expected_size:
            print(f"      WARN: size mismatch — reply said {expected_size}, downloaded {len(data)}")
        else:
            print(f"      OK — size matches reply")
    finally:
        channel.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="PostgreSQL content-mode kickoff for in-cluster testing.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--urn", required=True,
                        help="Content URN, e.g. nx:postgresql:v1?access=X&pqn=db.schema.table.col")

    parser.add_argument("--nats-url",              default=os.getenv("NATS_URL", DEFAULT_NATS_URL))
    parser.add_argument("--credmgr-addr",          default=os.getenv("CREDENTIAL_MANAGER_ADDR", DEFAULT_CREDMGR_ADDR))
    parser.add_argument("--content-transfer-addr", default=os.getenv("CONTENT_TRANSFER_ADDR", DEFAULT_CONTENT_TRANSFER_ADDR),
                        help="Content Service gRPC address for Download verification")
    parser.add_argument("--stream",  default=DEFAULT_STREAM,  help="JetStream stream name (created if missing)")
    parser.add_argument("--subject", default=DEFAULT_SUBJECT, help="Content fetch NATS subject")
    parser.add_argument("--timeout", type=float, default=60.0, help="Reply wait timeout in seconds")
    parser.add_argument("--no-verify", action="store_true",   help="Skip gRPC Download verification")
    parser.add_argument("--skip-seed", action="store_true",   help="Skip credential seeding (already seeded)")

    parser.add_argument("--pg-host",     default="postgres-datasource")
    parser.add_argument("--pg-port",     type=int, default=5432)
    parser.add_argument("--pg-user",     default="connector_user")
    parser.add_argument("--pg-password", default="connector_pass")
    parser.add_argument("--pg-database", default="", help="Bootstrap database (empty = use 'postgres')")
    parser.add_argument("--pg-sslmode",  default="disable")

    args = parser.parse_args()

    print("=" * 50)
    print("PostgreSQL Content-Mode Kickoff")
    print("=" * 50)
    print(f"NATS:             {args.nats_url}")
    print(f"CredentialManager: {args.credmgr_addr}")
    print(f"Content Service:  {args.content_transfer_addr}")
    print(f"URN:              {args.urn}")
    print()

    try:
        access_id = parse_access_from_urn(args.urn)
    except ValueError as e:
        print(f"Invalid URN: {e}", file=sys.stderr)
        return 2

    if not args.skip_seed:
        try:
            seed_credential(
                credmgr_addr=args.credmgr_addr,
                access_id=access_id,
                pg_host=args.pg_host,
                pg_port=args.pg_port,
                pg_user=args.pg_user,
                pg_password=args.pg_password,
                pg_database=args.pg_database,
                pg_sslmode=args.pg_sslmode,
            )
        except (grpc.RpcError, RuntimeError) as e:
            print(f"Credential seeding failed: {e}", file=sys.stderr)
            return 1
    else:
        print("[1/3] Skipping credential seed (--skip-seed)")

    try:
        resp = asyncio.run(
            publish_and_wait(
                nats_url=args.nats_url,
                subject=args.subject,
                stream=args.stream,
                urn=args.urn,
                timeout=args.timeout,
            )
        )
    except asyncio.TimeoutError:
        print(f"\nTimed out after {args.timeout}s — no terminal reply received.\n"
              f"Check: kubectl logs -l app.kubernetes.io/name=postgresql-connector-be-go-content",
              file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Publish/wait failed: {e}", file=sys.stderr)
        return 1

    print("------------------------------------")
    print("Reply details")
    print("------------------------------------")
    print(f"  success      : {resp.success}")
    print(f"  status       : {fetchpb.FetchResponseStatus.Name(resp.status)}")
    print(f"  duration_ms  : {resp.duration_ms}")
    if resp.success:
        print(f"  urn          : {resp.urn}")
        print(f"  content_size : {resp.content_size} bytes")
    else:
        err = resp.error
        print(f"  error.code   : {fetchpb.FetchErrorCode.Name(err.code)}")
        print(f"  error.msg    : {err.message}")
        print(f"  retryable    : {err.retryable}")

    if resp.success and not args.no_verify:
        print("------------------------------------")
        verify_via_grpc_download(args.content_transfer_addr, resp.urn or args.urn, resp.content_size)

    return 0 if resp.success else 1


if __name__ == "__main__":
    sys.exit(main())

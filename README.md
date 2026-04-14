# dev-testing

Scripts and seed data for testing the PostgreSQL connector inside a K8s cluster.

## Files

| File | Description |
|---|---|
| `init.sql` | Seed data: creates 3 databases (sales, hr, analytics) with ~3650 rows |
| `kickoff.py` | Triggers a scan: creates credentials, pipeline, and job |
| `setup.sh` | Installs Python + venv + dependencies inside a pod |
| `proto_py/` | Protobuf stubs needed by kickoff.py |

## Usage

### 1. Seed the test PostgreSQL

```bash
psql -U postgres -f init.sql
```

### 2. Install dependencies (inside the pod)

```bash
chmod +x setup.sh && ./setup.sh
```

### 3. Run the kickoff

```bash
source .venv/bin/activate

# With defaults (see kickoff.py --help for all options)
python kickoff.py

# With custom addresses (adjust to your cluster's service names)
python kickoff.py \
  --je-url http://jobengine:8081 \
  --credmgr-addr credential-manager:9090 \
  --pg-host my-postgres-service \
  --pg-port 5432
```

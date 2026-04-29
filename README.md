# dev-testing

Scripts y datos seed para probar el **PostgreSQL connector** end-to-end contra el cluster de staging (EKS).

Soporta los dos modos del connector:
- **Scanner mode** (`kickoff.py`) — descubre tablas, columnas y emite jobs al JobEngine.
- **Content mode** (`content_kickoff.py`) — samplea contenido de columnas y lo streamea al Content Service vía gRPC.

---

## Archivos

| Archivo | Descripción |
|---|---|
| `init.sql` | Seed de 3 bases (sales, hr, analytics) con ~3650 filas |
| `kickoff.py` | Dispara un scan: crea credenciales, pipeline y job en JobEngine |
| `content_kickoff.py` | Publica un `ContentFetchRequest` a NATS y verifica la subida al Content Service |
| `setup.sh` | Instala Python3 + venv + dependencias dentro de un pod |
| `proto_py/` | Stubs de protobuf que usan los kickoff |
| `CONTENT_KICKOFF_PLAN.md` | Notas de diseño del flujo de content mode |

---

## Pre-requisitos

- Acceso al cluster de staging vía `kubectl` (jumpbox EC2 o `aws eks update-kubeconfig`)
- Connector deployado en el namespace `default`:
  - `applications-postgresql-connector-be-go` (scanner)
  - `applications-postgresql-connector-be-go-content` (content)
- Content Service deployado y healthy (solo para content mode)
- Stream `CONTENT_FETCH` provisionado en NATS JetStream (lo crea el chart)

Verificar antes de arrancar:
```bash
kubectl get deployments -n default -o wide | grep postgres
kubectl get scaledobject -n default | grep postgres
kubectl logs deployment/applications-postgresql-connector-be-go-content -n default --tail=5
```

---

## Workflow completo

### 1. Clonar localmente (si querés editar/pushear cambios)

```bash
git clone git@github.com:andres-martinez-nvisionx/dev-testing.git
cd dev-testing
```

### 2. Pushear cambios (si modificaste algo)

```bash
git add -A
git commit -m "<mensaje>"
git push
```

### 3. Levantar el deployment del connector si está en 0 réplicas (KEDA scale-to-zero)

```bash
kubectl annotate scaledobject applications-postgresql-connector-be-go -n default \
  "autoscaling.keda.sh/paused-replicas=1" --overwrite

kubectl annotate scaledobject applications-postgresql-connector-be-go-content -n default \
  "autoscaling.keda.sh/paused-replicas=1" --overwrite
```

### 4. Entrar al pod del connector

```bash
POD=$(kubectl get pods -n default -l app.kubernetes.io/name=postgresql-connector-be-go \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec -it $POD -n default -- sh
```

### 5. Bajar el repo al pod vía curl (es público, sin auth)

Adentro del pod:

```bash
cd /tmp && \
curl -L https://github.com/andres-martinez-nvisionx/dev-testing/archive/refs/heads/main.tar.gz \
  -o dev-testing.tgz && \
tar xzf dev-testing.tgz && \
mv dev-testing-* dev-testing && \
cd dev-testing
```

> Cambiá `main` por el branch que necesites (ej. `feature/foo`).

### 6. Setup de Python y dependencias

```bash
chmod +x setup.sh && ./setup.sh
source .venv/bin/activate
```

`setup.sh` detecta el package manager (apk en alpine) e instala:
- `python3`, `pip`, `virtualenv`
- `grpcio`, `protobuf`, `requests`, `nats-py`
- `proto_py/` en modo editable

### 7. Correr el kickoff

#### Scanner mode

```bash
python kickoff.py --help

# Defaults
python kickoff.py

# Con overrides
python kickoff.py \
  --je-url http://jobengine:8081 \
  --credmgr-addr credential-manager:9090 \
  --pg-host postgres-datasource \
  --pg-port 5432
```

#### Content mode

```bash
python content_kickoff.py --help

# Disparo básico — necesita una URN apuntando a una columna específica
python content_kickoff.py \
  --urn 'nx:postgresql:v1?access=postgresql-access-001&pqn=sales.public.customers.email'

# Sin re-seedear credenciales (si ya las creaste antes)
python content_kickoff.py --urn '...' --skip-seed

# Con overrides de endpoints
python content_kickoff.py --urn '...' \
  --nats-url nats://nats:4222 \
  --credmgr-addr credential-manager:9090 \
  --content-transfer-addr applications-content-service:18092
```

---

## Cómo validar que funcionó

### A. Logs del connector

En sesiones paralelas (afuera del pod):

```bash
# Scanner
kubectl logs -f deployment/applications-postgresql-connector-be-go -n default

# Content
kubectl logs -f deployment/applications-postgresql-connector-be-go-content -n default

# Content Service (recibe los uploads)
kubectl logs -f deployment/applications-content-service -n default
```

**Qué buscar (content mode):**
- Connector content: `Connecting to Content Transfer`, `received ContentFetchRequest`, `streaming N rows`
- Content Service: `received upload stream`, `wrote N bytes to s3://...`

### B. Inspección de NATS

```bash
NATS_POD=$(kubectl get pods -n default -l app.kubernetes.io/name=nats -o jsonpath='{.items[0].metadata.name}')

# Ver el stream
kubectl exec -n default $NATS_POD -- nats stream info CONTENT_FETCH

# Ver el consumer durable
kubectl exec -n default $NATS_POD -- nats consumer info CONTENT_FETCH content-fetch-postgresql
```

### C. Verificación en S3

El Content Service persiste los samples en S3 (bucket configurado vía `contentService.s3Bucket` en helm).

```bash
# Ver dónde escribe el content service
kubectl exec deployment/applications-content-service -n default -- env | grep -iE "bucket|s3"

# Listar objetos recientes
aws s3 ls s3://<bucket>/<prefix>/ --recursive --human-readable | tail -20
```

### D. Verificación gRPC desde el kickoff

`content_kickoff.py` por default hace un `Download` del URN devuelto y compara el `sha256` para confirmar la integridad. Si la verificación falla, sale con error. Para saltearla:

```bash
python content_kickoff.py --urn '...' --no-verify
```

---

## Cleanup

Cuando termines, devolvé el control a KEDA:

```bash
kubectl annotate scaledobject applications-postgresql-connector-be-go -n default \
  "autoscaling.keda.sh/paused-replicas-" --overwrite

kubectl annotate scaledobject applications-postgresql-connector-be-go-content -n default \
  "autoscaling.keda.sh/paused-replicas-" --overwrite
```

KEDA va a llevar los deployments a 0 réplicas después del cooldown (15 min).

---

## Troubleshooting

| Síntoma | Posible causa | Cómo verificar |
|---|---|---|
| `kickoff.py` no llega a NATS | NATS_URL incorrecto desde el pod | `nslookup nats` adentro del pod |
| Content Fetch timeout | Consumer no leyendo del stream | `nats consumer info CONTENT_FETCH content-fetch-postgresql` |
| Pod en 0 réplicas y no levanta | KEDA destruyó el pod paused | re-applicar la annotation |
| `gRPC Unauthenticated` al content service | TLS toggle desincronizado | Ver `contentService.contentTransferInsecure` en values |
| `Tag not found` en pull de imagen | bot auto-update no corrió | `gh run list --workflow build.yaml` en el connector |

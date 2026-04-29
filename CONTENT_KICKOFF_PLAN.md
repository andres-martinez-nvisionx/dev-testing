# Content Mode Kickoff — Plan

Estado: **bloqueado por dispositivo** del Content Service en dev (Apr 2026).
Cuando el Content Service esté disponible en el cluster, este documento
describe los pasos exactos para construir `dev-testing/content_kickoff.py`,
el equivalente al `kickoff.py` actual (que ejercita scanner mode contra
JobEngine + Archiver) pero apuntado al flujo de content mode.

## Contexto

- El connector postgres tiene **dos modos**: `scanner` (existente, ya
  validado en dev con `kickoff.py`) y `content` (nuevo, refactorizado en
  PR del connector más reciente).
- En content mode el connector **no escribe a S3 directamente**. Recibe
  un `ContentFetchRequest` por NATS JetStream (`CONTENT_FETCH` stream),
  samplea la columna pedida vía `TABLESAMPLE SYSTEM + STRING_AGG`, y
  **streamea los bytes al Content Service** vía
  `ContentTransferService.Upload` (gRPC, definido en
  `pipeline-lib-go/proto/fetchpb/content_transfer.proto`).
- El Content Service responde con `UploadResponse{urn, size, sha256}`.
  El connector publica un `ContentFetchResponse{success, urn,
  content_size}` al `reply_subject` original del request por core NATS.
- Validamos localmente con un mock del Content Service
  (`connector-generator-ops/.../mock_content_service`, gRPC + endpoint
  HTTP `/_debug/urn/{urn}` para inspección). En dev el Content Service
  real **no expone `/_debug`** — la verificación se hace por otra vía.

## Pre-requisitos en dev (chequear antes de arrancar el script)

1. **Content Service deployado y healthy**:
   ```bash
   kubectl get pods -n <namespace> -l app.kubernetes.io/name=content-service
   kubectl get svc -n <namespace> | grep content-service
   ```
   Esperar `1/1 Running`. El service expone `:9090` gRPC.

2. **Connector content-mode deployado**:
   ```bash
   kubectl get pods -n <namespace> -l app.kubernetes.io/name=postgresql-connector-be-go-content
   kubectl logs -n <namespace> -l app.kubernetes.io/name=postgresql-connector-be-go-content --tail=20
   ```
   Esperar log `PostgreSQL connector running in CONTENT mode` y consumer
   bound al stream `CONTENT_FETCH`.

3. **Stream `CONTENT_FETCH` provisionado en JetStream**:
   ```bash
   kubectl exec -n <namespace> <nats-pod> -- nats stream info CONTENT_FETCH
   ```
   Si no existe lo crea el helm que aprovisiona la app.

## Diferencias respecto a `kickoff.py` (scanner)

| Aspecto | `kickoff.py` (scanner) | `content_kickoff.py` (content) |
|---|---|---|
| Punto de entrada | JobEngine HTTP `POST /pipelines` + `POST /jobs` | NATS JetStream publish a `content.fetch.postgresql` |
| Trigger del connector | JobEngine envía `ScanStart` por NATS al consumer del scanner | Connector ya está suscripto al stream `CONTENT_FETCH` con su durable |
| Validación final | Items publicados a `je.archiver.v1.in`, los buscamos en OpenSearch | Reply `ContentFetchResponse` en inbox efímero + verificación opcional vía `Download` gRPC |
| Credencial Postgres | Mismo `seed_credential` por gRPC al CredentialManager | **Idéntico** — reusa el mismo helper |

## Estructura del script

El plan propone reusar el `dev-testing/content_kickoff.py` que ya hicimos
en `pipeline-workspace/devtools/`. Tiene 4 piezas independientes; tres se
copian tal cual y la cuarta se reescribe.

```
parse_access_from_urn(urn) -> str        ← copiar tal cual (función pura)
seed_credential(...)                     ← copiar, ajustar default endpoints
publish_and_wait(nats_url, urn, ...)     ← copiar, ajustar default endpoints
verify_via_grpc_download(addr, urn, ...) ← NUEVA, reemplaza verify_in_content_service
```

### `verify_via_grpc_download` — diseño

El stub local exponía `/_debug/urn/{urn}` HTTP. El Content Service real
**no expone esa ruta**: solo el contrato gRPC.

`pipeline-lib-go/proto/fetchpb/content_transfer.proto` define:

```proto
service ContentTransferService {
  rpc Download(DownloadRequest) returns (stream DownloadResponse);
}
```

El cliente:

1. Abre stream `Download(DownloadRequest{urn: <urn>})`.
2. Primer mensaje del stream es un `Info{urn, size, content_type, ...}` —
   lo guardamos para reportar.
3. Mensajes siguientes: `chunk` con bytes. Concatenamos.
4. Calculamos SHA-256 sobre el resultado.
5. Comparamos con `resp.content_size` y `resp.urn` del reply NATS.

### Plumbing de protos

`dev-testing/proto_py/` actualmente tiene generados:

- `credential_requests_pb2*` (✓)
- `postgresql_pb2.py` (✓)

Faltan para content mode:

- `content_fetch_pb2.py` (request/response del NATS)
- `content_transfer_pb2.py` + `content_transfer_pb2_grpc.py` (Upload/Download)

Ambos `.proto` viven en `pipeline-lib-go/proto/fetchpb/`. La regeneración
es la misma que ya hace `pipeline-workspace/devtools/generate_python_protos.py`,
solo hay que portearla acá. Alternativa: copiar los `*_pb2.py` ya
generados desde `pipeline-workspace/devtools/proto_py/` (más rápido,
menos prolijo).

## Defaults sugeridos (en cluster)

```python
DEFAULT_NATS_URL          = "nats://nats:4222"
DEFAULT_CREDMGR_ADDR      = "credential-manager:9090"
DEFAULT_CONTENT_TRANSFER_ADDR = "content-service:9090"  # service del Content Service
DEFAULT_SUBJECT           = "content.fetch.postgresql"
DEFAULT_STREAM            = "CONTENT_FETCH"
```

Mantener flags `--nats-url`, `--credmgr-addr`, `--content-transfer-addr`
para overrides. Mismo patrón que `kickoff.py`.

## Flujo de ejecución esperado

```
$ python content_kickoff.py \
    --urn 'nx:postgresql:v1?access=postgresql-access-001&pqn=sales.public.customers.email'

[1/3] Seeding credential at credential-manager:9090 for access='postgresql-access-001'
      OK — Credentials updated successfully
[2/3] Publishing ContentFetchRequest to 'content.fetch.postgresql' via nats://nats:4222
      stream 'CONTENT_FETCH' already exists — reusing
      request_id=kickoff-...
      JetStream ACK: stream=CONTENT_FETCH seq=N
      ← PROCESSING (request_id=...)
      ← FETCH_STATUS_COMPLETED (request_id=...)
------------------------------------
Reply details
------------------------------------
  success      : True
  urn          : nx:postgresql:v1?access=postgresql-access-001&pqn=sales.public.customers.email
  content_size : 5091 bytes
[3/3] Verifying upload via gRPC Download from content-service:9090
      OK — downloaded 5091 bytes, sha256 matches reply
```

## Escenarios de fallo y qué validan

| Estado del Content Service | Reply esperado | Conclusión |
|---|---|---|
| Down/no existe | `success=false`, `error.code=CONNECTION_FAILED` | Connector bien wireado, destino caído. Útil para validar la mitad nuestra. |
| Up pero sin `UploadService` | `success=false`, `error.code=INTERNAL` o `CACHE_WRITE_FAILED` | Wiring + DNS OK, falta la lógica server. |
| Up con `UploadService` | `success=true`, paso 3 baja los bytes y matchea SHA-256 | Path completo verde. |

## Trabajo concreto (cuando arranquemos)

1. [ ] Copiar `pipeline-workspace/devtools/content_kickoff.py` →
   `dev-testing/content_kickoff.py`.
2. [ ] Copiar/regenerar `content_fetch_pb2.py`, `content_transfer_pb2.py`
   y su `_grpc.py` en `dev-testing/proto_py/`.
3. [ ] Reescribir `verify_in_content_service` →
   `verify_via_grpc_download` usando el client gRPC `Download`.
4. [ ] Cambiar defaults a service names del cluster (no `localhost:*`).
5. [ ] Actualizar `dev-testing/setup.sh` si hace falta agregar deps —
   `grpcio` ya está, no hace falta nada nuevo.
6. [ ] Actualizar `dev-testing/README.md` con sección "Content mode".
7. [ ] Probar con un acceso ya seedeado en dev (o `--skip-seed`).

## Notas sobre el endpoint del Content Service

El helm chart resuelve la dirección via helper
`applications.contentService.grpcEndpoint`, que renderea
`<release>-content-service:9090`. Si el chart aplicado en dev sigue esa
convención, el default `content-service:9090` debería funcionar (network
alias del Service). Confirmarlo cuando esté deployado:

```bash
kubectl get svc -n <namespace> | grep content-service
```

Si el nombre del Service tiene un prefijo (ej. `applications-content-service`),
se ajusta el default o se usa `--content-transfer-addr` explícito.

## Pendiente de confirmar con el equipo de Content Service

- [ ] ¿Auth en el `ContentTransferService` en dev? El connector hoy
      configura `CONTENT_TRANSFER_INSECURE=false` pero asume TLS via
      mesh, no auth aplicativa. Si dev exige token, el script tiene que
      inyectarlo en el call gRPC.
- [ ] ¿El Content Service real responde a `Download(urn)` para URNs
      uploaded recientemente, o solo cuando ya están persistidos en S3?
      Si hay un delay, el verify del paso 3 puede necesitar reintentos.

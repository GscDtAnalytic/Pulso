"""Catalogo Iceberg — onde vivem os metadados das tabelas do lake.

Catalogo SQL (`pyiceberg.catalog.sql.SqlCatalog`) sobre uma URI SQLAlchemy:

- **dev/prod** — Postgres do `docker-compose` (`iceberg_catalog_uri`). E o mesmo
  catalogo que o Trino lera no Marco 4 via conector `iceberg` jdbc; multi-engine
  sobre um dado so (principio do `ARCHITECTURE_PROPOSAL.md`).
- **testes** — sqlite local + warehouse em `file://` (ver `build_catalog(uri=..., warehouse=...)`),
  sem broker nem MinIO: o sink e testavel 100% offline, como a topologia do Marco 2.

O object storage (warehouse) e S3-compativel: MinIO em dev, GCS em prod. O endpoint
vem de `Settings` (`PULSO_S3_ENDPOINT`); warehouse `file://` desliga a config S3.
"""

from __future__ import annotations

from pulso_infra import Settings
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog


def build_catalog(
    settings: Settings,
    *,
    uri: str | None = None,
    warehouse: str | None = None,
) -> Catalog:
    """Constroi o `SqlCatalog` do Pulso.

    `uri`/`warehouse` sobrescrevem os de `Settings` — usado pelos testes para
    apontar a um sqlite + warehouse local temporario.
    """
    uri = uri or settings.iceberg_catalog_uri
    warehouse = warehouse or settings.iceberg_warehouse

    props: dict[str, str] = {"uri": uri, "warehouse": warehouse}
    # Warehouse S3 (MinIO dev) precisa de endpoint explícito + chave.
    # Warehouse GCS (prod) usa Application Default Credentials — sem chave explícita.
    # `file://` (testes) não precisa de FileIO remoto.
    if warehouse.startswith("s3://"):
        props.update(
            {
                "s3.endpoint": settings.s3_endpoint,
                "s3.access-key-id": settings.s3_access_key,
                "s3.secret-access-key": settings.s3_secret_key,
                "s3.region": "us-east-1",
                "s3.path-style-access": "true",  # MinIO não faz virtual-host buckets
            }
        )
    elif warehouse.startswith("gs://"):
        # GCS via ADC (Cloud Run SA ou `gcloud auth application-default login`).
        # FsspecFileIO delega para gcsfs que resolve as credenciais automaticamente.
        props["py-io-impl"] = "pyiceberg.io.fsspec.FsspecFileIO"
        if settings.gcs_project_id:
            props["gcs.project-id"] = settings.gcs_project_id
    return SqlCatalog(settings.iceberg_catalog_name, **props)

from typing import Dict, List, Optional
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col


spark = SparkSession.builder.getOrCreate()


def _parse_and_chunk_pdf(source_pdf_path: str) -> "DataFrame":
    """Parse a PDF with ai_parse_document and chunk with ai_prep_search.

    Returns a DataFrame with columns: path, chunk_id, chunk_position,
    chunk_to_retrieve, chunk_to_embed.
    """
    return spark.sql(f"""
        WITH parsed AS (
            SELECT
                path,
                ai_parse_document(content, MAP('version', '2.0')) AS parsed
            FROM READ_FILES('{source_pdf_path}', format => 'binaryFile')
        ),
        prepped AS (
            SELECT
                path,
                ai_prep_search(parsed) AS prepped
            FROM parsed
        )
        SELECT
            p.path,
            chunk.value:chunk_id::STRING          AS chunk_id,
            chunk.value:chunk_position::INT       AS chunk_position,
            chunk.value:chunk_to_retrieve::STRING AS chunk_to_retrieve,
            chunk.value:chunk_to_embed::STRING    AS chunk_to_embed
        FROM prepped p,
             LATERAL variant_explode(p.prepped:document:contents) AS chunk
    """)


def _write_chunks_table(chunks_df, chunks_table: str) -> None:
    """Write chunks to a CDF-enabled Delta table with the expected schema."""
    chunks_with_id = (
        chunks_df
        .withColumn("chunk_id", expr("uuid()"))
        .withColumn("source_path", col("path"))
        .select(
            "chunk_id",
            col("chunk_to_embed").alias("chunk_text"),
            col("chunk_to_retrieve").alias("retrieval_text"),
            "source_path",
        )
    )

    (
        chunks_with_id.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .saveAsTable(chunks_table)
    )

    spark.sql(
        f"ALTER TABLE {chunks_table} "
        f"SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )
    print(f"  Chunks table written: {chunks_table}")


def _ensure_index(
    endpoint_name: str,
    source_table: str,
    index_name: str,
    primary_key: str,
    embedding_source_column: str,
    embedding_model: str,
    pipeline_type: str,
    columns_to_sync: List[str],
) -> None:
    """Create a delta-sync vector search index if it doesn't already exist."""
    from databricks.vector_search.client import VectorSearchClient

    vsc = VectorSearchClient()

    # Wait for endpoint to be ONLINE
    print(f"  Waiting for endpoint '{endpoint_name}' to be ONLINE...")
    for _ in range(150):
        try:
            ep = vsc.get_endpoint(endpoint_name)
            status = ep.get("endpoint_status", {}).get("state", "")
            if status == "ONLINE":
                print(f"  Endpoint '{endpoint_name}' is ONLINE.")
                break
            print(f"    Endpoint status: {status} - waiting...")
            time.sleep(10)
        except Exception:
            print(f"    Endpoint '{endpoint_name}' not found yet - waiting...")
            time.sleep(10)
    else:
        print(f"  WARNING: Endpoint '{endpoint_name}' did not reach ONLINE status.")
        return

    # Create index, or skip if it already exists
    try:
        vsc.create_delta_sync_index(
            endpoint_name=endpoint_name,
            source_table_name=source_table,
            index_name=index_name,
            pipeline_type=pipeline_type,
            primary_key=primary_key,
            embedding_source_column=embedding_source_column,
            embedding_model_endpoint_name=embedding_model,
            columns_to_sync=columns_to_sync,
        )
        print(f"  Index '{index_name}' created and syncing.")
    except Exception as e:
        if "already exists" in str(e):
            print(f"  Index '{index_name}' already exists.")
        else:
            raise


def create_vector_search_index(
    catalog_name: str,
    schema_name: str,
    vs_config: Dict,
) -> Optional[str]:
    """Run the full parse → chunk → write → index pipeline.

    Parameters
    ----------
    catalog_name : str
        User catalog name.
    schema_name : str
        User schema name.
    vs_config : dict
        The vector_search section from the config YAML.

    Returns
    -------
    str or None
        Fully-qualified index name, or None if no index config was provided.
    """
    endpoint_name = vs_config["endpoint_name"]
    source_pdf = vs_config["source_pdf"]
    idx = vs_config.get("index", {})

    if not idx:
        return None

    chunks_table = f"{catalog_name}.{schema_name}.{idx['chunks_table']}"
    index_name = f"{catalog_name}.{schema_name}.{idx['index_name']}"

    # Parse and chunk
    print(f"  Parsing and chunking PDF: {source_pdf}")
    chunks_df = _parse_and_chunk_pdf(source_pdf)

    # Write chunks table
    _write_chunks_table(chunks_df, chunks_table)

    # Create index
    _ensure_index(
        endpoint_name=endpoint_name,
        source_table=chunks_table,
        index_name=index_name,
        primary_key=idx.get("primary_key", "chunk_id"),
        embedding_source_column=idx.get("embedding_source_column", "chunk_text"),
        embedding_model=idx.get("embedding_model", "databricks-gte-large-en"),
        pipeline_type=idx.get("pipeline_type", "TRIGGERED"),
        columns_to_sync=idx.get("columns_to_sync", ["chunk_id", "chunk_text", "source_path"]),
    )

    return index_name

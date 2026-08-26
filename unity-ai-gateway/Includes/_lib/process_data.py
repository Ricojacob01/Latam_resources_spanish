from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, regexp_replace, trim

from pyspark.sql.types import StructType, StructField, StringType, ArrayType

spark = SparkSession.builder.getOrCreate()


def process_csv_data(
    csv_path: str | Path,
    table_name: str
) -> None:
    """Load and process the bundled CSV into a Delta table.

    Reads the CSV that ships in the course's ``data/`` folder (a workspace file),
    not a Delta share. Serverless Spark can't read a ``/Workspace`` file path
    directly, so we load it with pandas and hand it to Spark; the dataset is tiny
    (~50 rows kept), so this is trivial. The transforms are unchanged from the
    former Delta-share path.
    """
    pdf = pd.read_csv(
        csv_path,
        dtype=str,          # keep everything as strings; Spark casts price below
        keep_default_na=False,
    )[["id", "name", "neighbourhood", "neighbourhood_cleansed", "summary", "price", "room_type"]]

    df = (
        spark.createDataFrame(pdf)
        # Make price numeric: remove $ and , then cast to double (empty strings become nulls when cast)
        .withColumn("price", regexp_replace(trim(col("price")), "[$,]", "").cast("double"))
        .withColumn(
            "listing_source_information",
            concat(
                lit("ID of the property: "), col("id"),
                lit("\nName of the property: "), col("name"),
                lit("\nSummary of the property: "), col("summary"),
            ),
        )
        .limit(50)
    )

    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


    # Example for setting up Genie-specific table
    # df.select(["id", "price"]).write.mode("overwrite").saveAsTable(table_name+"_genie")

def create_sample_labels(table_name: str = "tko_sample_labels") -> None:

    schema = StructType([
        StructField("eval_id", StringType(), True),
        StructField("request", StringType(), True),
        StructField("guidelines", ArrayType(StringType()), True),
        StructField("items", StringType(), True),     # optional
        StructField("metadata", StringType(), True),  # optional (JSON string)
        StructField("tags", StringType(), True),
    ])

    data = [
        # KA-only count (attributes from PDF)
        (
            "1",
            "How many properties have the Victorian home style?",
            [
                "Use only the Properties PDF to determine membership; do not use the price table.",
                "Return a single integer: count of properties with home_style = 'Victorian'.",
                "If the style is missing or ambiguous for a record, exclude it and note exclusions.",
                "Provide citations to the exact PDF page(s) used."
            ],
            None,
            "{}", 
            "supervisor_ka_genie"
        ),

        # KA → Genie average price conditioned on attribute from PDF
        (
            "2",
            "What's the average price for all properties with 2 bedrooms?",
            [
                "Resolve the set of property IDs using only the Properties PDF (bedrooms == 2); provide PDF citations.",
                "If zero IDs are found, abstain: respond 'No matching properties found' and ask for a clarifying filter.",
                "Query Genie for prices only for the KA-supplied IDs.",
                "Exclude null or non-numeric prices; do not impute.",
                "Compute arithmetic mean and report sample size n.",
                "Preserve units exactly as stored; do not add currency symbols unless present.",
                "If duplicate price rows exist per ID, include valid prices and report duplicate count.",
                "Output format: 'Average price: <numeric>' on first line; 'n: <count>' on second; 'Notes: <brief summary>' on third; 'Citations: <PDF pages>' on fourth.",
                "If the ID list length ≤ 20, include IDs; otherwise state 'IDs omitted for brevity.'"
            ],
            None,
            "{}", 
            "supervisor_ka_genie"
        ),

        # Another KA → Genie example with a style attribute
        (
            "3",
            "What is the average price for all properties with Victorian home style?",
            [
                "Use only the Properties PDF to select IDs where home_style = 'Victorian'; provide PDF citations.",
                "If KA returns zero IDs, abstain and ask the user to refine the filter.",
                "Use Genie to fetch prices for those IDs and compute the arithmetic mean; report n.",
                "Exclude null/non-numeric prices; do not impute.",
                "If conflicting sources are detected, prioritize PDF for attributes and Genie for prices; flag conflict in Notes.",
                "Output format and ID listing rules same as above."
            ],
            None,
            "{}", 
            "supervisor_ka_genie"
        ),
    ]

    df = spark.createDataFrame(data, schema=schema)

    (df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name))
# Structured Streaming in Databricks — Complete Explanation with PySpark

Structured Streaming is a Spark feature used to process continuously arriving data incrementally. In Databricks, you can use it with Auto Loader, Delta Lake, Kafka, and other streaming sources.

Since you're already building a Databricks Auto Loader project on GitHub, I'll explain Structured Streaming using the same three CSV files.

## 1. How Structured Streaming works

Your Auto Loader pipeline is a Structured Streaming application.

Structured Streaming commonly uses micro-batch processing: Spark processes new data in small batches and keeps track of progress using checkpoints.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



## 2. Batch processing vs. Structured Streaming

| Batch processing                 | Structured Streaming                      |
| -------------------------------- | ----------------------------------------- |
| `spark.read`                     | `spark.readStream`                        |
| Processes a fixed dataset        | Incrementally processes arriving data     |
| `df.write`                       | `df.writeStream`                          |
| Usually starts and finishes      | Can run continuously or in triggered runs |
| No streaming checkpoint required | Uses checkpointing for recovery           |

### Example: Batch processing

```
df = spark.read.csv(    "/Volumes/workspace/autoloader_demo/autoloader_files/input/",    header=True)display(df)
```

This reads the available files as a batch.

### Example: Structured Streaming using Auto Loader

```
df = (    spark.readStream    .format("cloudFiles")    .option("cloudFiles.format", "csv")    .option("header", "true")    .schema(order_schema)    .load(source_path))
```

This creates a streaming DataFrame that Auto Loader can use to incrementally discover files.

## 3. The five important components

1. Source: Where the data comes from, such as CSV files, Kafka, or Delta tables.
2. `readStream`: Reads incoming data incrementally.
3. Transformations: Applies operations such as filtering, selecting columns, casting, and aggregating.
4. `writeStream`: Writes processed records to the destination.
5. Checkpoint: Remembers processing progress so the stream can recover from interruptions.&#x20;

   [image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

   Databricks on AWS



## 4. Complete Structured Streaming example for your project

Use the paths and schema you previously created in your Auto Loader notebook.

```
from pyspark.sql.functions import col, current_timestamp# 1. Read incoming CSV files using Auto Loaderstreaming_df = (    spark.readStream    .format("cloudFiles")    .option("cloudFiles.format", "csv")    .option("header", "true")    .schema(order_schema)    .load(source_path))# 2. Apply transformationsprocessed_df = (    streaming_df    .filter(col("amount") > 0)    .withColumn("processed_at", current_timestamp())    .withColumn("source_file", col("_metadata.file_name")))# 3. Write streaming results to a NEW Delta tablequery = (    processed_df.writeStream    .format("delta")    .outputMode("append")    .option(        "checkpointLocation",        f"{base_path}/checkpoints/structured_orders"    )    .trigger(availableNow=True)    .toTable(        f"{catalog_name}.{schema_name}.structured_orders"    ))
```

This uses a different checkpoint and target table from your original `orders_bronze` pipeline, so the two exercises don't interfere with each other.

### Verify the results

```
display(    spark.table(        f"{catalog_name}.{schema_name}.structured_orders"    ))
```

If all three CSV files are present in your input Volume, you should see nine records.

## 5. Understanding streaming triggers

| Trigger                       | Behavior                                                                   |
| ----------------------------- | -------------------------------------------------------------------------- |
| `availableNow=True`           | Processes currently available data, then stops                             |
| `processingTime="10 seconds"` | Checks for new data in repeated micro-batches                              |
| Continuous processing         | Uses a specialized continuous processing model, subject to runtime support |

For your Databricks Free Edition serverless notebook, use:

```
.trigger(availableNow=True)
```

That lets you practice streaming semantics, checkpoints, and incremental processing without maintaining a long-running stream. Databricks also recommends AvailableNow for certain scheduled ingestion workloads.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



## 6. Test the streaming behavior interactively

Streaming ingestion simulator

Illustration

Initial run

Files processed

# 1

Total Delta rows

# 3

orders_1.csv

3 rows loaded

orders_2.csv

Not yet loaded

orders_3.csv

Not yet loaded

ResetNext streaming run&#x20;

In the third simulation step, the row count stays at six because no new files arrived. That is the checkpoint behavior you want to demonstrate in your GitHub project.

## 7. Interview questions to practice

Q1. What is Structured Streaming?\
It is Apache Spark's scalable, fault-tolerant stream-processing engine that supports incremental data processing.

Q2. What is the difference between Auto Loader and Structured Streaming?\
Structured Streaming is the general processing engine. Auto Loader is Databricks' specialized file-ingestion source built on that engine.

Q3. What is a micro-batch?\
A small set of newly available records processed together as part of a streaming query.

Q4. What happens when a stream stops unexpectedly?\
A properly checkpointed stream can recover processing progress when restarted with compatible settings.

Q5. Does Auto Loader automatically remove duplicate business records?\
No. It tracks file ingestion; duplicate order IDs across different files still need business-level deduplication.

Q6. What is the difference between `readStream` and `writeStream`?\
`readStream` defines the streaming source, while `writeStream` starts a query that delivers results to a destination.

For your GitHub portfolio, a logical next project is Structured Streaming: Bronze → Silver → Gold, where you use Auto Loader to ingest raw CSV files, clean and deduplicate incoming records, and create a final sales summary using Delta tables.

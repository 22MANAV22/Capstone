"""
checkpoint_manager.py — Track which Bronze batches have already been ingested.

Checkpoints are tiny JSON files written to S3 after each successful
Bronze ingestion. Before Bronze runs, it checks if a checkpoint exists
for that batch — if yes, it skips ingestion entirely.

FIX for PATH_NOT_FOUND:
  spark.read.text() throws an exception when the path does not exist.
  The old code caught all exceptions and returned False, but Databricks
  raises PATH_NOT_FOUND as an AnalysisException before the read even starts.
  The fix is to use dbutils.fs.ls() to check existence first — it returns
  an empty list or raises when the path is missing, without a Spark job.
  Only if the file exists do we read and parse it.
"""

import json
from datetime import datetime, timezone
from table_config import S3_BUCKET, S3_PROTOCOL


class CheckpointManager:

    def __init__(self, spark):
        self.spark = spark
        self.base  = f"{S3_PROTOCOL}://{S3_BUCKET}/checkpoints"

    def _path(self, batch_number, stage):
        """Return the full S3 path for a checkpoint file."""
        return f"{self.base}/batch_{batch_number}/{stage}.json"

    def _file_exists(self, path):
        """
        Check if an S3 path exists without triggering a Spark job.
        dbutils.fs.ls() raises an exception when the path is missing —
        we catch that and return False. This avoids PATH_NOT_FOUND from
        spark.read when the checkpoint hasn't been written yet.
        """
        try:
            files = dbutils.fs.ls(path)
            # ls on a file returns a list with one entry — check it's not empty
            return len(files) > 0
        except Exception:
            # Path does not exist — no checkpoint written yet
            return False

    def is_done(self, batch_number, stage):
        """
        Return True if this stage already completed successfully for this batch.
        Returns False if the checkpoint file doesn't exist (normal on first run).
        """
        path = self._path(batch_number, stage)

        # Check existence first — avoids PATH_NOT_FOUND from spark.read
        if not self._file_exists(path):
            print(f"    No checkpoint found for batch_{batch_number}/{stage} — will run.")
            return False

        # File exists — read and parse it
        try:
            df      = self.spark.read.text(path)
            content = df.first()[0]
            data    = json.loads(content)
            if data.get("status") == "completed":
                completed_at = data.get("completed_at", "unknown time")
                rows         = data.get("rows", 0)
                print(f"    CHECKPOINT HIT: batch_{batch_number}/{stage}")
                print(f"    Already completed at {completed_at} with {rows:,} rows.")
                print(f"    Skipping. To force re-run:")
                print(f"      CheckpointManager(spark).reset('{batch_number}')")
                return True
        except Exception as e:
            # Checkpoint file exists but is corrupt or unreadable — treat as not done
            print(f"    WARNING: Could not read checkpoint for batch_{batch_number}/{stage}: {e}")
            print(f"    Treating as not completed — will re-run.")

        return False

    def mark_done(self, batch_number, stage, rows=0):
        """
        Write a checkpoint file to S3 marking this stage as completed.
        Called immediately after a successful stage run.
        """
        path = self._path(batch_number, stage)
        data = {
            "batch"        : str(batch_number),
            "stage"        : stage,
            "status"       : "completed",
            "rows"         : rows,
            "completed_at" : datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        content_df = self.spark.createDataFrame(
            [(json.dumps(data),)], ["value"]
        )
        content_df.coalesce(1).write.mode("overwrite").text(path)
        print(f"    Checkpoint saved: batch_{batch_number}/{stage} ({rows:,} rows)")

    def reset(self, batch_number, stage="bronze"):
        """
        Delete a checkpoint to force that stage to re-run next time.

        Usage from a Databricks notebook:
          from checkpoint_manager import CheckpointManager
          CheckpointManager(spark).reset("2")
        """
        path = self._path(batch_number, stage)
        try:
            dbutils.fs.rm(path, recurse=True)
            print(f"Checkpoint deleted: {path}")
            print(f"Next Bronze run for batch_{batch_number} will re-ingest.")
        except Exception as e:
            print(f"Could not delete checkpoint {path}: {e}")

    def list_all(self):
        """
        Print a summary of all existing checkpoints.
        Useful for a quick health check before running the pipeline.

        Usage from a Databricks notebook:
          from checkpoint_manager import CheckpointManager
          CheckpointManager(spark).list_all()
        """
        try:
            batch_folders = dbutils.fs.ls(f"{self.base}/")
        except Exception:
            print("No checkpoints found. Pipeline has not run yet.")
            return

        print(f"\n{'Batch':<10} {'Stage':<12} {'Status':<12} {'Rows':<12} {'Completed At'}")
        print("-" * 68)

        for folder in batch_folders:
            try:
                stage_files = dbutils.fs.ls(folder.path)
                for sf in stage_files:
                    if not sf.name.endswith(".json"):
                        continue
                    try:
                        content = self.spark.read.text(sf.path).first()[0]
                        data    = json.loads(content)
                        print(
                            f"{data.get('batch','?'):<10} "
                            f"{data.get('stage','?'):<12} "
                            f"{data.get('status','?'):<12} "
                            f"{data.get('rows', 0):<12,} "
                            f"{data.get('completed_at','?')}"
                        )
                    except Exception:
                        pass
            except Exception:
                pass
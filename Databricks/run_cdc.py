"""Runner: CDC MERGE. Merges live Bronze rows into Silver."""
from cdc_engine import CDCEngine
CDCEngine(spark).run()
"""Runner: Bronze. Accepts batch_number: 1,2,3,4 or live."""
from bronze_engine import BronzeEngine
try:
    batch_number = dbutils.widgets.get("batch_number")
except:
    dbutils.widgets.text("batch_number", "1")
    batch_number = "1"
BronzeEngine(spark).run(batch_number)
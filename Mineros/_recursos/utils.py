# utilities/utils.es.py

from pyspark.dbutils import DBUtils
from datetime import datetime, timedelta
import random
import json

def add_orders_file(spark, working_dir: str, file_number: int, num_orders: int) -> str:
    """
    Crear un archivo JSON bajo {working_dir}/orders/{NN}.json
    con `num_orders` pedidos sintéticos.
    """
    dbutils = DBUtils(spark)
    base_date = datetime(2024, 1, 1)
    orders = []
    for i in range(num_orders):
        orders.append({
            "order_id": f"ORD{i + 1000 + file_number * 1000:05d}",
            "order_timestamp": (base_date + timedelta(days=random.randint(0, 30))).isoformat(),
            "customer_id": f"CUST{random.randint(1, 100):04d}",
            "notifications": {"email": random.choice([True, False]),
                              "sms": random.choice([True, False])}
        })
    file_name = f"{file_number:02d}.json"
    file_path = f"{working_dir}/orders/{file_name}"
    dbutils.fs.put(file_path, "\n".join(json.dumps(o) for o in orders), True)
    return f"Se crearon {num_orders} pedidos en orders/{file_name}"


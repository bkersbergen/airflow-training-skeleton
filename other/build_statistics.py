import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

dt = sys.argv[1]
print('working for date:{}'.format(dt))

spark = SparkSession.builder.getOrCreate()

registry = spark.read.json(
    "gs://mr_ds/price_paid_uk/*/registry.json"
)
print('registry count:{}'.format(registry.count()))

registry.withColumn(
    "transfer_date", F.col("transfer_date").cast("timestamp").cast("date")
).createOrReplaceTempView(
    "land_registry_price_paid_uk"
)


currency = spark.read.json("gs://mr_ds/currency/*.json")
print('currency count:{}'.format(currency.count()))


currency.withColumn(
    "date", F.col("date").cast("date")
).createOrReplaceTempView("currencies")



# Do some aggregations and write it back to Cloud Storage
aggregation = spark.sql(
    """
    SELECT
        CAST(CAST(transfer_date AS timestamp) AS date) transfer_date,
        county,
        district,
        city,
        `to` as currency,
        AVG(price * conversion_rate) as price
    FROM
        land_registry_price_paid_uk
    JOIN
        currencies
    ON
        currencies.date = land_registry_price_paid_uk.transfer_date
    WHERE
        transfer_date = '{}'
    GROUP BY
        currency,
        transfer_date,
        county,
        district,
        city
    ORDER BY
        county,
        district,
        city,
        currency
""".format(
        dt
    )
)


qty_output_records = aggregation.count()
print('qty_output_records:{}'.format(qty_output_records))

aggregation.write.mode("overwrite").partitionBy("transfer_date").parquet("gs://mr_ds/average_prices/")
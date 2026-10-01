import pyspark
from pyspark.sql.functions import *

def clean_data(df):
 pass


"""The clean_data function should:
• Remove rows where amount <= 0.
• Remove rows where name is NULL.
• Add a column amount_with_tax.
• Calculate amount_with_tax as amount * 1.20"""




def clean_data(df):
    return (
        df.filter(col("amount") > 0)
          .filter(col("name").isNotNull())
          .withColumn("amount_with_tax", col("amount") * 1.20)
    )

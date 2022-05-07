
import pyspark

#create spark session
spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Python spark SQL example") \
    .config('spark.driver.extraClassPath',"/c/Java/postgresql-42.3.4.jar") \
    .getOrCreate()

#create a function
def extract_payments_to_df():
#read table from the db using spark jdbc
    payment_df = spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://localhost:5432/dvdrental") \
        .option("dbtable", "payment") \
        .option("user", "postgres") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return payment_df

#create function
def extract_staff_to_df():
    staff_df = spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://localhost:5432/dvdrental") \
        .option("dbtable", "staff") \
        .option("user", "postgres") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return staff_df

def transform_avg_amount(payment_df, staff_df):
    #transform tables
    avg_amount = payment_df.groupBy("staff_id").mean("amount")
    #join the film df and avg_length table
    df = staff_df.join(
    avg_amount,
    staff_df.staff_id == payment_df.staff_id,
    )
    #print the movies df(arg):
    #print(movies_df.show())
    df = df.drop("staff_id")
    return df
    #print(df.show())


#laod tranformed data to DB
def load_df_to_db(df):
    mode = "overwrite"
    url = "jdbc:postgresql://localhost:5432/dvdrental"
    properties =    {"user":"postgres",
                     "password": "password",
                     "driver" : "org.postgresql.Driver"
                     }
    df.write.jdbc( url=url,
                    table = "avg_amount",
                    mode = mode,
                    properties = properties
                    )

if __name__ == "__main__":
    payment_df = extract_payments_to_df()
    staff_df = extract_staff_to_df()
    avgpayment_df = transform_avg_amount(payment_df, staff_df)
    load_df_to_db(avgpayment_df)

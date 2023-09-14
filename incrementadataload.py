from pyspark.sql import SparkSession
from configparser import ConfigParser
from pyspark.sql.functions import max
def create_spark_session():
    spark = SparkSession.builder.config("spark.jars","C:\installers\Drivers\postgresql-42.6.0.jar").appName("jdbc").master("local").getOrCreate()
    return spark
#1st report
def customers_updated_table():
    df = spark.read.parquet(input_path_cust)
    #df.show()
    latest_date = df.selectExpr("max(created_date) as max_date").collect()[0]["max_date"]
    print("Latest Date:", latest_date)
    db_customers = spark.read.jdbc(url=db_properties["url"], table='customers', properties=db_properties)
    #db_customers.show()
    filtered_db_df = db_customers.filter(db_customers["created_date"] > latest_date)
    filtered_db_df.show(100)
    print(filtered_db_df.count())
    table_name = "customers_updated_table1"
    filtered_db_df.write.mode("append").jdbc(db_properties["url"], table_name, properties=db_properties)
    filtered_db_df.write.mode("append").parquet("C:\Parquet\Localpath1\customers_updated_table1")
# #2nd report
# def items_updated_table():
#     df = spark.read.parquet(input_path_items)
#     latest_date = df.selectExpr("max(created_date) as max_date").collect()[0]["max_date"]
#     print("Latest Date:", latest_date)
#     db_items = spark.read.jdbc(url=db_properties["url"], table='items', properties=db_properties)
#     #db_items.show()
#     filtered_db_df = db_items.filter(db_items["created_date"] > latest_date)
#     filtered_db_df.show()
#     table_name = "items_updated_table1"
#     filtered_db_df.write.mode("append").jdbc(db_properties["url"], table_name, properties=db_properties)
#     filtered_db_df.write.mode("append").parquet("C:\Parquet\Localpath1\items_updated_table1")
#3rd report
def order_details_updated_table():
    df = spark.read.parquet(input_path_details)
    latest_date = df.selectExpr("max(created_date) as max_date").collect()[0]["max_date"]
    print("Latest Date:", latest_date)
    db_order_details = spark.read.jdbc(url=db_properties["url"], table='order_details', properties=db_properties)
    #db_order_details.show()
    filtered_db_df = db_order_details.filter(db_order_details["created_date"] > latest_date)
    filtered_db_df.show()
    table_name = "order_details_updated_table1"
    filtered_db_df.write.mode("append").jdbc(db_properties["url"], table_name, properties=db_properties)
    filtered_db_df.write.mode("append").parquet("C:\Parquet\Localpath1\order_details_updated_table1")
#4th report
# def orders_updated_table():
#     df = spark.read.parquet(input_path_orders)
#     latest_date = df.selectExpr("max(created_date) as max_date").collect()[0]["max_date"]
#     print("Latest Date:", latest_date)
#     db_orders = spark.read.jdbc(url=db_properties["url"], table='orders', properties=db_properties)
#     #db_customers.show()
#     filtered_db_df = db_orders.filter(db_orders["created_date"] > latest_date)
#     filtered_db_df.show()
#     table_name = "orders_updated_table1"
#     filtered_db_df.write.mode("append").jdbc(db_properties["url"], table_name, properties=db_properties)
#     filtered_db_df.write.mode("append").parquet("C:\Parquet\Localpath1\orders_updated_table1")
#5th report
def salesperson_updated_table():
    df = spark.read.parquet(input_path_sales)
    latest_date = df.selectExpr("max(created_date) as max_date").collect()[0]["max_date"]
    print("Latest Date:", latest_date)
    db_salesperson = spark.read.jdbc(url=db_properties["url"], table='salesperson', properties=db_properties)
    #db_salesperson.show()
    filtered_db_df = db_salesperson.filter(db_salesperson["created_date"] > latest_date)
    filtered_db_df.show()
    table_name = "salesperson_updated_table1"
    filtered_db_df.write.mode("append").jdbc(db_properties["url"], table_name, properties=db_properties)
    filtered_db_df.write.mode("append").parquet("C:\Parquet\Localpath1\salesperson_updated_table1")
#6th report
def ship_to_updated_table():
    df = spark.read.parquet(input_path_shipto)
    latest_date = df.selectExpr("max(created_date) as max_date").collect()[0]["max_date"]
    print("Latest Date:", latest_date)
    db_ship_to = spark.read.jdbc(url=db_properties["url"], table='ship_to', properties=db_properties)
    #db_ship_to.show()
    filtered_db_df = db_ship_to.filter(db_ship_to["created_date"] > latest_date)
    filtered_db_df.show()
    table_name = "shipto_updated_table1"
    filtered_db_df.write.mode("append").jdbc(db_properties["url"], table_name, properties=db_properties)
    filtered_db_df.write.mode("append").parquet("C:\Parquet\Localpath1\shipto_updated_table1")
if __name__ == "__main__":
    spark = create_spark_session()
    config = ConfigParser()
    config_path = "C:/Users/GVEERESH/PycharmProjects/pythonProject1/database.properties"
    with open(config_path, "r") as config_file:
        content = config_file.read()
        config.read_string(content)
    db_properties = {
    "driver": config.get("database", "driver"),
    "user": config.get("database", "user"),
    "url": config.get("database", "url"),
    "password": config.get("database", "password")
    }
    input_path_cust = "C:/Users/GVEERESH/PycharmProjects/pythonProject1/output_data1/customers"
    input_path_items = "C:/Users/GVEERESH/PycharmProjects/pythonProject1/output_data1/items"
    input_path_details = "C:/Users/GVEERESH/PycharmProjects/pythonProject1/output_data1/order_details"
    input_path_orders = "C:/Users/GVEERESH/PycharmProjects/pythonProject1/output_data1/orders"
    input_path_sales = "C:/Users/GVEERESH/PycharmProjects/pythonProject1/output_data1/salesperson"
    input_path_shipto = "C:/Users/GVEERESH/PycharmProjects/pythonProject1/output_data1/ship_to"

    # Call the generate_monthly_report method for each report
    customers_updated_table()#1
    # items_updated_table()#2
    order_details_updated_table()#3
    # orders_updated_table()#4
    salesperson_updated_table()#5
    ship_to_updated_table()#6
    #Stop the Spark session

    spark.stop()

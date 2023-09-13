from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("example") \
    .config("spark.driver.port", "4041") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .getOrCreate()

# Создаем датафрейм с продуктами
data_products = [("product1", 1), ("product2", 2), ("product3", 3)]
columns_products = ["product_name", "product_id"]
df_products = spark.createDataFrame(data=data_products, schema=columns_products)

# Создаем датафрейм с категориями
data_categories = [(1, "category1"), (2, "category2"), (3, "category3")]
columns_categories = ["category_id", "category_name"]
df_categories = spark.createDataFrame(data=data_categories, schema=columns_categories)

# Создаем связи между продуктами и категориями
data_relations = [("product1", 1), ("product1", 2), ("product2", 2)]
columns_relations = ["product_name", "category_id"]
df_relations = spark.createDataFrame(data=data_relations, schema=columns_relations)


def get_product_categories(df_products, df_relations, df_categories):
    # Присоединяем продукты к связям по их названию (left join)
    joined_df = df_relations.join(df_products, "product_name", "left")

    # Присоединяем категории к связям по их ID (left join)
    joined_df = joined_df.join(df_categories, "category_id", "left")

    # Выбираем нужные столбцы (Имя продукта - Имя категории)
    result_df = joined_df.select("product_name", "category_name")

    # Создаем датафрейм с продуктами без категорий и заполняем столбец category_name значением "None"
    products_without_categories = df_products.join(df_relations, "product_name", "left_anti")
    products_without_categories = products_without_categories.withColumn("category_name", lit("None"))

    # Присоединяем продукты без категорий к основному результату
    result_df = result_df.union(products_without_categories.select("product_name", "category_name"))

    return result_df


# Вызов метода
result_df = get_product_categories(df_products, df_relations, df_categories)

# Отображение результата
result_df.show()
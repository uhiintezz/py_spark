## Разработка

##### 1) Скачать spark-3.4.1-bin-hadoop3.tgz с сайта [Apache Spark website](https://spark.apache.org/downloads.html)  и распаковать.

##### 2) Скачать [Apache Hadoop](https://hadoop.apache.org/releases.html) и распаковать.

##### 3) Скачать [winutils](https://github.com/steveloughran/winutils) и заменить папку bin hadoop на скачанную.

##### 4) Активировать виртуальное окружение
    
Linux

    source djangoenv/bin/activate
    
Windows

    ./djangoenv/Scripts/activate

##### 5) Установить pyspark

    pip install pyspark

##### 6) Запустить main.py

    python manage.py main
    


## Результирующий запрос:

    +------------+-------------+
    |product_name|category_name|
    +------------+-------------+
    |    product1|    category1|
    |    product1|    category2|
    |    product2|    category2|
    |    product3|         None|
    +------------+-------------+



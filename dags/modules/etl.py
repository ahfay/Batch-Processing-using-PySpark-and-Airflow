from .connection import *   
import pandas as pd 

def extract_transform_top_countries():
    # Create view city
    df_city = spark_get_conn("city")
    df_city.createOrReplaceTempView("city")

    # Cretae view country
    df_country = spark_get_conn("country")
    df_country.createOrReplaceTempView("country")

    # Create view customer
    df_customer = spark_get_conn("customer")
    df_customer.createOrReplaceTempView("customer")

    # Create view address
    df_address = spark_get_conn("address")
    df_address.createOrReplaceTempView("address")

    # Query for answering the first question
    df_result = spark_get_session().sql('''
                SELECT
                    country,
                    COUNT(country) as total,
                    current_date() as date,
                    'fayyadh' as data_owner
                FROM customer
                JOIN address ON customer.address_id = address.address_id
                JOIN city ON address.city_id = city.city_id
                JOIN country ON city.country_id = country.country_id
                GROUP BY country
                ORDER BY total DESC
    ''')

    df_result.write.mode('overwrite') \
        .partitionBy('date') \
        .option('compression', 'snappy') \
        .option('partitionOverwriteMode', 'dynamic') \
        .save('output/data_result_1')
        
        
def load_top_countries():
    df = pd.read_parquet('output/data_result_1')
    engine = tidb_get_conn()
    df.to_sql(name='top_country', con=engine, if_exists='append')
    
def extract_transform_total_film():
    # Create view film
    df_film = spark_get_conn("film")
    df_film.createOrReplaceTempView("film")

    # Create view category
    df_category = spark_get_conn("category")
    df_category.createOrReplaceTempView("category")

    # Create view film_category
    df_film_cat = spark_get_conn("film_category")
    df_film_cat.createOrReplaceTempView("film_category")

    # Query for answering the second question
    df_result_2 = spark_get_session().sql(
        '''
        SELECT 
        name as category_name,
        COUNT(*) as total,
        'fayyadh' as data_owner
        FROM film
        JOIN film_category ON film.film_id = film_category.film_id
        JOIN category ON film_category.category_id = category.category_id
        GROUP BY name
        ORDER BY total desc;
        '''
    )
    
    # Save to local folder
    df_result_2.write.mode('overwrite') \
        .option('compression', 'snappy') \
        .option('partitionOverwriteMode', 'dynamic') \
        .save('output/data_result_2')
        
def load_total_film():
    df = pd.read_parquet('output/data_result_2')
    engine = tidb_get_conn()
    df.to_sql(name='total_film', con=engine, if_exists='append')
    
    

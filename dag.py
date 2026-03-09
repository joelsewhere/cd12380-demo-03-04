from airflow.sdk import dag, task, task_group, Param
import pathlib
from datetime import datetime
import os

SCHEMA="{{'scraped_quotes' if params.environment == 'production' else 'dev_joel'}}"
QUOTES = "{{'quotes' if params.environment == 'production' else 'scraped_quotes__quotes'}}"
AUTHORS = "{{'authors' if params.environment == 'production' else 'scraped_quotes__authors'}}"
DAG_ROOT=pathlib.Path(__file__).parent
BUCKET="{{ 'l3-external-storage-753900908173' if params.environment == 'production' else 'l3-external-storage-753900908173' }}"
S3_KEYS={
    'extract': '{{ dag.dag_id }}/extract/{{ ds }}',
    'transform': '{{ dag.dag_id }}/transform/unprocessed/{{ ds }}',
    'processed': '{{ dag.dag_id }}/transform/processed/{{ ds }}'
    }

@dag(
    schedule='@daily',
    start_date=datetime(2025, 3, 8),
    end_date=datetime(2026, 3, 16),
    params={
        'environment': Param(
            os.getenv('environment', 'production'),
            dtype='string',
            enum=['development', 'production']
            )
        }
    )
def quotes_scraper():

    @task_group
    def extract():

        @task
        def quotes(filepath, extract_key, BUCKET):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from bs4 import BeautifulSoup
            
            # Collect quotes
            html = pathlib.Path(filepath).read_text()
            
            # Push quotes to S3
            hook = S3Hook()
            hook.load_string(
                string_data=html,
                key=extract_key + '/quotes.html',
                bucket_name=BUCKET,
                replace=True,
                )
            
            # Scrape author urls
            soup = BeautifulSoup(html, features="lxml")
            author_containers = soup.find_all('small', {'class': 'author'})
            author_urls = [x.parent.find('a').attrs['href'] for x in author_containers]

            # Push author urls
            return author_urls
        
        @task
        def authors(author_links, extract_key, BUCKET, ds):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            # Initialize S3 Hook
            hook = S3Hook()

            # Loop over items in author_links
            for link in author_links:

                # Isolate the author's name in the author link
                author_name = link.split('/')[-1]

                # Define the filepath to the html file
                filepath = (
                    pathlib.Path(__file__).parent / 
                    'authors' / 
                    (ds + '-' + author_name + '.html')
                    )
                
                # Read the html from the file
                html = filepath.read_text()

                # Define the S3 Key for the raw author html file
                key = extract_key + f'/authors/{author_name}.html'

                # Push the author html to S3
                hook.load_string(
                    string_data=html,
                    key=key,
                    bucket_name=BUCKET,
                    replace=True
                    )
        
        # Define the filepath for the quotes html file
        filepath = (DAG_ROOT / 'quotes' / 'quotes-{{ ds }}.html').as_posix()

        # Call the `quotes` task
        author_links = quotes(filepath, S3_KEYS['extract'], BUCKET)

        # Call the `authors` task
        authors(author_links, S3_KEYS['extract'], BUCKET)

    @task_group
    def transform():

        @task
        def quotes(extract_key, transform_key, BUCKET):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from bs4 import BeautifulSoup
            import pandas as pd
            import json

            hook = S3Hook()
            html = hook.read_key(
                key=extract_key + '/quotes.html',
                bucket_name=BUCKET
                )
            
            soup = BeautifulSoup(html, features='lxml')

            quote_containers = soup.find_all('div', {'class': 'quote'})

            data = []

            for container in quote_containers:

                quote = container.find('span', {'class': 'text'}).text
                author = container.find('small', {'class': 'author'}).text
                tags = [
                    tag.text for tag in 
                    container.find('div', {'class': 'tags'}).find_all('a', {'class': 'tag'})
                ]
                data.append(
                    {
                        "quote": quote,
                        "author": author,
                        "tags": json.dumps(tags)
                    }
                )
            
            csv = pd.DataFrame(data).to_csv(index=False)

            hook.load_string(
                string_data=csv,
                key=transform_key + '/quotes.csv',
                bucket_name=BUCKET,
                replace=True
            )

        @task  
        def authors(extract_key, transform_key, BUCKET):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from bs4 import BeautifulSoup
            import pandas as pd

            hook = S3Hook()

            author_keys = hook.list_keys(
                prefix=extract_key + '/authors/',
                bucket_name=BUCKET
                )

            data = []
            for key in author_keys:

                html = hook.read_key(
                    key=key,
                    bucket_name=BUCKET
                    )
                
                soup = BeautifulSoup(html)

                author_details = soup.find('div', {'class': 'author-details'})

                data.append(
                    {
                        'author_name': author_details.find('h3', {'class': 'author-title'}).text,
                        'author_birthdate': author_details.find('span', {'class': 'author-born-date'}).text,
                        'author_birthplace': author_details.find('span', {'class': 'author-born-location'}).text,
                        'author_description': author_details.find('div', {'class': 'author-description'}).text
                        }
                )

            csv = pd.DataFrame(data).to_csv(index=False)
            hook.load_string(
                string_data=csv,
                key=transform_key + '/authors.csv',
                bucket_name=BUCKET,
                replace=True,
                )
        
        args = [S3_KEYS['extract'], S3_KEYS['transform'], BUCKET]
        quotes(*args), authors(*args)

    @task_group
    def redshift_init():
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

        schema = SQLExecuteQueryOperator(
            task_id='schema',
            conn_id="redshift_default",
            sql="CREATE SCHEMA IF NOT EXISTS " + SCHEMA
            )

        quotes= SQLExecuteQueryOperator(
            task_id="quotes",
            conn_id="redshift_default",
            sql=f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.{QUOTES} (
                quote  VARCHAR(MAX),
                author VARCHAR(255),
                tags   SUPER
                )""",
            )

        authors = SQLExecuteQueryOperator(
            task_id="authors",
            conn_id="redshift_default",
            sql=f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.{AUTHORS} (
                author_name        VARCHAR(255),
                author_birthdate   VARCHAR(255),
                author_birthplace  VARCHAR(255),
                author_description VARCHAR(MAX)
                )""",
            )

        schema >> [quotes, authors]

    @task_group
    def load():

         from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

         quotes = S3ToRedshiftOperator(
            task_id="quotes",
            table=QUOTES,
            schema=SCHEMA,
            s3_bucket=BUCKET,
            s3_key=S3_KEYS['transform'] + '/quotes.csv',
            copy_options=[
                "CSV",
                "IGNOREHEADER 1",
                ],
         )

         authors = S3ToRedshiftOperator(
            task_id="authors",
            table=AUTHORS,
            schema=SCHEMA,
            s3_bucket=BUCKET,
            s3_key= S3_KEYS['transform'] + '/authors.csv',
            method='UPSERT',
            upsert_keys=['author_name'],
            copy_options=[
                "CSV",
                "IGNOREHEADER 1",
                ],
            )
         
         quotes, authors

        
    extract() >> transform() >> redshift_init() >> load()
    
quotes_scraper()

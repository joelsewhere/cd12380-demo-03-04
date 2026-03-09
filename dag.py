from airflow.sdk import dag, task, task_group, Param
import pathlib
from datetime import datetime

SCHEMA="{{'scraped_quotes' if params.environment == 'development' else 'dev_joel'}}"
DAG_ROOT=pathlib.Path(__file__).parent
BUCKET="{{ 'l3-external-storage-753900908173' if params.environment == 'development' else 'l3-external-storage-753900908173-dev' }}"
S3_KEYS={
    'extract': '{{ dag.dag_id }}/extract/{{ ds }}',
    'transform': '{{ dag.dag_id }}/transform/unprocessed/{{ ds }}',
    'processed': '{{ dag.dag_id }}/transform/processed/{{ ds }}'
    }

@dag(
    schedule='@daily',
    start_date=datetime(2025, 3, 5),
    end_date=datetime(2026, 3, 13),
    params={
        'environment': Param('development', dtype='string', enum=['developmenr', 'environment'])
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
        author_links = quotes(filepath, S3_KEYS['extract'])

        # Call the `authors` task
        authors(author_links, S3_KEYS['extract'])

    @task_group
    def transform(extract_key, transform_key):

        def quotes(extract_key, transform_key, BUCKET):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from bs4 import BeautifulSoup
            import pandas as pd

            hook = S3Hook()
            html = hook.read_key(
                key=extract_key + '/quotes.html',
                bucket_name=BUCKET,
                )
            soup = BeautifulSoup(html)

            quote_containers = soup.find_all('div', {'class': 'quote'})
            data = []
            for container in quote_containers:

                quote = container.find('span', {"class": "text"}).text
                author = container.find('small', {"class": "author"}).text
                tags = [
                    tag.text for tag in
                    container.find('div', {"class": "tags"}).find_all('tag')
                    ]
                data.append(
                    {
                        "quote": quote,
                        "author": author,
                        "tags": tags
                    }
                )

            csv = pd.DatFrame(data).to_csv(index=False)
            
            hook.load_string(
                string_data=csv,
                key=transform_key + '/quotes.csv',
                bucket_name=BUCKET,
                replace=True,
                )
            
        def authors(extract_key, transform_key, BUCKET):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from bs4 import BeautifulSoup
            import pandas as pd

            hook = S3Hook()
            author_keys = hook.list_keys(
                prefix=extract_key + '/authors/',
                bucket_name=BUCKET,
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

        quotes(extract_key, transform_key, BUCKET)
        authors(extract_key, transform_key, BUCKET)

    @task_group
    def load():

         from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

         quotes = S3ToRedshiftOperator(
             task_id="quotes",
             table="{{'quotes' if params.environment == 'production' else 'scraped_quotes__quotes'}}",
             schema=SCHEMA,
             s3_bucket=BUCKET,
             s3_key=S3_KEYS['transform'] + '/quotes.csv'
         )

         authors = S3ToRedshiftOperator(
             task_id="authors",
             table="{{'authors' if params.environment == 'production' else 'scraped_quotes__authors'}}",
             schema=SCHEMA,
             s3_bucket=BUCKET,
             s3_key= + '/authors.csv',
             method='UPSERT',
             upsert_keys='author_name'
            )
         
         quotes, authors

        
    extract() >> transform() >> load()
    
quotes_scraper()

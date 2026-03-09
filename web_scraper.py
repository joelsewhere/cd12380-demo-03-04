from requests import get
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

project_root = Path(__file__).parent

def find_next_button(soup):

    try:
        next_page = (
            soup
            .find("ul", {"class": "pager"})
            .find('li', {"class": "next"})
            .find('a')
            .attrs['href']
            )
        
        return next_page
    
    except AttributeError:
        pass

def write_author_pages(soup, date_siffix):

    author_containers = soup.find_all('small', {'class': 'author'})
    author_links = [x.parent.find('a') for x in author_containers]
    for author in author_links:

        author_path = author.attrs['href'] + '/'
        url = root + author_path
        result = get(url)
        file_path = project_root / 'authors' / (date_suffix + '-' + author_path.split("/")[-2] + '.html')
        with file_path.open('w') as file:
            print('Writing', file_path.as_posix() + '...')
            file.write(result.text)

def write_quotes(html, date_suffix):

    website = Path(__file__).parent / 'quotes'
    file_path = website / f'quotes-{date_suffix}.html'

    with file_path.open('w') as file:
        print(f'Writing {file_path.as_posix()}...')
        file.write(html)

filepath = (project_root / 'quotes-{{ ds }}').as_posix()

def scrape(filepath):
    from bs4 import BeautifulSoup

    html = Path(filepath).read_text()
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
    
    return data





if __name__ == "__main__":

    root = 'https://quotes.toscrape.com'
    html = get(root).text
    soup = BeautifulSoup(html, features="lxml") 
    next_page = find_next_button(soup)

    date = datetime.now()
    date_suffix = date.strftime("%Y-%m-%d")
    
    while next_page:

        write_quotes(html, date_suffix)
        write_author_pages(soup, date_suffix,)
        url = root + next_page
        html = get(url).text
        soup = BeautifulSoup(html, features="lxml")
        next_page = find_next_button(soup)
        date = date + timedelta(days=1)
        date_suffix = date.strftime("%Y-%m-%d")

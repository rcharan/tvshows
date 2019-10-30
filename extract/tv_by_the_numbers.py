import sys
sys.path.append('..')

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlalchemy
import config
import time
import re
import copy
import mysql.connector
config.schema = 'tvshows'


################################################################################
#
# Part 1 : Set up a database connection and utilities
#           available throughout
#
################################################################################

_sql_alchemy_connection = (
                                f'mysql+mysqlconnector://'
                                f'{config.user}:{config.password}'
                                f'@{config.host}:{config.port}'
                                f'/{config.schema}'
                           )
db = sqlalchemy.create_engine(_sql_alchemy_connection,
                              echo = False,
                              connect_args = {'ssl_disabled' : True})

def query(q):
    return pd.read_sql_query(q, db)

def query_list(col, table, distinct = True):
    elts = ['SELECT',
            'DISTINCT' if distinct else '',
            col,
            'FROM',
            table]
    query_str = ' '.join(elts)
    df = query(query_str)
    l = df.iloc[:,0].tolist()
    return l


################################################################################
#
# Part 2 : Generic Utilities
#
################################################################################

def _soupify_response(response):
    return BeautifulSoup(response.content, 'html.parser')

def _fetch_page(url):
    page = requests.get(url, timeout=5)
    return page

################################################################################
#
# Part 3 : Generic function implementing error handling for
#           fetching, parsing, and inserting data
#
################################################################################

def _create_data_fetcher(parser,
                         inserter,
                         fetcher         = _fetch_page,
                         soupifyer       = _soupify_response):
    # Returns nothing, silently, on success
    # Raises an error when there is no data to lose
    # Returns relevant data when raising an error would lose data
    # Return values must be properly handled by caller
    # kwargs are passed to both parser and inserter, which must accept them
    # url is always passed as a kwarg
    def fetch(url, **kwargs):
        # Make the url available to parser and inserter
        kwargs['url'] = url

        # Get the HTTP response
        try:
            response = fetcher(url)
        except:
            print('Failed to fetch data; terminating with error')
            raise

        if response.status_code != 200:
            print(f'Bad status code {page.status_code}.'
                  f'Returning the response then terminating')
            return {'response' : response}

        # Use beautiful soup
        # (generically this is a first parsing step of two)
        try:
            soup = soupifyer(response)
        except Exception as e:
            print('Error soupifying; possible malformed HTML.'
                  'Returning the response then terminating')
            print(e)
            return {'response' : response}

        # Parse the soup as a pandas dataframe (df)
        # (generically this is the second parsing step of two)
        try:
            df = parser(soup, **kwargs)
        except Exception as e:
            print('Error parsing; possibly unanticipated data structure')
            print('Returning response and soup')
            print(e)
            return {'response' : response,
                    'soup'     : soup}

        # Insert the df
        try:
            inserter(df, **kwargs)
        except Exception as e:
            print('Error inserting')
            print('Returning response, soup, df')
            print(e)
            return {'response' : response,
                    'soup'     : soup,
                    'df'       :  df}

    return fetch

################################################################################
#
# Part 4 : Generic function for scraping a lot of pages
#
################################################################################

def iterate_scraping(scraper, input_list, sleep_time = 0.1, on_fail = 'abort'):
    for page in input_list:
        error_data = scraper(page)

        # Scraper returns None on success, data on failure
        #  This handles failures
        if error_data and on_fail == 'abort':
            print('Aborting Loop')
            return(error_data)
        elif error_data:
            print( '\n')
            print( '--------------------WARNING----------------------------')
            print( 'See error message above                                ')
            print(f'Proceeding to parse; possible data loss of input {page}')
            print('\n\n')
        else:
            print(f'Successfully scraped with input {page}')
            time.sleep(sleep_time)

################################################################################
#
# Part 5 : Generic function for building a list of targets to scrape
#            excluding those already scraped
#
################################################################################


def _get_missing_scrape_targets(target_list, query_col, query_table, verbose = True):
    try:
        have_list = query_list(query_col, query_table, distinct = True)
    except sqlalchemy.exc.DBAPIError as e:
        if isinstance(e.orig, mysql.connector.errors.ProgrammingError):
            to_do = target_list
            print(f'Unable to detect the database, assuming it is empty and'
                  f' no scraping has yet occured.\n'
                  f'Please confirm that the error message below matches')
            print(e._message())
        else:
            raise e
    else:
        difference = set(target_list).difference(have_list)
        to_do = list(difference)
        print(f'{len(have_list)} pages already scraped detected')

    print(f'{len(to_do)} pages needing scraping detected')
    return to_do

################################################################################
#
# Part 6 : Parse the pages that contain the articles
#            then write that information to a database
#
#    Exports:
#       min_page_num        - minimum to look for
#       max_page_num        - maximum to look for
#       get_missing_pages() - list of missing page numbers
#       fetch_page_num(page_num) - fetch, parse, and insert
#       update_pages_database()  - iterated scraping of missing pages
#
################################################################################

max_page_num = 351
min_page_num = 1
# Given a page number, return the URL to the list of ratings
def _page_get_url(page_num):
    base = 'https://tvbythenumbers.zap2it.com/category/daily-ratings/'
    if page_num != 1:
        return base + f'page/{page_num}'
    else:
        return base

# Input : a beautiful soup object with associated page number and url
# Output: a pandas data-frame
def _parse_page_soup(soup, **kwargs):
    url      = kwargs['url']
    page_num = kwargs['page_num']

    content  = soup.find(id = 'post-load')
    articles = content.find_all('article')
    def parse_article(article):
        a_tag = article.find('h2').find('a')
        return {'page_num' : page_num,
                'url'      : url,
                'link_text': a_tag.get_text(),
                'link'     : a_tag['href']
                }
    records = [parse_article(article) for article in articles]
    return pd.DataFrame.from_records(records)

# Insert into the database
def _insert_page_data(df, **kwargs):
    df.to_sql('tv_by_the_numbers_pages', db,
              index = False, if_exists = 'append')


_fetch_page_data = _create_data_fetcher(_parse_page_soup, _insert_page_data)
def fetch_page_data(page_num):
    assert page_num >= min_page_num
    assert page_num <= max_page_num
    url = _page_get_url(page_num)

    return _fetch_page_data(url, page_num = page_num)

def get_missing_pages():
    return _get_missing_scrape_targets(
            range(min_page_num , max_page_num+1),
            'page_num',
            'tv_by_the_numbers_pages'
            )

def update_pages_database():
    to_do = get_missing_pages()
    return iterate_scraping(fetch_page_data, to_do)

################################################################################
#
# Part 6: Fetch the articles themselves
#
################################################################################

bad_urls = []

def get_missing_articles():
    target = query_list('link', 'tv_by_the_numbers_pages')
    to_do  = _get_missing_scrape_targets(target,
                                       'url',
                                       'tv_by_the_numbers_articles')
    to_do = list(filter(lambda u : u not in bad_urls, to_do)    )
    return to_do



_known_headers = ['show', 'shows', 'viewers (000s)', 'viewers (000s, live+sd)',
                 'adults 18-49 rating/share', 'showq', 'viewers (millions)',
                 'net', 'total viewers (000s)',
                 'adults 18-49 rating (live+sd)', 'time', '18-49 rating'
                 'adults 18-49 rating', 'viewership (000s, live+sd)']

_found_headers = copy.copy(_known_headers)

def _parse_article_soup(soup, **kwargs):
    url = kwargs['url']

    article = soup.find('article')

    # Collect the tags and title
    tags = article['class']
    article_title = article.find('p').get_text()

    # Parse to a table (list of rows of strings)
    table = article.find('table').find('tbody')
    rows = table.find_all('tr')
    def get_entries(row):
        return [entry.get_text().strip() for entry in row.find_all('td')]
    rows = [get_entries(r) for r in rows]

    # Header/data format
    header = rows[0]
    data   = rows[1:]

    header = [h.lower().strip() for h in header]
    notes  = []

    allowed_headers = [
        {'re' : 'time', 'field' : 'time', 'note' : []},
        {'re' : 'show', 'field' : 'show', 'note' : []},
        {'re' : '(18-49 rating).*(live).*(sd)', 'field' : 'demo_rating', 'note' : ['live+sd']},
        {'re' : '18-49 rating',                 'field' : 'demo_rating', 'note' : []},
        {'re' : '(net$|network)', 'field' : 'network', 'note' : []},
        {'re' : '(view).*(000s).*(live).*(sd)', 'field' : 'viewers', 'note' : ['live+sd', '000s']},
        {'re' : '(view).*(000s)', 'field' : 'viewers', 'note' : ['000s']},
        {'re' : '(view).*(millions)', 'field' : 'viewers', 'note' : ['millions']}
    ]

    # Validate and parse the header
    #  for each header, format as desired column names
    parsed_header = []
    for h in header:
        success = False
        for possibility in allowed_headers:
            # On succesful match
            if re.search(possibility['re'], h):
                parsed_header.append(possibility['field'])
                notes += possibility['note']
                success = True

                #Update the list of found headers
                if h not in _found_headers:
                    print(f'Found new header {h}'
                          f' parsed as {possibility["field"]}'
                          f' with notes {possibility["note"]}'
                          f' at url {url}')
                    _found_headers.append(h)

        if success:
            continue

        raise UnexpectedDataFormat(f'Headers don\'t match: consider '
                                   f'\n{h} in'
                                   f'\nheader = {header}'
                                   f'\nfrom url = {url}')

    # Turn rows into records
    records = []
    for row in data:
        # Check for empty row
        filtered = list(filter(None, row))
        if not filtered:
            empty_row = True
        else:
            empty_row = False


        # Empty rows are okay if you already have data
        # More detailed checks could be performed e.g. the next row should be
        #  full, and an empty row should not end the table
        if empty_row and len(records) == 0:
            raise UnexpectedDataFormat(f'Empty First Row at url = {url}')
        elif empty_row:
            continue

        # An empty first cell is okay if it can be filled from the preceding
        #  row
        if not row[0] and len(filtered) == len(header) - 1 and len(records) != 0:
            row[0] = records[-1][header[0]]
        elif len(filtered) != len(header):
            raise UnexpectedDataFormat(f'Unexpected Missing Data'
                                       f' at url = {url}')

        # The data checks out and we can return it with extra metadata
        record_dict = dict(zip(parsed_header, row))
        record_dict['tags'] = str(tags)
        record_dict['article_title'] = article_title
        record_dict['url'] = url
        record_dict['notes'] = str(notes)
        if 'network' not in record_dict:
            record_dict['network'] = None
        records.append(record_dict)

    return pd.DataFrame.from_records(records)

def _insert_article_data(df, **kwargs):
    df.to_sql('tv_by_the_numbers_articles', db,
              index = False, if_exists = 'append')

fetch_article = _create_data_fetcher(_parse_article_soup, _insert_article_data)

def update_articles_database(on_fail = 'abort', sleep_time = 0.1):
    to_do = get_missing_articles()
    return iterate_scraping(fetch_article, to_do, sleep_time = sleep_time, on_fail = on_fail)


################################################################################
#
# Part 7: Exception Raising
#
################################################################################


class UnexpectedDataFormat(Exception):
    pass

class HeaderNotFound(Exception):
    pass


################################################################################
#
# Part 8: Post-process
#
################################################################################


# Attempt to add a primary key table_id if one does not exist
def add_primary_key(table):
    df = query(f'''SELECT * FROM {table} LIMIT 1''')
    existing_cols = df.columns
    if 'table_id' in existing_cols:
        print('primary key already detected')
        return
    db.execute(f'''
            ALTER TABLE
                {table}
            ADD
                table_id INT
                PRIMARY KEY
                AUTO_INCREMENT
            ;
    ''')

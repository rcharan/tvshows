import requests
from bs4 import BeautifulSoup as BS
import time
import pandas as pd
import mysql.connector
import string
import tv_by_the_numbers as utilities

################################################################################
#
# Part 1 : Get a list of TV Shows to search for
#
################################################################################

def fix_name(s):
    allowed_characters = string.ascii_lowercase + ' _' + string.digits
    s = ''.join(filter(lambda c : c in allowed_characters, s.lower()))
    s = s.replace(' ','_')
    s = s.strip()
    return s

def find_one_on_rotten_tomatoes(show):
    rt_name = fix_name(show)
    url     = f'https://www.rottentomatoes.com/tv/{rt_name}'
    r = requests.get(url)
    r.raise_for_status()

    return url, r, rt_name

def find_on_rotten_tomatoes(list_of_shows):
    list_of_rotten_tomatoes_urls = []

    for i, show in enumerate(list_of_shows):
        if i % 10 == 0:
            print(f'Attempting show number {i}')
        try:
                url, _, rt_name = find_one_on_rotten_tomatoes(show)
                out_dict = {'by_the_numbers_name' : show,
                            'rt_name'             : rt_name,
                            'url'                 : url}
                list_of_rotten_tomatoes_urls.append(out_dict)

        except requests.exceptions.HTTPError as e:
            if 'response' in dir(e) and e.response.status_code == 404:
                pass
            else:
                print(e)
                pass
        except Exception as e:
            print('\n------SOMETHING WENT HORRIBLY WRONG-----')
            print(f'show was {show}')
            print(e)
            print('\n')
            time.sleep(1)
            continue

        time.sleep(0.05)

    return list_of_rotten_tomatoes_urls

################################################################################
#
# Part #2 Parse
#
################################################################################

def tv_show_info(soup, **kwargs):
    rt_show_info = {}
    url = kwargs['url']
    rt_show_info['url'] = url

    # Get the TV Show name from RT
    name = soup.find('h1', class_='mop-ratings-wrap__title mop-ratings-wrap__title--top')
    name = name.get_text()
    name = name.strip()
    rt_show_info['title'] = name

    # Get the TV Show Critics Rating from RT
    cri_rating = soup.find('div', class_='mop-ratings-wrap__half critic-score')
    cri_rating = cri_rating.find('span', class_='mop-ratings-wrap__percentage')
    if cri_rating is None:
        rt_show_info['critic_rating'] = None
    else:
        cri_rating = cri_rating.get_text().strip()
        cri_rating = cri_rating.strip('%')
        rt_show_info['critic_rating'] = cri_rating

    # Get the TV Show Audience Rating from RT
    aud_rating = soup.find('div', class_='mop-ratings-wrap__half audience-score')
    aud_rating = aud_rating.find('span', class_='mop-ratings-wrap__percentage')
    if aud_rating is None:
        rt_show_info['audience_rating'] = None
    else:
        aud_rating = aud_rating.get_text().strip()
        aud_rating = aud_rating.strip('%')
        rt_show_info['audience_rating'] = aud_rating

    #TO DO = Get the Creator


    page = soup.find('div', class_='col-right col-full-xs pull-right')
    page = page.find('div', class_='panel-body content_body')

    field_dict = \
    {'TV Network'          : 'network'
    ,'Premiere Date'       : 'premiere_date'
    ,'Genre'               : 'genre'
    ,'Creator'             : 'creator'
    ,'Executive Producer'  : 'executive_producers'
    ,'Executive Producers' : 'executive_producers'}

    def parse_ep(ep_str):
        return str(list(
           filter(None,
            map(lambda s : s.strip(' ,'),
                ep_str.split('\n')
               ))))

    for row in page.find_all('tr'):
        entries = [r.get_text().strip() for r in row('td')]
        key = entries[0].strip(':')
        val = entries[1]
        if key not in field_dict:
            print(f'Unexpected Table Key {key}'
                  f' with value {val}'
                  f' at url {url}')
            raise utilities.UnexpectedDataFormat('Unknown Key')
        field = field_dict[key]
        if field == 'executive_producers':
            val = parse_ep(val)
        if not val:
            val = None
        rt_show_info[field] = val

    return pd.DataFrame.from_records([rt_show_info])

################################################################################
#
# Part 3 Glue everything together
#
################################################################################

def get_missing_rt_data():
    target = utilities.query_list('url', 'rt_urls')
    to_do  = utilities._get_missing_scrape_targets(target,
                                       'url',
                                       'rt_data')
    return to_do

def _insert_rt_data(df, **kwargs):
    df.to_sql('rt_data', utilities.db, index = False, if_exists = 'append')


fetch_rt_data = utilities._create_data_fetcher(tv_show_info, _insert_rt_data)


def update_rt_data(on_fail = 'abort', sleep_time = 0.1):
    to_do = get_missing_rt_data()
    return utilities.iterate_scraping(fetch_rt_data, to_do, sleep_time = sleep_time, on_fail = on_fail)

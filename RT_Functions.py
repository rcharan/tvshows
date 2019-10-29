import requests
from bs4 import BeautifulSoup as BS
import time
import pandas as pd
import mysql.connector
import string
import tvbythenumbers as utilities

################################################################################
#
# Part 1 : Get a list of TV Shows to search for
#
################################################################################

def find_on_rotten_tomatoes(list_of_shows):
    list_of_rotten_tomatoes_urls = []
    def fix_name(s):
        allowed_characters = string.ascii_lowercase + ' _'
        s = ''.join(filter(lambda c : c in allowed_characters, s.lower()))
        s = s.replace(' ','_')
        return s

    for show, i in enumerate(list_of_shows):
        if i % 10 == 0:
            print('Attempting show number {i}')
        try:
            rt_name = fix_name(show)
            r = requests.get(f'https://www.rottentomatoes.com/tv/{rt_name}')
            if r.status_code == 200:
                url = f'https://www.rottentomatoes.com/tv/{rt_name}'
                out_dict = {'by_the_numbers_name' : show,
                            'rt_name'             : rt_name,
                            'url'                 : url}
                list_of_rotten_tomatoes_urls.append(out_dict)
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

################################################################################
#
# Part # One field at a time
#
################################################################################


def get_critic_rating(url):
    critic_rating = {}
    soup = BS((requests.get(url)).content, 'html.parser')
    cri_rating = soup.find('div', class_='mop-ratings-wrap__half critic-score')
    cri_rating = cri_rating.find('span', class_='mop-ratings-wrap__percentage')
    cri_rating = cri_rating.get_text().strip()
    cri_rating = cri_rating.strip('%')
    critic_rating['critic_rating'] = cri_rating
    return critic_rating

# get_critic_rating('https://www.rottentomatoes.com/tv/REAL_TIME_WITH_BILL_MAHER')
# get_critic_rating(list_of_urls)
# for x in list_of_urls:
    # print(x)

# get_critic_rating('https://www.rottentomatoes.com/tv/prodigal_son/s01')

def get_audience_rating(url):
    soup = BS((requests.get(url)).content, 'html.parser')
    aud_rating = soup.find('div', class_='mop-ratings-wrap__half audience-score')
    aud_rating = aud_rating.find('span', class_='mop-ratings-wrap__percentage')
    aud_rating = aud_rating.get_text().strip()
    aud_rating = aud_rating.strip('%')
    rt_show_info['audience_rating'] = aud_rating


# get_audience_rating('https://www.rottentomatoes.com/tv/prodigal_son/s01')

def get_genre(url):
    list_of_info = []
    soup = BS((requests.get(url)).content, 'html.parser')
    list_of_info = []
    genre = soup.find('div', class_='col-right col-full-xs pull-right')
    genre = genre.find('div', class_='panel-body content_body')
    for row in genre.find_all('tr'):
        for cell in row("td"):
            info = cell.get_text()
            all_info = (info.split())
            list_of_info.append(all_info)
    return list_of_info[5]


# get_genre('https://www.rottentomatoes.com/tv/prodigal_son')

def get_network(url):
    list_of_info = []
    soup = BS((requests.get(url)).content, 'html.parser')
    page = soup.find('div', class_='col-right col-full-xs pull-right')
    page = page.find('div', class_='panel-body content_body')
    for row in page.find_all('tr'):
        for cell in row("td"):
            info = cell.get_text()
            all_info = (info.split())
            list_of_info.append(all_info)
    return (list_of_info[1])

# get_network('https://www.rottentomatoes.com/tv/prodigal_son')


def get_premiere_date(url):
    list_of_info = []
    soup = BS((requests.get(url)).content, 'html.parser')
    page = soup.find('div', class_='col-right col-full-xs pull-right')
    page = page.find('div', class_='panel-body content_body')
    for row in page.find_all('tr'):
        for cell in row('td'):
            info = cell.get_text()
            all_info = (info.split())
            list_of_info.append(all_info)
    return (list_of_info[3])

# get_premiere_date('https://www.rottentomatoes.com/tv/prodigal_son')


def get_producers(url):
    list_of_info = []
    soup = BS((requests.get(url)).content, 'html.parser')
    page = soup.find('div', class_='col-right col-full-xs pull-right')
    page = page.find('div', class_='panel-body content_body')
    for row in page.find_all('tr'):
        for cell in row("td"):
            info = cell.get_text()
            all_info = (info.split())
            list_of_info.append(all_info)
    return (list_of_info[-1])

# get_producers('https://www.rottentomatoes.com/tv/prodigal_son')

def get_producers_season(url):
    list_of_info = [] #list of data from site
    soup = BS((requests.get(url)).content, 'html.parser')
    page = soup.find('section', class_='panel panel-rt panel-box movie_info') #first section
    page = page.find('ul', class_='content-meta info') #second section
    for row in page.find_all('div'):
        info = row.get_text().split()
        list_of_info.append(info)
    clean_list = (list_of_info[-1]) #getting specific info - producers
    cleaner_list = ''.join(clean_list) #getting rid of commas
    return cleaner_list

# get_producers_season('https://www.rottentomatoes.com/tv/castle_rock/s02')

def get_genre_season(url):
    list_of_info = []
    soup = BS((requests.get(url)).content, 'html.parser')
    page = soup.find('section', class_='panel panel-rt panel-box movie_info')
    page = page.find('ul', class_='content-meta info')
    for row in page.find_all('div'):
        info = row.get_text().split()
        list_of_info.append(info)
    clean_list = (list_of_info[1])
    cleaner_list = ''.join(clean_list)
    return cleaner_list

# get_genre_season('https://www.rottentomatoes.com/tv/castle_rock/s02')

def get_network_season(url):
    list_of_info = []
    soup = BS((requests.get(url)).content, 'html.parser')
    page = soup.find('section', class_='panel panel-rt panel-box movie_info')
    page = page.find('ul', class_='content-meta info')
    for row in page.find_all('div'):
        info = row.get_text().split()
        list_of_info.append(info)
    clean_list = (list_of_info[3])
    cleaner_list = ''.join(clean_list)
    return cleaner_list

# get_network_season('https://www.rottentomatoes.com/tv/castle_rock/s02')

def get_premiere_date_season(url):
    list_of_info = []
    soup = BS((requests.get(url)).content, 'html.parser')
    page = soup.find('section', class_='panel panel-rt panel-box movie_info')
    page = page.find('ul', class_='content-meta info')
    for row in page.find_all('div'):
        info = row.get_text().split()
        list_of_info.append(info)
    clean_list = (list_of_info[5])
    cleaner_list = ''.join(clean_list)
    return cleaner_list

# get_premiere_date_season('https://www.rottentomatoes.com/tv/castle_rock/s02')

def get_name_of_show_season(url):
    soup = BS((requests.get(url)).content, 'html.parser')
    page = soup.find('h1', class_='movie_title')
    page = page.get_text()
    page = page.split('\n')
    return (page[1])

# get_name_of_show_season('https://www.rottentomatoes.com/tv/mayans_m_c_/s02')

def get_name_of_show(url):
    soup = BS((requests.get(url)).content, 'html.parser')
    page = soup.find('h1', class_='mop-ratings-wrap__title mop-ratings-wrap__title--top')
    page = page.get_text()
    page = page.split('\n')
    return (page[1])

# get_name_of_show('https://www.rottentomatoes.com/tv/FORGED_IN_FIRE')

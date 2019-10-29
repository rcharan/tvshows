import requests
from bs4 import BeautifulSoup as BS
import time
import mysql.connector 
db_name = 'tvshows'
cnx = mysql.connector.connect(
    host = "mon-dragon.cwxehvwsg5bg.us-east-1.rds.amazonaws.com",
    user = "findlay",
    password = "tvshow-*",
    database = db_name
)
cursor = cnx.cursor() 

cursor.execute('''SELECT s.show FROM tv_by_the_numbers_articles s LIMIT 100''')
list_of_tv_shows = cursor.fetchall()

list_of_tv_shows

string_list_of_tv_shows = [''.join(i) for i in list_of_tv_shows] 

string_list_of_tv_shows
  
list_of_tv_shows

find_on_rotten_tomatoes(string_list_of_tv_shows)


def tv_show_info(url):
    rt_show_info = {}
    soup = BS((requests.get(url)).content, 'html.parser')
    name = soup.find('h1', class_='mop-ratings-wrap__title mop-ratings-wrap__title--top')
    name = name.get_text()
    name = name.split('\n')
    rt_show_info['title'] = name[1].strip()
    cri_rating = soup.find('div', class_='mop-ratings-wrap__half critic-score')
    cri_rating = cri_rating.find('span', class_='mop-ratings-wrap__percentage')
    if cri_rating is None:
        rt_show_info['critic_rating'] == 'none'
    else:
        cri_rating = cri_rating.get_text().strip()
        cri_rating = cri_rating.strip('%')
        rt_show_info['critic_rating'] = cri_rating
    aud_rating = soup.find('div', class_='mop-ratings-wrap__half audience-score')
    aud_rating = aud_rating.find('span', class_='mop-ratings-wrap__percentage')
    if aud_rating is None:
        rt_show_info['audience_rating'] == 'none'
    else:
        aud_rating = aud_rating.get_text().strip()
        aud_rating = aud_rating.strip('%')
        rt_show_info['audience_rating'] = aud_rating
    list_of_info = []
    page = soup.find('div', class_='col-right col-full-xs pull-right')
    page = page.find('div', class_='panel-body content_body')
    for row in page.find_all('tr'):
        for cell in row("td"):
            info = cell.get_text()
            all_info = (info.split())
            list_of_info.append(all_info)
    rt_show_info['genre'] = list_of_info[5]
    rt_show_info['network'] = list_of_info[1]
    rt_show_info['premiere_date'] = list_of_info[3]
    rt_show_info['producers'] = list_of_info[-1]
    return rt_show_info


tv_show_info('https://www.rottentomatoes.com/tv/REAL_TIME_WITH_BILL_MAHER')



list_of_shows = ['Modern Family', 'Arrow', 'The Flash', 'MAYANS M.C']

def find_on_rotten_tomatoes(list_of_shows):
    list_of_rotten_tomatoes_urls =[]
    did_not_work = []
    for show in list_of_shows:
        show = show.replace(' ','_')
        show = show.replace('.','')
        r = requests.get('https://www.rottentomatoes.com/tv/%s' % show)
        r.status_code
        if r.status_code == 200:
            url = ('https://www.rottentomatoes.com/tv/%s' % show)
            print('nice')
            list_of_rotten_tomatoes_urls.append(url)
        else:
            did_not_work.append(show)
            print('did not work')
            print(show)
    return list_of_rotten_tomatoes_urls

list_of_urls = find_on_rotten_tomatoes(list_of_tv_shows)

print(list_of_urls)


def get_critic_rating(url):
    critic_rating = {}
    soup = BS((requests.get(url)).content, 'html.parser')
    cri_rating = soup.find('div', class_='mop-ratings-wrap__half critic-score')
    cri_rating = cri_rating.find('span', class_='mop-ratings-wrap__percentage')
    cri_rating = cri_rating.get_text().strip()
    cri_rating = cri_rating.strip('%')
    critic_rating['critic_rating'] = cri_rating
    return critic_rating

get_critic_rating('https://www.rottentomatoes.com/tv/REAL_TIME_WITH_BILL_MAHER')

get_critic_rating(list_of_urls)

for x in list_of_urls:
    print(x)

get_critic_rating('https://www.rottentomatoes.com/tv/prodigal_son/s01')

def get_audience_rating(url):
    soup = BS((requests.get(url)).content, 'html.parser')
    aud_rating = soup.find('div', class_='mop-ratings-wrap__half audience-score')
    aud_rating = aud_rating.find('span', class_='mop-ratings-wrap__percentage')
    aud_rating = aud_rating.get_text().strip()
    aud_rating = aud_rating.strip('%')
    rt_show_info['audience_rating'] = aud_rating


get_audience_rating('https://www.rottentomatoes.com/tv/prodigal_son/s01')

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
         

get_genre('https://www.rottentomatoes.com/tv/prodigal_son')

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

get_network('https://www.rottentomatoes.com/tv/prodigal_son')


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
         
get_premiere_date('https://www.rottentomatoes.com/tv/prodigal_son')


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

get_producers('https://www.rottentomatoes.com/tv/prodigal_son')

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

get_producers_season('https://www.rottentomatoes.com/tv/castle_rock/s02')

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

get_genre_season('https://www.rottentomatoes.com/tv/castle_rock/s02')

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

get_network_season('https://www.rottentomatoes.com/tv/castle_rock/s02')

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

get_premiere_date_season('https://www.rottentomatoes.com/tv/castle_rock/s02')

def get_name_of_show_season(url):
    soup = BS((requests.get(url)).content, 'html.parser')
    page = soup.find('h1', class_='movie_title')
    page = page.get_text()
    page = page.split('\n')
    return (page[1])

get_name_of_show_season('https://www.rottentomatoes.com/tv/mayans_m_c_/s02')

def get_name_of_show(url):
    soup = BS((requests.get(url)).content, 'html.parser')
    page = soup.find('h1', class_='mop-ratings-wrap__title mop-ratings-wrap__title--top')
    page = page.get_text()
    page = page.split('\n')
    return (page[1])

get_name_of_show('https://www.rottentomatoes.com/tv/FORGED_IN_FIRE')


# def find_on_rotten_tomatoes(string):
#     string = string.replace(' ','_')
#     r = requests.get('https://www.rottentomatoes.com/tv/%s' % string)
#     r.status_code
#     if r.status_code == 200:
#         url = ('https://www.rottentomatoes.com/tv/%s' % string)
#         print('nice')
#         return url
#     else:
#         pass

find_on_rotten_tomatoes('FORGED IN FIRE')


list_of_shows = ['Modern Family', 'Arrow', 'The Flash', 'MAYANS M.C']

def find_on_rotten_tomatoes(list_of_shows):
    list_of_rotten_tomatoes_urls =[]
    did_not_work = []
    for show in list_of_shows:
        show = show.replace(' ','_')
        show = show.replace('.','')
        r = requests.get('https://www.rottentomatoes.com/tv/%s' % show)
        r.status_code
        if r.status_code == 200:
            url = ('https://www.rottentomatoes.com/tv/%s' % show)
            print('nice')
            list_of_rotten_tomatoes_urls.append(url)
        else:
            did_not_work.append(show)
            print('did not work')
    return list_of_rotten_tomatoes_urls

list_of_urls = find_on_rotten_tomatoes(list_of_shows)

print(list_of_urls)
import tv_by_the_numbers as _utilities
import re
import rotten_tomatoes as rt
import requests
import pandas as pd

db = _utilities.db

def list_subtract(l1, l2):
    return list(set(l1).difference(set(l2)))

def get_missing_tv_shows():
    all_tv_shows = _utilities.query_list('s.show', 'tv_by_the_numbers_articles s')
    rt_tv_shows  = _utilities.query_list('by_the_numbers_name', 'rt_urls')
    missing = list_subtract(all_tv_shows, rt_tv_shows)
    print(f'{len(missing)} missing shows')
    return missing

def assess_theories(re_dict, missing_shows, verbose = True, dont_dump_list = [], dump_none = True):
    assessement = {k : [] for k in re_dict.keys()}
    assessement['no match'] = []
    for show in missing_shows:
        match_count = 0
        for name, pattern in re_dict.items():
            if (name.lower()[:6] == 'parser' and match_count > 0):
                continue
            if re.search(pattern.lower(), show.lower()):
                match_count += 1
                assessement[name].append(show)
        if match_count == 0:
            assessement['no match'].append(show)
        if match_count > 1:
            print(f'Multiple ({match_count}) matches for show: {show}')

    print('\n\n---Done. Match counts below---')
    total_matches = 0
    for k in assessement:
        if k != 'no match':
            total_matches += len(assessement[k])
        print(f'{k} : {len(assessement[k])}')

    print(f'\nTotal matches found : {total_matches}')

    if verbose:
        to_dump = list_subtract(re_dict.keys(), dont_dump_list)
        if dump_none:
            to_dump.append('no match')
        if not to_dump:
            return assessement
        print('\n\nDumping matches\n')
        for k in to_dump:
            print(f'----------DUMPING {k}----------')
            for v in assessement[k]:
                print(v)
            print('\n\n')

    return assessement

def find_urls_with_parser(shows, parser):
    good = []
    for show in shows:
        parsings = parser(show)
        for parsing in parsings:
            try:
                url, _ = rt.find_one_on_rotten_tomatoes(parsing)
                if url:
                    print(f'Found a show! {show} can be found by calling it {parsing}')
                    good.append({'by_the_numbers_name' : show, 'rt_name' : rt.fix_name(parsing), 'url' : url})
            except requests.exceptions.HTTPError as e:
                if 'response' in dir(e) and e.response.status_code == 404:
                    pass
                else:
                    print(e)
                    pass


    return good

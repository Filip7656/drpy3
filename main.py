# coding=utf-8
import xml.etree.ElementTree as ET
import pandas as pd
import os
import numpy as np
import pandasql as pdsql

# Config
file_extension = '.xml'
data_path = os.getcwd() + '/data'
origin_site = '.stackexchange.com/'
data_pack_names = ['Badges', 'Comments', 'PostHistory', 'PostLinks', 'Posts', 'Tags', 'Users', 'Votes']
data_pack_websites = ['devops', 'law', 'gamedev']
data_dict = {}
clean_data_pack = {}

# End config


def xml2df(xml_data):
    root = ET.XML(xml_data)  # element tree
    all_records = []
    for i, child in enumerate(root):
        record = {}
        for subchild in child.attrib:
            record[subchild] = child.attrib[subchild]
        all_records.append(record)
    return pd.DataFrame(all_records)


def open_file(file_name, website):
    path = data_path + '/' + website + origin_site + file_name
    return open(path, 'r').read()


def load_sites_data():
    for site_name in data_pack_websites:
        data = []
        for data_pack in data_pack_names:
            data.append(xml2df(open_file(data_pack + file_extension, site_name)))
        data_dict[site_name] = data


def clean_data(website):
    packs = []
    for data in data_dict[website]:
        website = data.fillna(value=np.nan)
        for column in website.columns:
            if 'date' in column.lower():
                # converting to datetime column
                data[column] = pd.to_datetime(data[column])
                data[column] = data[column].map(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else np.nan)
        packs.append(data)
    return packs


def run_sql_in_context_of(site, sql_query):
    badges = clean_data_pack[site][0]
    comments = clean_data_pack[site][1]
    post_history = clean_data_pack[site][2]
    post_links = clean_data_pack[site][3]
    posts = clean_data_pack[site][4]
    tags = clean_data_pack[site][5]
    users = clean_data_pack[site][6]
    votes = clean_data_pack[site][7]
    print(pdsql.sqldf(sql_query, locals()))


def base_stats_for_website(site):
    print('test')
    # print(pdsql.sqldf(top_answeres, locals()))
    # print(pdsql.sqldf(top_bounty_hunters, locals()))
    # print(pdsql.sqldf(questions_by_day, locals()))



def init_data():
    load_sites_data()
    for website in data_pack_websites:
        clean_data_pack[website] = clean_data(website)


if __name__ == '__main__':
    init_data()



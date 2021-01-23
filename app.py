# coding=utf-8
import xml.etree.ElementTree as ET
from flask import Flask, render_template, jsonify, request, redirect
import pandas as pd
import os
import numpy as np
import pandasql as pdsql

# Config
file_extension = '.xml'
data_path = os.getcwd() + '/data'
origin_site = '.stackexchange.com/'
data_pack_names = ['Badges', 'Comments', 'PostHistory', 'PostLinks', 'Posts', 'Tags', 'Users', 'Votes']
data_pack_websites = ['computergraphics', 'drones', 'cstheory']
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
                data[column] = data[column].map(lambda x: x.strftime('%m/%Y') if pd.notnull(x) else np.nan)
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
    return pdsql.sqldf(sql_query, locals())


def base_stats_for_website(site):
    print('test')
    # print(pdsql.sqldf(top_answeres, locals()))
    # print(pdsql.sqldf(top_bounty_hunters, locals()))
    # print(pdsql.sqldf(questions_by_day, locals()))



def init_data():
    load_sites_data()
    for website in data_pack_websites:
        clean_data_pack[website] = clean_data(website)

# define web app
app = Flask(__name__)

# home
@app.route('/')
def home():
    a = clean_data_pack['drones']
    return jsonify(a)

@app.route('/question_count/<df_name>')
def chart1(df_name):
    if df_name in data_pack_websites:
        sql = '''
        SELECT date1, count(id) as cnt
        FROM (
        SELECT 
        d.CreationDate as date1,
            (d.Id) as id
        FROM posts d  -- d=duplicate
            LEFT JOIN post_history ph ON ph.PostId = d.Id
            LEFT JOIN post_links pl ON pl.PostId = d.Id
            LEFT JOIN posts o ON o.Id = pl.RelatedPostId  -- o=original
        WHERE
            d.PostTypeId = 1  -- 1=Question
        ) as t1
        group by date1
        order by date1
        '''
        result = run_sql_in_context_of(df_name, sql)
        questions = {}
        for index, row in result.iterrows():
            questions[f'{row["date1"]}'] = row['cnt'] 
    elif df_name == 'all':
        questions = []
        questions_temp = {}
        for website in data_pack_websites:
            sql = '''
            SELECT date1, count(id) as cnt
            FROM (
            SELECT 
            d.CreationDate as date1,
                (d.Id) as id
            FROM posts d  -- d=duplicate
                LEFT JOIN post_history ph ON ph.PostId = d.Id
                LEFT JOIN post_links pl ON pl.PostId = d.Id
                LEFT JOIN posts o ON o.Id = pl.RelatedPostId  -- o=original
            WHERE
                d.PostTypeId = 1  -- 1=Question
            ) as t1
            group by date1
            order by date1
            '''
            result = run_sql_in_context_of(website, sql)
            for index, row in result.iterrows():
                questions_temp[f'{row["date1"]}'] = row['cnt'] 
            questions.append(questions_temp)
            questions_temp = {}
    else:
        return "not found", 404
    return render_template('question_count.html', questions=questions, df_name=df_name, websites=data_pack_websites)


@app.route('/duplicates_count/<df_name>')
def duplicates_count(df_name):
    if df_name in data_pack_websites:
        sql = '''
        SELECT date1, count(id) as cnt
        FROM (
        SELECT 
        d.CreationDate as date1, 
        (d.Id) as id
        FROM posts d  -- d=duplicate
            LEFT JOIN post_history ph ON ph.PostId = d.Id
            LEFT JOIN post_links pl ON pl.PostId = d.Id
            LEFT JOIN posts o ON o.Id = pl.RelatedPostId 
        WHERE
            d.PostTypeId = 1
            AND pl.LinkTypeId = 3 
        ) as t1
        group by date1
        order by date1
        '''
        result = run_sql_in_context_of(df_name, sql)
        questions = {}
        for index, row in result.iterrows():
            questions[f'{row["date1"]}'] = row['cnt'] 
    elif df_name == 'all':
        questions = []
        questions_temp = {}
        for website in data_pack_websites:
            sql = '''
            SELECT date1, count(id) as cnt
            FROM (
            SELECT 
            d.CreationDate as date1, 
            (d.Id) as id
            FROM posts d  -- d=duplicate
                LEFT JOIN post_history ph ON ph.PostId = d.Id
                LEFT JOIN post_links pl ON pl.PostId = d.Id
                LEFT JOIN posts o ON o.Id = pl.RelatedPostId 
            WHERE
                d.PostTypeId = 1
                AND pl.LinkTypeId = 3  
            ) as t1
            group by date1
            order by date1
            '''
            result = run_sql_in_context_of(website, sql)
            for index, row in result.iterrows():
                questions_temp[f'{row["date1"]}'] = row['cnt'] 
            questions.append(questions_temp)
            questions_temp = {}
    else:
        return "not found", 404
    return render_template('duplicates_count.html', questions=questions, df_name=df_name, websites=data_pack_websites)

@app.route('/top_post/<df_name>/<int:number>')
def top_post(df_name, number):
    try:
        sql = f'''
        SELECT  Score, Title
        FROM posts 
        WHERE Title IS NOT NULL
        ORDER BY Score DESC limit {number}
        '''
        result = run_sql_in_context_of(df_name, sql)
        print(result)

        return render_template('top_post.html', df_name=df_name, number=number, posts=result)
    except:
        return "not found", 404

@app.route('/post_types')
def post_types():
    sql = '''
        select count(*) as cnt, p.PostTypeId as id from posts p group by p.PostTypeId
    '''
    type_dict = {
        '1': 'Question',
        '2': 'Answer',
        '3': 'Orphaned tag wiki',
        '4': 'Tag wiki excerpt',
        '5': 'Tag wiki',
        '6': 'Moderator nomination',
        '7': 'Wiki placeholder',
        '8': 'Privilege wiki'
    }
    types_count = []
    for website in data_pack_websites:
        result = run_sql_in_context_of(website, sql)
        temp = {}
        for index, row in result.iterrows():
            temp[type_dict[row['id']]] = row['cnt']
        types_count.append(temp)
    return render_template('post_types.html', types_count=types_count, websites=data_pack_websites)

if __name__ == '__main__':
    init_data()
    app.run(debug=True)

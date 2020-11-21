import requests
import sqlite3
import datetime
import re
import unicodedata
import concurrent.futures
from lxml import html

def db_connect(db_path='articles.db'):
    con = sqlite3.connect(db_path)
    return con

def db_insert(cursor, article):
    try:
        query = f"INSERT INTO articles VALUES ({article['id']}, '{article['title']}', '{article['arthor']}', '{article['datetime']}', {article['timestamp']}, '{article['content']}')"
        cursor.execute(query)
    except sqlite3.IntegrityError:
        error = f"{article['id']} already exists in database."
        print(error)
        errors.append(error)

def preprocess(article):
    if article["datetime"][0:1] == "\n":
        article["datetime"] = article["datetime"][1:].strip()
    article["timestamp"] = datetime_to_timestamp(article["datetime"])
    if re.match('^[0-9\.\ :]*$', article["content"][0]):
        article["content"] = article["content"][1:]
    article["content"][-1] = unicodedata.normalize("NFKD", article["content"][-1])
    article["content"] = "".join(article["content"])
    article["content"] = article["content"].replace("'", "''")
    
def datetime_to_timestamp(dt):
    date = dt.split(',')[0]
    time = dt.split(',')[1]
    year = int(date.split(' ')[2])
    month_conv = {"Jan":"1", "Feb":"2", "Mar":"3", "Apr":"4", "May":"5", "Jun":"6", "Jul":"7", "Aug":"8", "Sep":"9", "Oct":"10", "Nov":"11", "Dec":"12"}
    month = int(month_conv[date.split(' ')[1]])
    day = int(date.split(' ')[0])
    hour = int(time.split(':')[0])
    minute = int(time.split(':')[1])
    return int((datetime.datetime(year, month, day, hour, minute) - datetime.datetime(1970,1,1)).total_seconds())

def get_article(pageID):
    url = f'https://sun.mv/{pageID}'
    resp = requests.get(url)
    if resp.ok:
        if resp.text == "404 page":
            error = f"{pageID} gives 404 (Page not found) error."
            print(error)
            errors.append(error)
            return
        tree = html.fromstring(resp.content)
        article = {
            "id": pageID,
            "content": tree.xpath('/html/body/main/div[2]/div[2]/div[2]/div[2]/div[1]/div[2]/p/text()')
        }
        if not article["content"]:
            article["content"] = tree.xpath('/html/body/main/div[2]/div[2]/div[2]/div[2]/div[1]/div[2]/text()')
            if not article["content"]:
                error = f"Cannot parse content in {pageID}. Skipping article."
                print(error)
                errors.append(error)
                return
        try:
            article["title"] = tree.xpath('/html/body/main/div[2]/div[2]/div[2]/div[1]/div/div[1]/h1/text()')[0]
        except IndexError:
            article["title"] = ""
        try:
            article["arthor"] = tree.xpath('/html/body/main/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[1]/div[2]/span[1]/text()')[0]
        except IndexError:
            article["arthor"] = ""
        try: 
            article["datetime"] = tree.xpath('/html/body/main/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[1]/div[2]/span[2]/text()')[0]
        except IndexError:
            article["datetime"] = tree.xpath('/html/body/main/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[1]/div/span/text()')[0]
        try:
            preprocess(article)
        except IndexError:
            error = f"Cannot parse content in {pageID}. Skipping article."
            print(error)
            errors.append(error)
        return article
    else:
        error = f"{pageID} gives {resp.status_code} error."
        print(error)
        errors.append(error)

def write_article(i):
    con = db_connect()
    cursor = con.cursor()
    print(f"Fetching article sun.mv/{i}")
    article = get_article(i)
    if article:
        db_insert(cursor, article)
        con.commit()
    con.close()

errors = []
with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
    futures = {executor.submit(write_article, i): i for i in range(1, 145846)} #145845 is the latest article as of 2020/11/21
    for future in concurrent.futures.as_completed(futures):
        try:
            data = future.result()
        except Exception as e:
            print(e)

with open(f'error_log_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.txt', 'w') as f:
    for i in errors:
        f.write(f'{i}\n')
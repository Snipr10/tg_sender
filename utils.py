import logging
import time
from enum import Enum

import requests


from datetime import datetime, timedelta

from settings import login, password, auth_url, thread_id, referenceFilter, post_url, UTC, FROM_MINUTES, POSTS_LIMIT
import glob
import os

IDS_PATH = "ids"

class Networks(Enum):
    ok = "Одноклассники"
    dz = "Дзен"
    tt = "TikTok"
    yt = "YouTube"
    ig = "Instagram"
    tg = "Telegram"
    gs = "GlobalSearch"
    fb = "Facebook"
    tw = "Twitter"
    vk = "ВКонтакте"

network = {e.name:e.value for e in Networks}


def login_func(session=requests.session()):
    response = session.post(auth_url, json={
        "login": login,
        "password": password
    })
    if not response.ok:
        logging.error("Unable to login")
    return session


def get_posts(session, from_, to_):
    logging.info(f"from {from_}")
    print(f"from {from_}")

    response = session.post(post_url, json={
        "thread_id": thread_id,
        "from": from_.strftime("%Y-%m-%d %H:%M:%S"),
        "to": to_.strftime("%Y-%m-%d %H:%M:%S"),
        "limit": POSTS_LIMIT,

        "filter": {

            "referenceFilter": referenceFilter,
            "repostoption": "whatever"
        }
    })
    try:
        return response.json()
    except Exception as e:
        logging.error(f"Unable to get posts {e}")
    raise Exception(f"Unable to get posts")


def update_time_zone(date):
    return date + timedelta(hours=UTC)


def get_exists_ids():
    files = os.listdir(IDS_PATH)
    paths = [os.path.join(IDS_PATH, basename) for basename in files]
    last_file = max(paths, key=os.path.getctime)
    with open(last_file, "r") as f:
        lines = f.readlines()
    return [l.strip() for l in lines]


def add_new_ids(ids):
    with open(f"{IDS_PATH}/ids_{time.time()}.txt", "w") as f:
        for id_ in ids:
            f.write(f"{id_}\n")


def get_results():
    to_ = update_time_zone(datetime.now())
    posts = get_posts(session=login_func(), from_=to_ - timedelta(minutes=FROM_MINUTES), to_=to_)
    res_dict = {}
    for p in posts.get("posts"):
        res_dict[f"{p['network_name']}_{p['id']}"] = p
    ids = get_exists_ids()
    add_new_ids(list(res_dict.keys()))
    return [v for k, v in res_dict.items() if k not in ids]


def get_result_messages():
    posts = get_results()
    messages = []
    i = 0
    for p in posts:
        i += 1
        text = f"<b>Источник</b>: {network.get(p.get('network_name'))} \n"
        text += f"<b>Автор</b>: {p.get('author') or ' '} \n"
        text += f"<b>Дата создания поста</b>: {p.get('created_date') or ' '} \n"
        text += f"<b>Лайки</b>: {p.get('likes') or '0'} \n"
        text += f"<b>Репосты</b>: {p.get('reposts') or '0'} \n"
        text += f"<b>Комментарии</b>: {p.get('comments') or '0'} \n"
        text += f"<b>Содержание</b>: {p.get('text') or ' '} \n"
        text += f"<b>Ссылка</b>: {p.get('uri') or ' '} \n"
        messages.append(text)
    return messages

import time
from concurrent.futures import ThreadPoolExecutor
import logging

import schedule
from datetime import datetime

from telebot.apihelper import ApiTelegramException

from settings import bot_data
import telebot

from utils import get_results, get_result_messages

bot = telebot.TeleBot(bot_data)


@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.reply_to(message, f'Бот рассылки из DB')


@bot.message_handler(commands=['posts'])
def send_posts(message):
    send_messages()


def send_messages():
    logging.info("start")
    for m in get_result_messages():
        send_message(m)


def send_message(m, attempt=0):
    try:
        bot.send_message('-1001963743004', m, parse_mode='HTML')
    except ApiTelegramException as e:
        try:
            time.sleep(e.result_json['parameters']['retry_after'])
            send_message(m, attempt + 1)
        except Exception as e:
            logging.error(f"unable to send messages {e}")


schedule.every(4).minutes.do(send_messages)


def start_bot():
    while True:
        try:
            logging.info(f"start_bot {datetime.now()}")
            bot.polling(none_stop=True)
        except Exception as e:
            print(e)


def start_sending_message():
    while True:
        schedule.run_pending()
        time.sleep(60)


pool_source = ThreadPoolExecutor(3)
pool_source.submit(start_sending_message)
pool_source.submit(start_bot)
pool_source.shutdown()

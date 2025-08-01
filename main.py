import aiohttp
import asyncio
import aiofiles
import json
import html
import re
import pytz
import yaml

from datetime import datetime
from bs4 import BeautifulSoup

async def main():
    payload = []
    offset = 0
    done = False  

    async with aiofiles.open('config.yaml', 'r') as f:
        config = yaml.safe_load(await f.read())
    
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15"}) as session:
        while not done:
            print(f"Начинаю парсинг страницы: {offset}")
            async with session.get(f"{config['setting']['url']}?offset={offset}&day={(config['setting']['parse_from']).replace('.', '')}&own=1") as rq:
                soup = BeautifulSoup(await rq.text(), 'html.parser')
                time_string = soup.find_all("div", {'class': "PostContentContainer__root PostContentContainer"})
                for i in time_string:
                    json_data = json.loads(i['data-exec'])
                    post_url = f'https://vk.com/wall{json_data["PostContentContainer/init"]["item"]["from_id"]}_{json_data["PostContentContainer/init"]["item"]["id"]}'
                    try:
                        if any(p["post_url"] == post_url for p in payload)or int(json_data["PostContentContainer/init"]["profiles"][-1]["id"]) in config["setting"]["filter_after"]:
                            continue
                    except:
                        pass

                    if int(json_data["PostContentContainer/init"]["item"]["date"]) > datetime.strptime(config["setting"]["parse_until"], "%d.%m.%Y").replace(tzinfo=pytz.timezone('Europe/Moscow')).timestamp():
                        cleaned_text = re.sub(r'<br\s*/?>', '\n', html.unescape(json_data["PostContentContainer/init"]["item"]["text"]))
                        time_post = datetime.fromtimestamp(int(json_data["PostContentContainer/init"]["item"]["date"]), tz=pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d')
                        try:
                            photo_urls = [
                                photo["photo"]["sizes"][-1]["url"]
                                for photo in json_data["PostContentContainer/init"]["item"]["attachments"]
                            ]
                        except:
                            photo_urls = None
                        payload.append({"post_url": post_url,"time": time_post, "text": cleaned_text, "photo_urls": photo_urls})
                    else:
                        async with aiofiles.open('posts.json', 'w', encoding='utf-8') as f:
                            await f.write(json.dumps(payload, ensure_ascii=False, indent=4))
                            return print(f"Записал в файл {len(payload)} постов")
                        
                print(f"Закончил парсинг страницы: {offset}")
                offset += 10

asyncio.run(main())
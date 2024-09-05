import time
import os
import aiofiles
import pandas as pd
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, BrowserContext
import execjs
import importlib.resources as pkg_resources
import yaml

def validate_or_default(value, valid_options, default):
    return value if value in valid_options else default

def load_config(config_path='config/config.yaml'):
    try:
        with open(config_path, 'r', encoding='utf-8-sig') as file:
            config = yaml.safe_load(file)
        return config if config else {}
    except FileNotFoundError:
        print(f"配置文件 {config_path} 未找到，使用默认配置。")
        return {}
    except yaml.YAMLError as e:
        print(f"配置文件解析错误：{e}，使用默认配置。")
        return {}

def check_path(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)

async def save_to_csv(data_accumulator: List, file_path: str, columns: List) -> None:
    try:
        df = pd.DataFrame(data_accumulator, columns=columns)
        file_exists = os.path.exists(file_path)
        csv_content = df.to_csv(index=False, header=not file_exists, encoding='utf-8-sig')
        async with aiofiles.open(file_path, mode='a', encoding='utf-8-sig') as f:
            await f.write(csv_content)
    except Exception as e:
        print(f"保存数据到 CSV 文件 {file_path} 时出错: {e}")

def get_time(ctime: int) -> str:
    return time.strftime("%Y.%m.%d", time.localtime(ctime))

async def get_data(context: BrowserContext, url: str, headers: Dict, params: Dict=None, timeout: int=None, type: str='json')-> Dict[str, any]:
    try:
        response = await context.request.get(url, headers=headers, params=params, timeout=timeout)
        if not response:
            print(f"获取数据时 response 为 None 或无效")
            return {}
        if type == 'json':
            try:
                return await response.json()
            except Exception as e:
                print(f"解析 JSON 数据时出错: {e}")
                return {}
        elif type == 'src':
            return response
    except Exception as e:
        print(f"获取数据时出错: {e}")
        return {}

async def post_data(context: BrowserContext, url: str, headers: Dict, data: Dict=None)-> Dict:
    response = await context.request.post(url, headers=headers, data=data)
    return await response.json()

def get_x_bogus(query: str) -> Optional[str]:
    with pkg_resources.open_text('lib', 'X-Bogus.js') as f:
        js_code = f.read()
    x_bogus = execjs.compile(js_code).call('sign', query, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
    return x_bogus

def splice_url(params):
    splice_url_str = ''
    for key, value in params.items():
        if value is None:
            value = ''
        splice_url_str += key + '=' + str(value) + '&'
    return splice_url_str[:-1]

def get_headers(cookie: str, platform: str):
    if platform == 'dy':
        return {
            "authority": "www.douyin.com",
            'accept': "application/json, text/plain, */*",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"117\", \"Not;A=Brand\";v=\"8\", \"Chromium\";v=\"117\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "cookie": cookie,
        }
    elif platform == 'ks':
        return {
            'content-type':'application/json',
            'host':'www.kuaishou.com',
            'origin':'https://www.kuaishou.com',
            'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'cookie':cookie,
        }
    elif platform == 'wb_topic':
        return {
            'Connection': 'close',
            'sec-ch-ua-mobile': '?0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Referer': 'https://s.weibo.com/article?q=^%^E5^%^8D^%^8E^%^E4^%^B8^%^BAp50&Refer=weibo_article',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-CN;q=0.8,en;q=0.7,es-MX;q=0.6,es;q=0.5',
            'Cookie': cookie
        }
    elif platform == 'wb_comment':
        return {
            'authority': 'weibo.com',
            'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
            'accept': 'application/json, text/plain, */*',
            'x-xsrf-token': '-OsgXanv8IeYF0Sc1UdtiBB7',
            'x-requested-with': 'XMLHttpRequest',
            'traceparent': '00-e5b530ae9b5a4f289194c266b198bc0b-49c92543eb3e163f-00',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://weibo.com/6882947163/KqO9rrjTm?refer_flag=1001030103_&type=comment',
            'accept-language': 'zh-CN,zh;q=0.9,en-CN;q=0.8,en;q=0.7,es-MX;q=0.6,es;q=0.5',
            'cookie': cookie
        }
    elif platform == 'wb_forward':
        return {
            'authority': 'weibo.com',
            'x-requested-with': 'XMLHttpRequest',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'content-type': 'application/x-www-form-urlencoded',
            'accept': '*/*',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://weibo.com/1192329374/KnnG78Yf3?filter=hot&root_comment_id=0&type=comment',
            'accept-language': 'zh-CN,zh;q=0.9,en-CN;q=0.8,en;q=0.7,es-MX;q=0.6,es;q=0.5',
            'cookie': cookie
        }
    elif platform == 'wb_user':
        return {
            'authority': 'weibo.com',
            'x-requested-with': 'XMLHttpRequest',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'content-type': 'application/x-www-form-urlencoded',
            'accept': '*/*',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://weibo.com/1192329374/KnnG78Yf3?filter=hot&root_comment_id=0&type=comment',
            'accept-language': 'zh-CN,zh;q=0.9,en-CN;q=0.8,en;q=0.7,es-MX;q=0.6,es;q=0.5',
            'cookie': cookie
        }
    else:
        return

def get_profile_params():
    return {
        "device_platform": "webapp",
        "aid": "6383",
        "channel": "channel_pc_web",
        "publish_video_strategy_type": "2",
        "source": "channel_pc_web",
        "sec_user_id": "",
        "pc_client_type": "1",
        "version_code": "170400",
        "version_name": "17.4.0",
        "cookie_enabled": "true",
        "screen_width": "1707",
        "screen_height": "1067",
        "browser_language": "zh-CN",
        "browser_platform": "Win32",
        "browser_name": "Edge",
        "browser_version": "117.0.2045.47",
        "browser_online": "true",
        "engine_name": "Blink",
        "engine_version": "117.0.0.0",
        "os_name": "Windows",
        "os_version": "10",
        "cpu_core_num": "20",
        "device_memory": "8",
        "platform": "PC",
        "downlink": "10",
        "effective_type": "4g",
        "round_trip_time": "50",
        "webid": "",
        "msToken": "",
    }

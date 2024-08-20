import pandas as pd
import os
import json
from typing import Dict
import asyncio
from playwright.async_api import async_playwright, BrowserContext
from lib.utils import get_headers, get_data, check_path

async def parseUid(cookie, uid):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            context = await browser.new_context()
            response = await get_data(context, url=f'https://weibo.com/ajax/profile/info?custom={uid}', headers=get_headers(cookie,'wb_user'))
            try:
                return response['data']['user']['id']
            except:
                return None
        finally:
            await browser.close()

# 判断 uid2 的用户是否为 uid1 的用户的粉丝，如果是则返回True， 如果不是则返回False
async def is_fans(cookie, uid1, uid2):
    try:
        uid1, uid2 = int(uid1), int(uid2)
    except:
        uid1, uid2 = parseUid(uid1), parseUid(uid2)
        if not uid1 or not uid2:
            return None
    friends_id = []
    pageIdx = 1
    while True:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                context = await browser.new_context()
                response = await get_data(context, url=f'https://www.weibo.com/ajax/friendships/friends?page={pageIdx}&uid={uid2}', headers=get_headers(cookie,'wb_user'))
                response = response.get('users', None)
                if not response:
                    break
                for item in response:
                    friends_id.append(item['id'])
                await asyncio.sleep(3)
                pageIdx += 1
            finally:
                await browser.close()
    if uid1 in friends_id:
        return True
    '''
    followers_id = []
    pageIdx = 1
    while True:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                context = await browser.new_context()
                response = await get_data(context, url=f'https://weibo.com/ajax/friendships/friends?relate=fans&page={pageIdx}&uid={uid1}&type=all&newFollowerCount=0', headers=get_headers(cookie,'wb_user'))
                response = response.get('users', None)
                if not response:
                    break
                for item in response:
                    followers_id.append(item['id'])
                pageIdx += 1
            finally:
                await browser.close()
    if uid2 in followers_id:
        print(1)
        return True
    '''
    print(0)
    return False


class WeiboUserCrawler:
    def __init__(self):
        self.headers: Dict
        self.keyword: str
        self.save_path: str
        self.cookie: str
        self.context: BrowserContext

    def set(self, keyword: str, save_path: str, cookie: str):
        self.keyword = keyword
        self.save_path = save_path + f'/weibo/{keyword}'
        self.cookie = cookie
        self.headers = get_headers(self.cookie,'wb_user')

    async def get_user_info(self, uid):
        try:
            uid = int(uid)
        except:
            uid = parseUid(self.cookie, uid)
            if not uid:
                return None
        response = await self.context.request.get(url=f'https://weibo.com/ajax/profile/detail?uid={uid}', headers=self.headers)
        if response.status_code == 400:
            return {
                'errorMsg': '用户可能注销或者封号',
                'location': None,
                'user_link': f'https://weibo.com/{uid}'
            }
        resp_json = response.json().get('data', None)
        if not resp_json:
            return None
        sunshine_credit = resp_json.get('sunshine_credit', None)
        if sunshine_credit:
            sunshine_credit_level = sunshine_credit.get('level', None)
        else:
            sunshine_credit_level = None
        education = resp_json.get('education', None)
        if education:
            school = education.get('school', None)
        else:
            school = None

        label_desc = resp_json.get('label_desc', None)
        if label_desc:
            first = True
            label = ''
            for id in label_desc:
                if not first:
                    label += "  "
                else:
                    first = False
                label += id['name']
        else:
            label = None

        location = resp_json.get('ip_location', None)
        gender = resp_json.get('gender', None)

        birthday = resp_json.get('birthday', None)
        created_at = resp_json.get('created_at', None)
        description = resp_json.get('description', None)

        # 我关注的人中有多少人关注 ta
        followers = resp_json.get('followers', None)
        if followers:
            followers_num = followers.get('total_number', None)
        else:
            followers_num = None

        response = await self.context.request.get(url=f'https://weibo.com/ajax/profile/info?uid={uid}', headers=self.headers)
        if response.status_code == 400:
            return {
                'errorMsg': '用户可能注销或者封号',
                'location': None,
                'user_link': f'https://weibo.com/{uid}'
            }
        resp_json = response.json().get('data', None)
        if not resp_json:
            return None
        user = resp_json.get('user', None)
        if not user:
            return None
        followers_count = user.get('followers_count', None)
        friends_count = user.get('friends_count', None)

        return {
            'sunshine_credit_level': sunshine_credit_level,
            'school': school,
            'location': location,
            'gender': gender,
            'birthday': birthday,
            'created_at': created_at,
            'description': description,
            'followers_num': followers_num,
            'followers_count': followers_count,
            'friends_count': friends_count,
            'label': label
        }

    async def add_user_info(self, file_path=None, user_col='user_link', user_info_col='user_info'):
        '''
        @params file_path 指定添加用户信息文件路径
        @params user_col 指定用户主页链接所在列名，默认是 user_link
        @params user_info_col 指定新加的 userinfo 列名，默认是 user_info
        '''
        if file_path is None:
            print('run after set csv file path')
            return
        else:
            df = pd.read_csv(file_path)
            user_info_init_value = 'init'
            columns = df.columns.values.tolist()
            if not user_info_col in columns:
                df[user_info_col] = [
                    user_info_init_value for _ in range(df.shape[0])]
            for index, row in df.iterrows():
                print(f'   {index+1}/{df.shape[0]}   ')
                if (index+1) % 100 == 0:
                    df.to_csv(file_path, index=False, encoding='utf-8-sig')
                if not row.get(user_info_col, user_info_init_value) == user_info_init_value:
                    print('skip')
                    continue
                user_link = row[user_col]
                if '?' in user_link:
                    user_link = user_link[:user_link.rindex('?')]
                user_id = user_link[user_link.rindex('/')+1:]
                user_info = await self.get_user_info(user_id)
                print(user_info)
                if user_info:
                    # 在 user_info 中统一为 user_link
                    user_info['user_link'] = user_link
                    df.loc[index, user_info_col] = json.dumps(user_info, ensure_ascii=False)
                    await asyncio.sleep(1)
                else:
                    print(user_link)
                    break
                df.to_csv(file_path, index=False, encoding='utf-8-sig')

    async def run(self, file_path = None) -> None:
        if not self.keyword:
            print("run after set")
            return
        check_path(f'{self.save_path}')
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            self.context = await browser.new_context()
            if file_path is None:
                if os.path.exists(f'{self.save_path}/{self.keyword}.csv'):
                    await self.add_user_info(file_path=f'{self.save_path}/{self.keyword}.csv')
                else:
                    print('topic csv file not find')

                if os.path.exists(f'{self.save_path}/comment'):
                        directory = f'{self.save_path}/comment'
                        for filename in os.listdir(directory):
                            if filename.endswith('.csv'):
                                await self.add_user_info(file_path=os.path.join(directory, filename))
                else:
                    print('comment csv file path not find')

                if os.path.exists(f'{self.save_path}/forward'):
                    directory = f'{self.save_path}/forward'
                    for filename in os.listdir(directory):
                        if filename.endswith('.csv'):
                            await self.add_user_info(file_path=os.path.join(directory, filename))
                else:
                    print('comment csv file path not find')
            else:
                await self.add_user_info(file_path=file_path)
            await browser.close()

if __name__ == '__main__':
    keyword = '张本智和发文回应败给樊振东'
    save_path='./data'

    cookie = 'ttwid=1%7CiFrIbo5zacI4ggwR_lLWAeCSUmLqkBuXBX_qe8zBXoU%7C1722161376%7Ccba5a9e8bc0bb928681e50aa867d5b1e80d0ae5c0a129476bfb25031a2d36482; UIFID_TEMP=3c3e9d4a635845249e00419877a3730e2149197a63ddb1d8525033ea2b3354c200afd64d8c1da1ce41b1ee1bbb5f209dc6251ef96e5c67a33ee8bd729f6745cb5de701d949d74ff9264d56ea102b354e8af69aa3db82add4e64dada7f96d037c89e2362e6188ae17dc96c9eb19ac23b5; dy_swidth=1707; dy_sheight=960; fpk1=U2FsdGVkX18w6wg2y3GHFgRlPqr9PVTbe7Ec+iNqMr3KTqQE5cudpn9AmSAf5Gr2b+7+7ZbhU7XiDE1D6/TWaw==; fpk2=f1f6b29a6cc1f79a0fea05b885aa33d0; s_v_web_id=verify_lz5ef986_YtGvWi74_5grO_4C6Y_9uRD_nknO19CQX3E8; xgplayer_user_id=986392702227; passport_csrf_token=019173788f6bd671f34676b4620d19d2; passport_csrf_token_default=019173788f6bd671f34676b4620d19d2; bd_ticket_guard_client_web_domain=2; UIFID=3c3e9d4a635845249e00419877a3730e2149197a63ddb1d8525033ea2b3354c200afd64d8c1da1ce41b1ee1bbb5f209dc6251ef96e5c67a33ee8bd729f6745cb9c4d53ba7684c5d32c87d18c636e9cb41413e1dd3b742ea271cb21a96706b88b1f7bc42f45d10ebd560b036c3ad13331b2da0b3994918adb0761cfbfc9de149b2a0b8ae294f2c4fe944c3aa41928019aa2eb335a7d40698c8e3eddb7627f42e1121a35df8003c96abe71f8040ddff4cbe0b6c5493baaf7618018460655e2c133; live_use_vvc=%22false%22; FORCE_LOGIN=%7B%22videoConsumedRemainSeconds%22%3A180%2C%22isForcePopClose%22%3A1%7D; SEARCH_RESULT_LIST_TYPE=%22single%22; d_ticket=4837a1f785bad649ed04dcc018b52541d1a81; n_mh=9QQeuVUxM4mSMdUxjWaD4Jt2f1UXjlKvsPxugBOFshw; sso_auth_status=72f222ff4ada49b49a8d07fb936f8509; sso_auth_status_ss=72f222ff4ada49b49a8d07fb936f8509; _bd_ticket_crypt_doamin=2; __security_server_data_status=1; store-region=cn-jl; store-region-src=uid; is_staff_user=false; my_rd=2; passport_auth_status=5a552fab2ee073e90c03a5350df21ca5%2Ccba223b3a04022fb0ef128d0609fcf5d; passport_auth_status_ss=5a552fab2ee073e90c03a5350df21ca5%2Ccba223b3a04022fb0ef128d0609fcf5d; strategyABtestKey=%221723266438.532%22; publish_badge_show_info=%220%2C0%2C0%2C1723266443099%22; xgplayer_device_id=92915748857; volume_info=%7B%22isUserMute%22%3Afalse%2C%22isMute%22%3Atrue%2C%22volume%22%3A0.6%7D; stream_player_status_params=%22%7B%5C%22is_auto_play%5C%22%3A0%2C%5C%22is_full_screen%5C%22%3A0%2C%5C%22is_full_webscreen%5C%22%3A0%2C%5C%22is_mute%5C%22%3A1%2C%5C%22is_speed%5C%22%3A1%2C%5C%22is_visible%5C%22%3A1%7D%22; download_guide=%223%2F20240810%2F0%22; csrf_session_id=83e9cec3d90e17cd9efcfd09100bf98a; biz_trace_id=543f0a58; passport_assist_user=CkBKO39GjbcQt2Mf5b0x35LoEcUwfrO44SPO3_QWeomfmqyY82xbeFSfNIUMvyb8ui7cIK5q2vM6lfkaVPlVUV-3GkoKPL_jn8O2VQ6UP1Fv_sormgplo_6K6kylSizvTwPJgj7XXZXa5oXPipOtOQ8ahINEPTyM-4Vsygjhb0xdGxDZ_9gNGImv1lQgASIBA7LhWVY%3D; sso_uid_tt=9238686f1526b23e2f7f8f6452014bde; sso_uid_tt_ss=9238686f1526b23e2f7f8f6452014bde; toutiao_sso_user=fbb34bd50183e9071f4787cfd53b9b2c; toutiao_sso_user_ss=fbb34bd50183e9071f4787cfd53b9b2c; sid_ucp_sso_v1=1.0.0-KGM2OGMxNjYyYWY1OTcxNGE5NzFmNTk5NDY1MTFjNGI0MTkzZmUwYTQKIAjEouCdmfQ5EPWS3LUGGO8xIAww1p3l8AU4AkDvB0gGGgJobCIgZmJiMzRiZDUwMTgzZTkwNzFmNDc4N2NmZDUzYjliMmM; ssid_ucp_sso_v1=1.0.0-KGM2OGMxNjYyYWY1OTcxNGE5NzFmNTk5NDY1MTFjNGI0MTkzZmUwYTQKIAjEouCdmfQ5EPWS3LUGGO8xIAww1p3l8AU4AkDvB0gGGgJobCIgZmJiMzRiZDUwMTgzZTkwNzFmNDc4N2NmZDUzYjliMmM; uid_tt=2b631745fc5d041c67dc0a8ffb0a2934; uid_tt_ss=2b631745fc5d041c67dc0a8ffb0a2934; sid_tt=4f1178dd046fcfdf123d9873c21fff1c; sessionid=4f1178dd046fcfdf123d9873c21fff1c; sessionid_ss=4f1178dd046fcfdf123d9873c21fff1c; _bd_ticket_crypt_cookie=ea0aca8e2fe74d7fbb5cccff4fb8b1d7; sid_guard=4f1178dd046fcfdf123d9873c21fff1c%7C1723271545%7C5183999%7CWed%2C+09-Oct-2024+06%3A32%3A24+GMT; sid_ucp_v1=1.0.0-KGU5OGJiMDcxZmIzNzBkMTBlMzUwNjUxZjE2YTE5OTBlNzkwMWY4ZGYKGgjEouCdmfQ5EPmS3LUGGO8xIAw4AkDvB0gEGgJobCIgNGYxMTc4ZGQwNDZmY2ZkZjEyM2Q5ODczYzIxZmZmMWM; ssid_ucp_v1=1.0.0-KGU5OGJiMDcxZmIzNzBkMTBlMzUwNjUxZjE2YTE5OTBlNzkwMWY4ZGYKGgjEouCdmfQ5EPmS3LUGGO8xIAw4AkDvB0gEGgJobCIgNGYxMTc4ZGQwNDZmY2ZkZjEyM2Q5ODczYzIxZmZmMWM; pwa2=%220%7C0%7C3%7C0%22; __live_version__=%221.1.2.2711%22; webcast_local_quality=null; live_can_add_dy_2_desktop=%220%22; stream_recommend_feed_params=%22%7B%5C%22cookie_enabled%5C%22%3Atrue%2C%5C%22screen_width%5C%22%3A1707%2C%5C%22screen_height%5C%22%3A960%2C%5C%22browser_online%5C%22%3Atrue%2C%5C%22cpu_core_num%5C%22%3A20%2C%5C%22device_memory%5C%22%3A8%2C%5C%22downlink%5C%22%3A10%2C%5C%22effective_type%5C%22%3A%5C%224g%5C%22%2C%5C%22round_trip_time%5C%22%3A50%7D%22; home_can_add_dy_2_desktop=%221%22; bd_ticket_guard_client_data=eyJiZC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwiYmQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJiZC10aWNrZXQtZ3VhcmQtcmVlLXB1YmxpYy1rZXkiOiJCRmZ0VW5yWEE2MDc1dkFUN3BhNXppQjBnajk1MTBIWitHMjZjU08zTHlFdjduZWkwYlYxTEJmWHJHc1lteElQaVFBdGNJcXFLMGhCbFpiL1FrVEZ3RlU9IiwiYmQtdGlja2V0LWd1YXJkLXdlYi12ZXJzaW9uIjoxfQ%3D%3D; odin_tt=a87e86db22068aa9b1feb37220c56114545586bcd6a01f76ac0f7b9597d23549beba8917b6cbf85bd40cc795dbbb6450; WallpaperGuide=%7B%22showTime%22%3A1723266497064%2C%22closeTime%22%3A0%2C%22showCount%22%3A3%2C%22cursor1%22%3A106%2C%22cursor2%22%3A0%2C%22hoverTime%22%3A1722180561085%7D; passport_fe_beating_status=true; IsDouyinActive=false; __ac_nonce=066b74337001142102e7e; __ac_signature=_02B4Z6wo00f014dJTqAAAIDCWzDRFdUXJo-HaUoAAIdtee; __ac_referer=https://www.douyin.com/search/%E5%BC%A0%E6%9C%AC%E6%99%BA%E5%92%8C%E5%8F%91%E6%96%87%E5%9B%9E%E5%BA%94%E8%B4%A5%E7%BB%99%E6%A8%8A%E6%8C%AF%E4%B8%9C?type=video'

    crawler = WeiboUserCrawler()
    crawler.set(keyword=keyword, save_path=save_path, cookie=cookie)
    asyncio.run(crawler.run())

    '''
    file_path = './data/weibo/topic.csv'
    crawler = WeiboUserCrawler()
    crawler.set(keyword=keyword, save_path=save_path, cookie=cookie)
    asyncio.run(crawler.run(file_path=file_path))
    '''

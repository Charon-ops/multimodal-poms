import execjs
import os
import csv
import traceback
import pandas as pd
from datetime import datetime
from typing import Dict
import asyncio
from playwright.async_api import async_playwright, BrowserContext
from lib.utils import get_headers, get_data, check_path
from .user_crawler import is_fans

def time_formater(input_time_str):
    input_format = '%a %b %d %H:%M:%S %z %Y'
    output_format = '%Y-%m-%d %H:%M:%S'
    return datetime.strptime(input_time_str, input_format).strftime(output_format)


def mid2id(id):
    jspython = u'str62keys = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";\n    /**\n    * 62进制值转换为10进制\n    * @param {String} str62 62进制值\n    * @return {String} 10进制值\n    */\n    function str62to10(str62) {\n        var i10 = 0;\n        for (var i = 0; i < str62.length; i++) {\n                var n = str62.length - i - 1;\n                var s = str62.substr(i, 1);  // str62[i]; 字符串用数组方式获取,IE下不支持为“undefined”\n                i10 += parseInt(str62keys.indexOf(s)) * Math.pow(62, n);\n        }\n        return i10;\n    }\n    /**\n    * mid转换为id\n    * @param {String} mid 微博mid,如 "wr4mOFqpbO"\n    * @return {String} 微博id,如 "201110410216293360"\n    */\n    function mid2id(mid) {\n        var id = \'\';\n        for (var i = mid.length - 4; i > -4; i = i - 4) //从最后往前以4字节为一组读取mid字符\n{\n                var offset1 = i < 0 ? 0 : i;\n                var len = i < 0 ? parseInt(mid.length % 4) : 4;\n                var str = mid.substr(offset1, len);\n                str = str62to10(str).toString();\n                if (offset1 > 0)//若不是第一组,则不足7位补0\n                {\n                        while (str.length < 7) {\n                                str = \'0\' + str;\n                        }\n                }\n                id = str + id;\n        }\n        return id;\n    }'
    ctx = execjs.compile(jspython)
    return ctx.call('mid2id', id)


class WeiboCommentCrawler:
    def __init__(self):
        self.headers: Dict
        self.keyword: str
        self.save_path: str
        self.params: Dict
        self.limit: int = 1000000
        self.per_request_sleep_sec: int = 6
        self.request_timeout: int = 1000
        self.child_max_page: int = 5
        self.got_comments: Dict
        self.got_comments_num: int
        self.written_comments_num: int
        self.root_id: str
        self.context: BrowserContext
        self.cookie: str

    def set(self, keyword: str, save_path: str, cookie: str, uid=None, id=None, limit=None, child_max_page=None) -> None:
        self.params = {
            'is_reload': '1',
            'id': '4663893136511324',
            'is_show_bulletin': '2',
            'is_mix': '0',
            'count': '100',
            'uid': '6882947163',
            'flow': '0'
        }
        self.keyword = keyword
        self.cookie = cookie
        self.save_path = save_path + f'/weibo/{keyword}'
        self.headers = get_headers(self.cookie,'wb_comment')
        if uid:
            self.params['uid'] = uid
        if id:
            if not str.isdigit(id):
                id = mid2id(id)
            self.params['id'] = id
        if limit:
            self.limit = limit
        if child_max_page:
            self.child_max_page = child_max_page
        self.got_comments = []
        self.got_comments_num = 0
        self.written_comments_num = 0
        self.root_id = None

    async def parse(self, res_json, root_comment_id=None):
        if res_json['ok'] == 0:
            print('system is busy')
            return
        else:
            comments = res_json['data']
            if len(comments) == 0:
                print('data crawl finish')
                return
            for comment in comments:
                parent_comment_id = '' if root_comment_id is None else root_comment_id
                comment_id = comment['id']
                comment_time = comment['created_at']
                comment_time = time_formater(comment_time)
                comment_user_name = comment['user']['screen_name']
                comment_user_link = 'https://weibo.com' + \
                    comment['user']['profile_url']
                comment_content = comment['text']
                try:
                    comment_like_num = comment['like_counts']
                except:
                    try:
                        comment_like_num = comment['like_count']
                    except:
                        comment_like_num = -1

                child_comment_num = comment['total_number'] if root_comment_id is None else 0
                isfan = None

                # 以下为判断粉丝功能，可能导致运行时间过长，可删去
                # root_uid = self.params['uid'] if root_comment_id is None else self.root_id
                # isfan = 'YES' if is_fans(self,cookie, root_uid, comment_user_link[comment_user_link.rindex('/')+1:]) else 'NO'

                one_comment = {
                    'parent_comment_id': parent_comment_id,
                    'comment_id': comment_id,
                    'comment_time': comment_time,
                    'comment_user_name': comment_user_name,
                    'user_link': comment_user_link,
                    'comment_content': comment_content,
                    'comment_like_num': comment_like_num,
                    'child_comment_num': child_comment_num,
                    'isfan': '' if isfan is None else isfan
                }
                print(parent_comment_id, comment_id, comment_time, comment_user_name,
                      comment_user_link, comment_content, comment_like_num, child_comment_num, isfan)
                self.got_comments.append(one_comment)
                self.got_comments_num += 1
                if not root_comment_id:
                    self.root_id = comment_user_link[comment_user_link.rindex('/')+1:]
                    await self.parseChild(comment_id)

            if not res_json['max_id']:
                print('max_id is null')
            return res_json['max_id']


    async def parseChild(self, root_comment_id):
        child_params = {
            'is_reload': '1',
            'id': '4663893136511324',
            'is_show_bulletin': '2',
            'is_mix': '0',
            'count': '20',
            'uid': '6882947163',
            'fetch_level': '1',
            'flow': '0'
        }
        child_params['uid'] = self.params['uid']
        child_params['id'] = root_comment_id
        page = 1
        child_max_page = self.child_max_page
        while True:
            if page > child_max_page:
                print(
                    f'''child page up to max_page_limit == {child_max_page}''')
                break
            print(
                f'''............ {root_comment_id} child page: {page} .........''')
            try:
                response = await get_data(self.context, 'https://weibo.com/ajax/statuses/buildComments',headers=self.headers, params=child_params, timeout=self.request_timeout)
            except:
                print(traceback.format_exc())
                break

            res_json = response
            max_id = await self.parse(res_json, root_comment_id)
            if not max_id:
                break
            child_params['max_id'] = max_id
            await asyncio.sleep(self.per_request_sleep_sec)
            page += 1

    def write_csv(self):
        u"""将爬取的信息写入csv文件"""
        try:
            result_headers = [
                'parent_comment_id',
                'comment_id',
                'comment_time',
                'comment_user_name',
                'user_link',
                'comment_content',
                'comment_like_num',
                'child_comment_num',
                'isfan']
            result_data = [w.values()
                           for w in self.got_comments][self.written_comments_num:]
            file_path = f"{self.save_path}/comment/{self.params['id']}.csv"
            with open(file_path, 'a', encoding='utf-8-sig', newline='') as (f):
                writer = csv.writer(f)
                if self.written_comments_num == 0:
                    writer.writerows([result_headers])
                writer.writerows(result_data)
            print(u'%d 条评论写入csv文件完毕:' % self.got_comments_num)
            self.written_comments_num = self.got_comments_num
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def drop_duplicate(self, path, col_index=0):
        df = pd.read_csv(path)
        first_column = df.columns.tolist()[col_index]
        df.drop_duplicates(keep='first', inplace=True, subset=[first_column])
        df = df[-df[first_column].isin([first_column])]
        df.to_csv(path, encoding='utf-8-sig', index=False)

    async def fetch(self):
        page = 1
        last_max_id = None
        self.params['max_id'] = None
        max_id = None
        while True:
            print(f'''............page: {page} .........''')
            try:
                response = await get_data(self.context, 'https://weibo.com/ajax/statuses/buildComments',headers=self.headers, params=self.params, timeout=self.request_timeout)
            except:
                print(traceback.format_exc())
                break
            try:
                max_id = await self.parse(response)
            except:
                print(response['data'])
                print(traceback.format_exc())
                break

            if not max_id or max_id == last_max_id:
                break
            last_max_id = max_id
            self.params['max_id'] = max_id
            if page % 3 == 0:
                if self.got_comments_num > self.written_comments_num:
                    self.write_csv()
            if self.written_comments_num >= self.limit:
                break
            await asyncio.sleep(self.per_request_sleep_sec)
            page += 1

        if self.got_comments_num > self.written_comments_num:
            self.write_csv()
        file_path = f"{self.save_path}/comment/{self.params['id']}.csv"
        if os.path.exists(file_path):
            self.drop_duplicate(file_path, col_index=1)

    async def main(self) -> None:
        if not self.keyword:
            print("run after set")
            return
        check_path(f'{self.save_path}')
        check_path(f'{self.save_path}/comment')
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            self.context = await browser.new_context()
            await self.fetch()
            await browser.close()

    def get_filenames(self,path):
        if not os.path.exists(path):
            os.mkdir(path)
        filenames = [f[:-4] for f in os.listdir(path) if f.endswith('.csv')]
        return filenames

    def is_in_results(self, mid, path):
        filenames = self.get_filenames(path)
        return mid in filenames

    async def run(self, keyword=None, save_path=None, cookie=None, mid_col='mid', user_col='user_link', limit=None, child_max_page=None):
        if not self.keyword:
            print("run after set")
            return
        if keyword is None:
            keyword = self.keyword
        if save_path is None:
            save_path = self.save_path
        if cookie is None:
            cookie = self.cookie
        file_path = f'{save_path}/{keyword}.csv'
        if not os.path.exists(file_path):
            print("run after get topic csv file")
            return
        print(u'\n----------------------------------------------------------------\n')
        path = f'{save_path}//comment'
        df = pd.read_csv(file_path)
        num = 0
        for index, row in df.iterrows():
            mid = row[mid_col]
            if self.is_in_results(str(mid), path):
                num += 1
        count = 1
        for index, row in df.iterrows():
            mid = row[mid_col]
            if count < num:
                if self.is_in_results(str(mid), path):
                    count += 1
                print(f'   {index+1}/{df.shape[0]}   ')
                print('skip')
                continue
            elif count == num:
                if self.is_in_results(str(mid), path):
                    count += 1
                    os.remove(os.path.join(path, str(mid) + '.csv'))
                    print(f'   {index+1}/{df.shape[0]}   ')
                    mid = row[mid_col]
                    user_link = row[user_col]
                    if '?' in user_link:
                        user_link = user_link[:user_link.rindex('?')]
                    uid = user_link[user_link.rindex('/')+1:]
                    print(f'''-------- 开始爬取 mid = {mid} --------''')
                    crawler = WeiboCommentCrawler()
                    crawler.set(keyword=keyword, save_path=save_path, cookie=cookie, uid=uid, id=str(mid), limit=limit, child_max_page=child_max_page)
                    await crawler.main()
                else:
                    print(f'   {index+1}/{df.shape[0]}   ')
                    print('skip')
            else:
                print(f'   {index+1}/{df.shape[0]}   ')
                mid = row[mid_col]
                user_link = row[user_col]
                if '?' in user_link:
                    user_link = user_link[:user_link.rindex('?')]
                uid = user_link[user_link.rindex('/')+1:]
                print(f'''-------- 开始爬取 mid = {mid} --------''')
                crawler = WeiboCommentCrawler()
                crawler.set(keyword=keyword, save_path=save_path, cookie=cookie, uid=uid, id=str(mid), limit=limit, child_max_page=child_max_page)
                await crawler.main()

if __name__ == '__main__':
    keyword = '张本智和发文回应败给樊振东'
    save_path='./data'

    cookie = 'ttwid=1%7CiFrIbo5zacI4ggwR_lLWAeCSUmLqkBuXBX_qe8zBXoU%7C1722161376%7Ccba5a9e8bc0bb928681e50aa867d5b1e80d0ae5c0a129476bfb25031a2d36482; UIFID_TEMP=3c3e9d4a635845249e00419877a3730e2149197a63ddb1d8525033ea2b3354c200afd64d8c1da1ce41b1ee1bbb5f209dc6251ef96e5c67a33ee8bd729f6745cb5de701d949d74ff9264d56ea102b354e8af69aa3db82add4e64dada7f96d037c89e2362e6188ae17dc96c9eb19ac23b5; dy_swidth=1707; dy_sheight=960; fpk1=U2FsdGVkX18w6wg2y3GHFgRlPqr9PVTbe7Ec+iNqMr3KTqQE5cudpn9AmSAf5Gr2b+7+7ZbhU7XiDE1D6/TWaw==; fpk2=f1f6b29a6cc1f79a0fea05b885aa33d0; s_v_web_id=verify_lz5ef986_YtGvWi74_5grO_4C6Y_9uRD_nknO19CQX3E8; xgplayer_user_id=986392702227; passport_csrf_token=019173788f6bd671f34676b4620d19d2; passport_csrf_token_default=019173788f6bd671f34676b4620d19d2; bd_ticket_guard_client_web_domain=2; UIFID=3c3e9d4a635845249e00419877a3730e2149197a63ddb1d8525033ea2b3354c200afd64d8c1da1ce41b1ee1bbb5f209dc6251ef96e5c67a33ee8bd729f6745cb9c4d53ba7684c5d32c87d18c636e9cb41413e1dd3b742ea271cb21a96706b88b1f7bc42f45d10ebd560b036c3ad13331b2da0b3994918adb0761cfbfc9de149b2a0b8ae294f2c4fe944c3aa41928019aa2eb335a7d40698c8e3eddb7627f42e1121a35df8003c96abe71f8040ddff4cbe0b6c5493baaf7618018460655e2c133; live_use_vvc=%22false%22; FORCE_LOGIN=%7B%22videoConsumedRemainSeconds%22%3A180%2C%22isForcePopClose%22%3A1%7D; SEARCH_RESULT_LIST_TYPE=%22single%22; d_ticket=4837a1f785bad649ed04dcc018b52541d1a81; n_mh=9QQeuVUxM4mSMdUxjWaD4Jt2f1UXjlKvsPxugBOFshw; sso_auth_status=72f222ff4ada49b49a8d07fb936f8509; sso_auth_status_ss=72f222ff4ada49b49a8d07fb936f8509; _bd_ticket_crypt_doamin=2; __security_server_data_status=1; store-region=cn-jl; store-region-src=uid; is_staff_user=false; my_rd=2; passport_auth_status=5a552fab2ee073e90c03a5350df21ca5%2Ccba223b3a04022fb0ef128d0609fcf5d; passport_auth_status_ss=5a552fab2ee073e90c03a5350df21ca5%2Ccba223b3a04022fb0ef128d0609fcf5d; strategyABtestKey=%221723266438.532%22; publish_badge_show_info=%220%2C0%2C0%2C1723266443099%22; xgplayer_device_id=92915748857; volume_info=%7B%22isUserMute%22%3Afalse%2C%22isMute%22%3Atrue%2C%22volume%22%3A0.6%7D; stream_player_status_params=%22%7B%5C%22is_auto_play%5C%22%3A0%2C%5C%22is_full_screen%5C%22%3A0%2C%5C%22is_full_webscreen%5C%22%3A0%2C%5C%22is_mute%5C%22%3A1%2C%5C%22is_speed%5C%22%3A1%2C%5C%22is_visible%5C%22%3A1%7D%22; download_guide=%223%2F20240810%2F0%22; csrf_session_id=83e9cec3d90e17cd9efcfd09100bf98a; biz_trace_id=543f0a58; passport_assist_user=CkBKO39GjbcQt2Mf5b0x35LoEcUwfrO44SPO3_QWeomfmqyY82xbeFSfNIUMvyb8ui7cIK5q2vM6lfkaVPlVUV-3GkoKPL_jn8O2VQ6UP1Fv_sormgplo_6K6kylSizvTwPJgj7XXZXa5oXPipOtOQ8ahINEPTyM-4Vsygjhb0xdGxDZ_9gNGImv1lQgASIBA7LhWVY%3D; sso_uid_tt=9238686f1526b23e2f7f8f6452014bde; sso_uid_tt_ss=9238686f1526b23e2f7f8f6452014bde; toutiao_sso_user=fbb34bd50183e9071f4787cfd53b9b2c; toutiao_sso_user_ss=fbb34bd50183e9071f4787cfd53b9b2c; sid_ucp_sso_v1=1.0.0-KGM2OGMxNjYyYWY1OTcxNGE5NzFmNTk5NDY1MTFjNGI0MTkzZmUwYTQKIAjEouCdmfQ5EPWS3LUGGO8xIAww1p3l8AU4AkDvB0gGGgJobCIgZmJiMzRiZDUwMTgzZTkwNzFmNDc4N2NmZDUzYjliMmM; ssid_ucp_sso_v1=1.0.0-KGM2OGMxNjYyYWY1OTcxNGE5NzFmNTk5NDY1MTFjNGI0MTkzZmUwYTQKIAjEouCdmfQ5EPWS3LUGGO8xIAww1p3l8AU4AkDvB0gGGgJobCIgZmJiMzRiZDUwMTgzZTkwNzFmNDc4N2NmZDUzYjliMmM; uid_tt=2b631745fc5d041c67dc0a8ffb0a2934; uid_tt_ss=2b631745fc5d041c67dc0a8ffb0a2934; sid_tt=4f1178dd046fcfdf123d9873c21fff1c; sessionid=4f1178dd046fcfdf123d9873c21fff1c; sessionid_ss=4f1178dd046fcfdf123d9873c21fff1c; _bd_ticket_crypt_cookie=ea0aca8e2fe74d7fbb5cccff4fb8b1d7; sid_guard=4f1178dd046fcfdf123d9873c21fff1c%7C1723271545%7C5183999%7CWed%2C+09-Oct-2024+06%3A32%3A24+GMT; sid_ucp_v1=1.0.0-KGU5OGJiMDcxZmIzNzBkMTBlMzUwNjUxZjE2YTE5OTBlNzkwMWY4ZGYKGgjEouCdmfQ5EPmS3LUGGO8xIAw4AkDvB0gEGgJobCIgNGYxMTc4ZGQwNDZmY2ZkZjEyM2Q5ODczYzIxZmZmMWM; ssid_ucp_v1=1.0.0-KGU5OGJiMDcxZmIzNzBkMTBlMzUwNjUxZjE2YTE5OTBlNzkwMWY4ZGYKGgjEouCdmfQ5EPmS3LUGGO8xIAw4AkDvB0gEGgJobCIgNGYxMTc4ZGQwNDZmY2ZkZjEyM2Q5ODczYzIxZmZmMWM; pwa2=%220%7C0%7C3%7C0%22; __live_version__=%221.1.2.2711%22; webcast_local_quality=null; live_can_add_dy_2_desktop=%220%22; stream_recommend_feed_params=%22%7B%5C%22cookie_enabled%5C%22%3Atrue%2C%5C%22screen_width%5C%22%3A1707%2C%5C%22screen_height%5C%22%3A960%2C%5C%22browser_online%5C%22%3Atrue%2C%5C%22cpu_core_num%5C%22%3A20%2C%5C%22device_memory%5C%22%3A8%2C%5C%22downlink%5C%22%3A10%2C%5C%22effective_type%5C%22%3A%5C%224g%5C%22%2C%5C%22round_trip_time%5C%22%3A50%7D%22; home_can_add_dy_2_desktop=%221%22; bd_ticket_guard_client_data=eyJiZC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwiYmQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJiZC10aWNrZXQtZ3VhcmQtcmVlLXB1YmxpYy1rZXkiOiJCRmZ0VW5yWEE2MDc1dkFUN3BhNXppQjBnajk1MTBIWitHMjZjU08zTHlFdjduZWkwYlYxTEJmWHJHc1lteElQaVFBdGNJcXFLMGhCbFpiL1FrVEZ3RlU9IiwiYmQtdGlja2V0LWd1YXJkLXdlYi12ZXJzaW9uIjoxfQ%3D%3D; odin_tt=a87e86db22068aa9b1feb37220c56114545586bcd6a01f76ac0f7b9597d23549beba8917b6cbf85bd40cc795dbbb6450; WallpaperGuide=%7B%22showTime%22%3A1723266497064%2C%22closeTime%22%3A0%2C%22showCount%22%3A3%2C%22cursor1%22%3A106%2C%22cursor2%22%3A0%2C%22hoverTime%22%3A1722180561085%7D; passport_fe_beating_status=true; IsDouyinActive=false; __ac_nonce=066b74337001142102e7e; __ac_signature=_02B4Z6wo00f014dJTqAAAIDCWzDRFdUXJo-HaUoAAIdtee; __ac_referer=https://www.douyin.com/search/%E5%BC%A0%E6%9C%AC%E6%99%BA%E5%92%8C%E5%8F%91%E6%96%87%E5%9B%9E%E5%BA%94%E8%B4%A5%E7%BB%99%E6%A8%8A%E6%8C%AF%E4%B8%9C?type=video'

    crawler = WeiboCommentCrawler()
    crawler.set(keyword=keyword, save_path=save_path, cookie=cookie)
    asyncio.run(crawler.run())

import csv
import traceback
from datetime import datetime, timedelta
from time import sleep
from lxml import etree
from typing import Dict
import asyncio
from playwright.async_api import async_playwright, BrowserContext
from lib.utils import get_headers, get_data, check_path, load_config

def formatLimitTime(limit_time):
    if limit_time[-2] == '-':
        limit_time = limit_time[:-2] + ' 0' + limit_time[-1]
    else:
        if limit_time[-3] == '-':
            limit_time = limit_time[:-3] + ' ' + limit_time[-2]
    return limit_time


def unformatLimitTime(limit_time):
    if limit_time[-2] == '0':
        limit_time = limit_time[:-3] + '-' + limit_time[-1]
    else:
        limit_time = limit_time[:-3] + '-' + limit_time[-2:]
    return limit_time


def parseTime(publish_time):
    if u'刚刚' in publish_time:
        publish_time = datetime.now().strftime('%Y-%m-%d %H:%M')
    else:
        if u'分钟' in publish_time:
            minute = publish_time[:publish_time.find(u'分钟')]
            minute = timedelta(minutes=int(minute))
            publish_time = (datetime.now() - minute).strftime('%Y-%m-%d %H:%M')
        else:
            if u'今天' in publish_time:
                today = datetime.now().strftime('%Y-%m-%d')
                time = publish_time[3:]
                publish_time = today + ' ' + time
            else:
                if u'月' in publish_time:
                    if u'年' in publish_time:
                        if u'日' in publish_time:
                            if publish_time.index(u'月') == 6:
                                publish_time = publish_time[:5] +\
                                    '0' + publish_time[5:]
                            if publish_time.index(u'日') == 9:
                                publish_time = publish_time[:8] + \
                                    '0' + publish_time[8:]
                            publish_time = publish_time.replace(u'年', '-')
                            publish_time = publish_time.replace(u'月', '-')
                            publish_time = publish_time.replace(u'日', '')
                    else:
                        year = datetime.now().strftime('%Y')
                        month = publish_time[0:2]
                        day = publish_time[3:5]
                        time = publish_time[7:12]
                        publish_time = year + '-' + month + '-' + day + ' ' + time
                else:
                    publish_time = publish_time[:16]
    return publish_time

def strToDate(str_time):
    times_arr = str_time.split('-')
    year = int(times_arr[0])
    month = int(times_arr[1])
    day = int(times_arr[2])
    hour = int(times_arr[3])
    return datetime(year=year, month=month, day=day, hour=hour)


def dateToStr(dt_time):
    year = str(dt_time.year)
    month = str(dt_time.month).zfill(2)
    day = str(dt_time.day).zfill(2)
    hour = str(dt_time.hour)
    return f'''{year}-{month}-{day}-{hour}'''

class WeiboTopicCrawler:
    def __init__(self):
        self.headers: Dict
        self.keyword: str
        self.save_path: str
        self.start_time: str
        self.end_time: str
        self.got_weibos: Dict
        self.got_weibo_ids: Dict
        self.got_weibos_num: int
        self.written_weibos_num: int
        self.context: BrowserContext
        self.timeout: int = 10
        self.max_page: int = 50
        self.params: Dict
        self.realtime_params: Dict
        self.hot_params: Dict

    def set(self, keyword: str, save_path: str, cookie: str) -> None:
        self.keyword = keyword
        self.save_path = save_path + f'/weibo/{keyword}'
        self.headers = get_headers(cookie,'wb_topic')
        config = load_config()
        weibo_config = config.get('weibo', {})
        self.start_time = weibo_config.get('start_time')
        self.end_time = weibo_config.get('end_time')
        self.got_weibos = []
        self.got_weibo_ids = []
        self.got_weibos_num = 0
        self.written_weibos_num = 0
        self.params = {
            'q': u'华为P50',
            'typeall': '1',
            'suball': '1',
            'timescope': 'custom:2021-05-01-9:2021-06-01-16',
            'Refer': 'g',
            'page': '1'
        }
        self.realtime_params = {
            'q': u'苏炳添晋级百米半决赛',
            'rd': 'realtime',
            'tw': 'realtime',
            'Refer': 'hot_realtime',
            'page': '3'
        }
        self.hot_params = {
            'q': u'苏炳添晋级百米半决赛',
            'suball': '1',
            'xsort': 'hot',
            'tw': 'hotweibo',
            'Refer': 'weibo_hot',
            'page': '2'
        }

    def getLocation(self, html):
        candidate = html.xpath('.//p[@class="txt"]/a[child::*[contains(text(), "2")]]')
        if len(candidate) > 0:
            location_url = candidate[0].xpath('./@href')[0]
            location_name = candidate[0].xpath('string(.)').strip().replace('2', '')
            return (location_url, location_name)

    def parseWeibo(self, html):
        weibos = html.xpath('//div[@class="card-wrap" and @mid]')
        if len(weibos) == 0:
            print(html)
            yield None
        print(len(weibos))
        for weibo in weibos:
            mid = weibo.xpath('./@mid')[0]
            user_info = weibo.xpath('.//div[@class="info"]/div/a[@class="name"]')[0]
            user_name = user_info.xpath('./text()')[0]
            user_link = 'https:' + user_info.xpath('./@href')[0]
            try:
                content = weibo.xpath('.//div[@class="content"]/p[@class="txt" and @node-type="feed_list_content_full"]')[0].xpath('string(.)').strip()
            except:
                content = weibo.xpath('.//div[@class="content"]/p[@class="txt" and @node-type="feed_list_content"]')[0].xpath('string(.)').strip()

            images = weibo.xpath('.//div[@node-type="feed_list_media_prev"]/div/ul/li')
            image_urls = []
            for image in images:
                image_url = image.xpath('./img/@src')[0]
                if not image_url.startswith('http'):
                    image_url = 'https:' + image_url
                image_urls.append(image_url)

            p_from = weibo.xpath('.//div[@class="content"]/div[@class="from"]/a')[0]
            try:
                source = weibo.xpath('.//div[@class="content"]/div[@class="from"]/a[@rel="nofollow"]/text()')[0]
            except:
                source = ''

            weibo_link = 'https:' + p_from.xpath('./@href')[0]
            weibo_link = weibo_link[:weibo_link.rindex('?')]
            publish_time = p_from.xpath('./text()')[0].strip()
            publish_time = parseTime(publish_time)
            bottoms = weibo.xpath('.//div[@class="card-act"]/ul/li')
            location = self.getLocation(weibo)
            if location and not location[0].strip() == 'javascript:void(0);':
                location_url, location_name = location[0], location[1]
            else:
                (location_url, location_name) = ('', '')
            print(location_url, location_name)
            if weibo.xpath(u'.//span[@title="微博个人认证"]'):
                if weibo.xpath('.//svg[@id="woo_svg_vyellow"]'):
                    verify_typ = u'黄V认证'
                elif weibo.xpath('.//svg[@id="woo_svg_vgold"]'):
                    verify_typ = u'红V认证'
                else:
                    raise Exception(f'''{weibo_link} {user_name}''')
            else:
                if weibo.xpath(u'.//span[@title="微博官方认证"]'):
                    verify_typ = u'蓝V认证'
                else:
                    verify_typ = u'没有认证'
            (forward_num, comment_num, like_num) = (0, 0, 0)
            start_index = len(bottoms) - 4
            for index, bottom in enumerate(bottoms):
                if index == start_index + 1:
                    try:
                        forward_num = bottom.xpath('./a/text()')[1].strip()
                    except:
                        forward_num = bottom.xpath('./a/text()')[0].strip()

                    forward_num = forward_num.replace(u'转发', '').strip()
                    if len(forward_num) == 0:
                        forward_num = 0
                elif index == start_index + 2:
                    comment_num = bottom.xpath('./a/text()')[0].strip()
                    comment_num = comment_num.replace(u'评论', '').strip()
                    if len(comment_num) == 0:
                        comment_num = 0
                elif index == start_index + 3:
                    try:
                        like_num = bottom.xpath(
                            './a/button/span[last()]/text()')[0].strip()
                        like_num = like_num.replace(u'赞', '').strip()
                        if len(like_num) == 0:
                            like_num = 0
                    except:
                        try:
                            like_num = bottom.xpath(
                                './/em/text()')[0].strip()
                        except:
                            like_num = 0

            aweibo = {'mid': mid,
                      'publish_time': publish_time,
                      'user_name': user_name,
                      'user_link': user_link,
                      'content': content,
                      'source': source,
                      'location_url': location_url,
                      'location_name': location_name,
                      'image_urls': (' ').join(image_urls),
                      'weibo_link': weibo_link,
                      'forward_num': forward_num,
                      'comment_num': comment_num,
                      'like_num': like_num,
                      'verify_typ': verify_typ}
            print(publish_time, mid, user_name, user_link, content.encode('GBK', 'ignore').decode(
                'GBK'), image_urls, weibo_link, forward_num, comment_num, like_num, verify_typ)
            yield aweibo

    def write_csv(self):
        u"""将爬取的信息写入csv文件"""
        try:
            result_headers = ['mid',
                              'publish_time',
                              'user_name',
                              'user_link',
                              'content',
                              'source',
                              'location_url',
                              'location_name',
                              'image_urls',
                              'weibo_link',
                              'forward_num',
                              'comment_num',
                              'like_num',
                              'verify_typ']
            result_data = [w.values()
                           for w in self.got_weibos][self.written_weibos_num:]
            file_path = f"{self.save_path}/{self.keyword}.csv"
            with open(file_path, 'a', encoding='utf-8-sig', newline='') as (f):
                writer = csv.writer(f)
                if self.written_weibos_num == 0:
                    writer.writerows([result_headers])
                writer.writerows(result_data)
            print(u'%d条微博写入csv文件完毕:' %
                  self.got_weibos_num)
            self.written_weibos_num = self.got_weibos_num
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    async def fetch(self):
        while True:
            print(u'----------------------------------------------------------------\n\n')
            self.params['q'] = self.keyword
            self.params['timescope'] = f'''custom:{(self.start_time)}:{(self.end_time)}'''
            current_page = 1
            this_turn_weibo_count = 0
            while current_page <= self.max_page:
                self.params['page'] = str(current_page)
                try:
                    response = await get_data(self.context, 'https://s.weibo.com/weibo', headers=self.headers, params=self.params, timeout=self.timeout)
                    print(response.url)
                except:
                    print(traceback.format_exc())
                    print('network error')
                    break

                print(f'''page : {current_page} {response.url}''')
                if response.status_code == 200:
                    res_html = etree.HTML(response.text.encode(response.encoding).decode('utf-8-sig', 'ignore'))
                    if res_html.xpath('//div[contains(@class,"card-no-result")]/p/text()'):
                        print(f'''抱歉,未找到“{self.keyword}”相关结果。''')
                        await asyncio.sleep(10)
                        break
                    for weibo in self.parseWeibo(res_html):
                        if weibo is None:
                            current_page = self.max_page
                            print('\n________ DATA IS NONE__________\n')
                            break
                        if self.keyword in weibo.get('content', 'weibo') or True:
                            self.got_weibos.append(weibo)
                            self.got_weibo_ids.append(weibo['mid'])
                            self.got_weibos_num += 1
                            this_turn_weibo_count += 1

                response.close()
                if current_page % 3 == 0:
                    if self.got_weibos_num > self.written_weibos_num:
                        self.write_csv()
                await asyncio.sleep(3)
                current_page += 1

            if self.got_weibos_num > self.written_weibos_num:
                self.write_csv()
            try:
                earliest_time = unformatLimitTime(self.got_weibos[-1]['publish_time'][:-3])
                if this_turn_weibo_count == 0:
                    raise Exception('data none exception')
                consist_error_times = 0
            except:
                consist_error_times += 1
                print(traceback.format_exc())
                earliest_time = dateToStr(strToDate(self.end_time) + (timedelta(hours=consist_error_times * -2)))

            if earliest_time > self.start_time:
                self.end_time = earliest_time
                print(f'''------------{self.start_time}---------------{self.end_time}--------------''')
            else:
                break

    async def run(self) -> None:
        if not self.keyword:
            print("run after set")
            return
        check_path(f'{self.save_path}')
        with open(f"{self.save_path}/{self.keyword}.csv", "w", encoding="utf-8-sig", newline="") as f:
            pass
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            self.context = await browser.new_context()
            await self.fetch()
            await browser.close()

if __name__ == '__main__':
    keyword = '张本智和发文回应败给樊振东'
    save_path='./data'

    cookie = 'ttwid=1%7CiFrIbo5zacI4ggwR_lLWAeCSUmLqkBuXBX_qe8zBXoU%7C1722161376%7Ccba5a9e8bc0bb928681e50aa867d5b1e80d0ae5c0a129476bfb25031a2d36482; UIFID_TEMP=3c3e9d4a635845249e00419877a3730e2149197a63ddb1d8525033ea2b3354c200afd64d8c1da1ce41b1ee1bbb5f209dc6251ef96e5c67a33ee8bd729f6745cb5de701d949d74ff9264d56ea102b354e8af69aa3db82add4e64dada7f96d037c89e2362e6188ae17dc96c9eb19ac23b5; dy_swidth=1707; dy_sheight=960; fpk1=U2FsdGVkX18w6wg2y3GHFgRlPqr9PVTbe7Ec+iNqMr3KTqQE5cudpn9AmSAf5Gr2b+7+7ZbhU7XiDE1D6/TWaw==; fpk2=f1f6b29a6cc1f79a0fea05b885aa33d0; s_v_web_id=verify_lz5ef986_YtGvWi74_5grO_4C6Y_9uRD_nknO19CQX3E8; xgplayer_user_id=986392702227; passport_csrf_token=019173788f6bd671f34676b4620d19d2; passport_csrf_token_default=019173788f6bd671f34676b4620d19d2; bd_ticket_guard_client_web_domain=2; UIFID=3c3e9d4a635845249e00419877a3730e2149197a63ddb1d8525033ea2b3354c200afd64d8c1da1ce41b1ee1bbb5f209dc6251ef96e5c67a33ee8bd729f6745cb9c4d53ba7684c5d32c87d18c636e9cb41413e1dd3b742ea271cb21a96706b88b1f7bc42f45d10ebd560b036c3ad13331b2da0b3994918adb0761cfbfc9de149b2a0b8ae294f2c4fe944c3aa41928019aa2eb335a7d40698c8e3eddb7627f42e1121a35df8003c96abe71f8040ddff4cbe0b6c5493baaf7618018460655e2c133; live_use_vvc=%22false%22; FORCE_LOGIN=%7B%22videoConsumedRemainSeconds%22%3A180%2C%22isForcePopClose%22%3A1%7D; SEARCH_RESULT_LIST_TYPE=%22single%22; d_ticket=4837a1f785bad649ed04dcc018b52541d1a81; n_mh=9QQeuVUxM4mSMdUxjWaD4Jt2f1UXjlKvsPxugBOFshw; sso_auth_status=72f222ff4ada49b49a8d07fb936f8509; sso_auth_status_ss=72f222ff4ada49b49a8d07fb936f8509; _bd_ticket_crypt_doamin=2; __security_server_data_status=1; store-region=cn-jl; store-region-src=uid; is_staff_user=false; my_rd=2; passport_auth_status=5a552fab2ee073e90c03a5350df21ca5%2Ccba223b3a04022fb0ef128d0609fcf5d; passport_auth_status_ss=5a552fab2ee073e90c03a5350df21ca5%2Ccba223b3a04022fb0ef128d0609fcf5d; strategyABtestKey=%221723266438.532%22; publish_badge_show_info=%220%2C0%2C0%2C1723266443099%22; xgplayer_device_id=92915748857; volume_info=%7B%22isUserMute%22%3Afalse%2C%22isMute%22%3Atrue%2C%22volume%22%3A0.6%7D; stream_player_status_params=%22%7B%5C%22is_auto_play%5C%22%3A0%2C%5C%22is_full_screen%5C%22%3A0%2C%5C%22is_full_webscreen%5C%22%3A0%2C%5C%22is_mute%5C%22%3A1%2C%5C%22is_speed%5C%22%3A1%2C%5C%22is_visible%5C%22%3A1%7D%22; download_guide=%223%2F20240810%2F0%22; csrf_session_id=83e9cec3d90e17cd9efcfd09100bf98a; biz_trace_id=543f0a58; passport_assist_user=CkBKO39GjbcQt2Mf5b0x35LoEcUwfrO44SPO3_QWeomfmqyY82xbeFSfNIUMvyb8ui7cIK5q2vM6lfkaVPlVUV-3GkoKPL_jn8O2VQ6UP1Fv_sormgplo_6K6kylSizvTwPJgj7XXZXa5oXPipOtOQ8ahINEPTyM-4Vsygjhb0xdGxDZ_9gNGImv1lQgASIBA7LhWVY%3D; sso_uid_tt=9238686f1526b23e2f7f8f6452014bde; sso_uid_tt_ss=9238686f1526b23e2f7f8f6452014bde; toutiao_sso_user=fbb34bd50183e9071f4787cfd53b9b2c; toutiao_sso_user_ss=fbb34bd50183e9071f4787cfd53b9b2c; sid_ucp_sso_v1=1.0.0-KGM2OGMxNjYyYWY1OTcxNGE5NzFmNTk5NDY1MTFjNGI0MTkzZmUwYTQKIAjEouCdmfQ5EPWS3LUGGO8xIAww1p3l8AU4AkDvB0gGGgJobCIgZmJiMzRiZDUwMTgzZTkwNzFmNDc4N2NmZDUzYjliMmM; ssid_ucp_sso_v1=1.0.0-KGM2OGMxNjYyYWY1OTcxNGE5NzFmNTk5NDY1MTFjNGI0MTkzZmUwYTQKIAjEouCdmfQ5EPWS3LUGGO8xIAww1p3l8AU4AkDvB0gGGgJobCIgZmJiMzRiZDUwMTgzZTkwNzFmNDc4N2NmZDUzYjliMmM; uid_tt=2b631745fc5d041c67dc0a8ffb0a2934; uid_tt_ss=2b631745fc5d041c67dc0a8ffb0a2934; sid_tt=4f1178dd046fcfdf123d9873c21fff1c; sessionid=4f1178dd046fcfdf123d9873c21fff1c; sessionid_ss=4f1178dd046fcfdf123d9873c21fff1c; _bd_ticket_crypt_cookie=ea0aca8e2fe74d7fbb5cccff4fb8b1d7; sid_guard=4f1178dd046fcfdf123d9873c21fff1c%7C1723271545%7C5183999%7CWed%2C+09-Oct-2024+06%3A32%3A24+GMT; sid_ucp_v1=1.0.0-KGU5OGJiMDcxZmIzNzBkMTBlMzUwNjUxZjE2YTE5OTBlNzkwMWY4ZGYKGgjEouCdmfQ5EPmS3LUGGO8xIAw4AkDvB0gEGgJobCIgNGYxMTc4ZGQwNDZmY2ZkZjEyM2Q5ODczYzIxZmZmMWM; ssid_ucp_v1=1.0.0-KGU5OGJiMDcxZmIzNzBkMTBlMzUwNjUxZjE2YTE5OTBlNzkwMWY4ZGYKGgjEouCdmfQ5EPmS3LUGGO8xIAw4AkDvB0gEGgJobCIgNGYxMTc4ZGQwNDZmY2ZkZjEyM2Q5ODczYzIxZmZmMWM; pwa2=%220%7C0%7C3%7C0%22; __live_version__=%221.1.2.2711%22; webcast_local_quality=null; live_can_add_dy_2_desktop=%220%22; stream_recommend_feed_params=%22%7B%5C%22cookie_enabled%5C%22%3Atrue%2C%5C%22screen_width%5C%22%3A1707%2C%5C%22screen_height%5C%22%3A960%2C%5C%22browser_online%5C%22%3Atrue%2C%5C%22cpu_core_num%5C%22%3A20%2C%5C%22device_memory%5C%22%3A8%2C%5C%22downlink%5C%22%3A10%2C%5C%22effective_type%5C%22%3A%5C%224g%5C%22%2C%5C%22round_trip_time%5C%22%3A50%7D%22; home_can_add_dy_2_desktop=%221%22; bd_ticket_guard_client_data=eyJiZC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwiYmQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJiZC10aWNrZXQtZ3VhcmQtcmVlLXB1YmxpYy1rZXkiOiJCRmZ0VW5yWEE2MDc1dkFUN3BhNXppQjBnajk1MTBIWitHMjZjU08zTHlFdjduZWkwYlYxTEJmWHJHc1lteElQaVFBdGNJcXFLMGhCbFpiL1FrVEZ3RlU9IiwiYmQtdGlja2V0LWd1YXJkLXdlYi12ZXJzaW9uIjoxfQ%3D%3D; odin_tt=a87e86db22068aa9b1feb37220c56114545586bcd6a01f76ac0f7b9597d23549beba8917b6cbf85bd40cc795dbbb6450; WallpaperGuide=%7B%22showTime%22%3A1723266497064%2C%22closeTime%22%3A0%2C%22showCount%22%3A3%2C%22cursor1%22%3A106%2C%22cursor2%22%3A0%2C%22hoverTime%22%3A1722180561085%7D; passport_fe_beating_status=true; IsDouyinActive=false; __ac_nonce=066b74337001142102e7e; __ac_signature=_02B4Z6wo00f014dJTqAAAIDCWzDRFdUXJo-HaUoAAIdtee; __ac_referer=https://www.douyin.com/search/%E5%BC%A0%E6%9C%AC%E6%99%BA%E5%92%8C%E5%8F%91%E6%96%87%E5%9B%9E%E5%BA%94%E8%B4%A5%E7%BB%99%E6%A8%8A%E6%8C%AF%E4%B8%9C?type=video'

    crawler = WeiboTopicCrawler()
    crawler.set(keyword=keyword, save_path=save_path, cookie=cookie)
    asyncio.run(crawler.run())

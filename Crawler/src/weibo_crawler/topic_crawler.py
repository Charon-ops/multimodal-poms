import re
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
        self.timeout: int = 1000
        self.max_page: int = 50
        self.params: Dict
        self.realtime_params: Dict
        self.hot_params: Dict
        self.consist_error_times: int

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
        self.consist_error_times = 0
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
                elif weibo.xpath('.//svg[@id="woo_svg_vorange"]'):
                    verify_typ = u'橙V认证'
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
                    response = await self.context.request.get('https://s.weibo.com/weibo', headers=self.headers, params=self.params, timeout=self.timeout)
                    print(response.url)

                except:
                    print(traceback.format_exc())
                    print('network error')
                    break

                print(f'''page : {current_page} {response.url}''')
                if response.status == 200:
                    html_content = await response.text()
                    content_type = response.headers.get('content-type')
                    match = re.search(r'charset=([\w-]+)', content_type)
                    res_html = etree.HTML(html_content)
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
                self.consist_error_times = 0
            except:
                self.consist_error_times += 1
                print(traceback.format_exc())
                earliest_time = dateToStr(strToDate(self.end_time) + (timedelta(hours=self.consist_error_times * -2)))

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
    keyword = '#苹果官方回应iPhone16不支持微信#'
    save_path='./data'

    cookie = 'SSOLoginState=1660184239; SCF=At-x8pE6AZmEu_dgGas5Uox2X5BNAD5ACioXZFIk34WKhojT459gh9rsD80-3aqOQ_tpMdXqB3x1hhAT5pgrvGA.; _s_tentry=weibo.com; Apache=7389492910465.878.1723296461625; SINAGLOBAL=7389492910465.878.1723296461625; ULV=1723296461670:1:1:1:7389492910465.878.1723296461625:; PC_TOKEN=f211dfa34e; ALF=1727870140; SUB=_2A25L0dfsDeRhGeFJ41MR-S_MzjmIHXVor1UkrDV8PUJbkNAGLWfbkW1Nfx8MjUqSgzkjYz_2ykzyYonbho1xjTQv; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFvc6eMrwWd.Ac9w4zliKDH5JpX5KMhUgL.FoMN1h271K27SK-2dJLoIp7LxKML1KBLBKnLxKqL1hnLBoMNS0npeh.peh-f'

    crawler = WeiboTopicCrawler()
    crawler.set(keyword=keyword, save_path=save_path, cookie=cookie)
    asyncio.run(crawler.run())

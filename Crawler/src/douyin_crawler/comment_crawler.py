import urllib.parse
from typing import Dict, List
import asyncio
from playwright.async_api import async_playwright, BrowserContext
from lib.utils import get_time, get_data, get_headers, load_config, get_x_bogus, check_path, save_to_csv, splice_url, validate_or_default


class DouyinCommentCrawler:
    def __init__(self):
        self.headers: Dict
        self.keyword: str
        self.save_path: str
        self.columns: List
        self.context: BrowserContext
        self.video_count: int = 0
        self.comment_count: int = 0
        self.data_accumulator: List = []


    def set(self, keyword: str, save_path: str, cookie: str) -> None:
        if not keyword or not save_path or not cookie:
            self.keyword = self.save_path = self.headers = None
            return
        self.keyword = keyword
        self.save_path = save_path + f'/douyin/{keyword}'
        self.headers = get_headers(cookie,'dy')

        self.columns = ["用户名", "视频链接", "评论时间", "IP属地", "点赞数量", "评论内容"]

    async def save_comment_data(self, comment: Dict, aweme_id: str) -> None:
        try:
            ip_label = comment.get('ip_label', '未知')
            user_name = comment.get('user', {}).get('nickname', '无').strip()

            comment_data_dict = {
                "用户名": user_name,
                "视频链接": f"https://www.douyin.com/video/{aweme_id}",
                "评论时间": get_time(comment.get('create_time', 0)),
                "IP属地": ip_label,
                "点赞数量": comment.get('digg_count', 0),
                "回复数量": comment.get('reply_comment_total', 0),
                "评论内容": comment.get('text', '').strip().replace('\n', '')
            }

            self.comment_count += 1

            print(f"当前评论数: {self.comment_count}\n",
                f"用户名：{comment_data_dict['用户名']}\n",
                f"视频链接：{comment_data_dict['视频链接']}\n",
                f"评论时间：{comment_data_dict['评论时间']}\n",
                f"IP属地：{comment_data_dict['IP属地']}\n",
                f"点赞数量：{comment_data_dict['点赞数量']}\n",
                f"回复数量：{comment_data_dict['回复数量']}\n",
                f"评论内容：{comment_data_dict['评论内容']}\n"
                )
            self.data_accumulator.append(comment_data_dict)
            if len(self.data_accumulator) >= 30:
                file_name = f'{self.save_path}/{self.keyword}_comment.csv'
                await save_to_csv(self.data_accumulator, file_name, self.columns)
                file_name = f'{self.save_path}/{aweme_id}/{aweme_id}_comment.csv'
                await save_to_csv(self.data_accumulator, file_name, self.columns)
                self.data_accumulator = []
        except Exception as e:
            print(f"处理评论数据时出错: {e}")

    async def get_comment(self, page: int, aweme_id: str) -> Dict[str, any]:
        try:
            cursor = page * 20
            url = 'https://www.douyin.com/aweme/v1/web/comment/list?'
            params = {
                    'aid': 6383,
                    'aweme_id': aweme_id,
                    'cursor': cursor,
                    'cookie_enabled': 'true',
                    'platform': 'PC',
                    'downlink': 10,
                    'count': 20,
                }
            video_url =  url + splice_url(params)
            query = urllib.parse.urlparse(video_url).query
            x_bogus = get_x_bogus(query)
            video_url = video_url + '&X-Bogus=' + x_bogus

            await asyncio.sleep(4)
            json_data = await get_data(context=self.context, url=video_url, headers=self.headers)
            return json_data
        except Exception as e:
            print(f"获取评论时发生错误 (aweme_id: {aweme_id}, page: {page}): {e}")
            return {}

    async def search_keyword(self, offset: int=0) -> Dict[str, any]:
        url = 'https://www.douyin.com/aweme/v1/web/search/item/?'
        config = load_config()
        douyin_config = config.get('douyin', {})
        is_filter_search = douyin_config.get('is_filter_search')
        is_filter_search = validate_or_default(is_filter_search, valid_options=[0, 1], default=0)
        publish_time = douyin_config.get('publish_time')
        publish_time = validate_or_default(publish_time, valid_options=[0, 1, 7, 182], default=0)
        sort_type = douyin_config.get('sort_type')
        sort_type = validate_or_default(sort_type, valid_options=[0, 1, 2], default=0)
        params = {
            'aid': 6383,
            'channel': 'channel_pc_web',
            'search_channel': 'aweme_video_web',
            'keyword': self.keyword,
            'offset': offset,
            'is_filter_search': is_filter_search,
            'publish_time': publish_time,
            'sort_type': sort_type,
            'count': 16 if offset == 0 else 10,
        }
        await asyncio.sleep(4)
        try:
            json_data = await get_data(context=self.context, url=url, headers=self.headers, params=params)
            return json_data
        except asyncio.TimeoutError:
            print("请求超时")
            return {}
        except Exception as e:
            print(f"发生错误: {e}")
            return {}

    async def fetch(self, start_offset: int=0) -> None:
        offset = start_offset
        count = 16 if offset == 0 else 10
        while True:
            try:
                json_data = await self.search_keyword(offset)
                video_data_dict = json_data.get('data')
                if not video_data_dict:
                    print(f"未获取到有效的视频数据 (offset: {offset})")
                    offset += count
                    count = 10
                    continue
                for video_data in video_data_dict:
                    try:
                        if not video_data or 'aweme_info' not in video_data or not video_data['aweme_info']:
                            print("视频数据无效或缺少 aweme_info，跳过该条数据")
                            continue
                        aweme_info = video_data['aweme_info']
                        if isinstance(aweme_info, dict):
                            desc = aweme_info.get('desc', '').strip().replace('\n', '')
                            aweme_id = aweme_info.get('aweme_id').strip()
                            if not aweme_id:
                                print("视频ID缺失，跳过该条数据")
                                continue
                        else:
                            print(f"无效的视频信息数据类型：{aweme_info}")
                            continue
                        print(f"开始爬取 {desc} 的评论\n")
                        check_path(f'{self.save_path}/{aweme_id}')
                        with open(f'{self.save_path}/{aweme_id}/{aweme_id}_comment.csv', 'wb') as file:
                            pass
                        page = 0
                        while True:
                            try:
                                comment_data = await self.get_comment(page, aweme_id)
                                if not comment_data.get('comments'):
                                    print(f"没有评论数据 (aweme_id: {aweme_id}, page: {page})")
                                    continue
                                for comment in comment_data['comments']:
                                    if not comment:
                                        print(f"空的评论数据 (aweme_id: {aweme_id}, page: {page})：{comment}")
                                        continue
                                    if isinstance(comment, dict):
                                        await self.save_comment_data(comment, aweme_id)
                                    else:
                                        print(f"无效的评论数据类型 (aweme_id: {aweme_id}, page: {page})：{comment}")
                                        continue
                                if not comment_data.get('has_more'):
                                    break
                                page += 1
                                print('================爬取Page{}完毕================'.format(page))
                            except Exception as e:
                                print(f"获取与保存评论数据时出错 (aweme_id: {aweme_id}, page: {page}): {e}")
                                continue
                        if self.data_accumulator:
                            try:
                                await save_to_csv(self.data_accumulator, f'{self.save_path}/{self.keyword}_comment.csv', self.columns)
                                await save_to_csv(self.data_accumulator, f'{self.save_path}/{aweme_id}/{aweme_id}_comment.csv', self.columns)
                                self.data_accumulator = []
                            except Exception as e:
                                print(f"保存评论数据到CSV时出错: {e}")
                        print(f"{desc} 的评论爬取完毕\n")
                        await asyncio.sleep(1)
                        self.video_count += 1
                    except Exception as e:
                        print(f"处理视频数据时出错: {e}")
                        continue
                if json_data.get('has_more') == 0:
                    break
                offset += count
                count = 10
            except Exception as e:
                print(f"获取关键词搜索数据时出错 (offset: {offset}): {e}")
                offset += count
                count = 10
                continue

    async def run(self) -> None:
        try:
            if not self.keyword or not self.save_path or not self.headers:
                print("请先设置关键字、保存路径和请求头。")
                return
            check_path(f'{self.save_path}')
            try:
                with open(f"{self.save_path}/{self.keyword}_comments.csv", "w", encoding="utf-8-sig", newline="") as f:
                    pass
            except Exception as e:
                print(f"创建文件{self.save_path}/{self.keyword}_comments.csv时出错: {e}")
                return
            async with async_playwright() as p:
                try:
                    browser = await p.chromium.launch(headless=True)
                    self.context = await browser.new_context()
                    await self.fetch()
                except Exception as e:
                    print(f"浏览器启动或运行过程中出错: {e}")
                finally:
                    await browser.close()
        except Exception as e:
            print(f"运行过程中发生错误: {e}")

if __name__ == '__main__':
    keyword = '张本智和发文回应败给樊振东'
    save_path='./data'

    cookie = 'ttwid=1%7CiFrIbo5zacI4ggwR_lLWAeCSUmLqkBuXBX_qe8zBXoU%7C1722161376%7Ccba5a9e8bc0bb928681e50aa867d5b1e80d0ae5c0a129476bfb25031a2d36482; UIFID_TEMP=3c3e9d4a635845249e00419877a3730e2149197a63ddb1d8525033ea2b3354c200afd64d8c1da1ce41b1ee1bbb5f209dc6251ef96e5c67a33ee8bd729f6745cb5de701d949d74ff9264d56ea102b354e8af69aa3db82add4e64dada7f96d037c89e2362e6188ae17dc96c9eb19ac23b5; fpk1=U2FsdGVkX18w6wg2y3GHFgRlPqr9PVTbe7Ec+iNqMr3KTqQE5cudpn9AmSAf5Gr2b+7+7ZbhU7XiDE1D6/TWaw==; fpk2=f1f6b29a6cc1f79a0fea05b885aa33d0; s_v_web_id=verify_lz5ef986_YtGvWi74_5grO_4C6Y_9uRD_nknO19CQX3E8; xgplayer_user_id=986392702227; passport_csrf_token=019173788f6bd671f34676b4620d19d2; passport_csrf_token_default=019173788f6bd671f34676b4620d19d2; bd_ticket_guard_client_web_domain=2; UIFID=3c3e9d4a635845249e00419877a3730e2149197a63ddb1d8525033ea2b3354c200afd64d8c1da1ce41b1ee1bbb5f209dc6251ef96e5c67a33ee8bd729f6745cb9c4d53ba7684c5d32c87d18c636e9cb41413e1dd3b742ea271cb21a96706b88b1f7bc42f45d10ebd560b036c3ad13331b2da0b3994918adb0761cfbfc9de149b2a0b8ae294f2c4fe944c3aa41928019aa2eb335a7d40698c8e3eddb7627f42e1121a35df8003c96abe71f8040ddff4cbe0b6c5493baaf7618018460655e2c133; live_use_vvc=%22false%22; SEARCH_RESULT_LIST_TYPE=%22single%22; d_ticket=4837a1f785bad649ed04dcc018b52541d1a81; n_mh=9QQeuVUxM4mSMdUxjWaD4Jt2f1UXjlKvsPxugBOFshw; _bd_ticket_crypt_doamin=2; __security_server_data_status=1; store-region=cn-jl; store-region-src=uid; is_staff_user=false; my_rd=2; xgplayer_device_id=92915748857; passport_assist_user=CkBKO39GjbcQt2Mf5b0x35LoEcUwfrO44SPO3_QWeomfmqyY82xbeFSfNIUMvyb8ui7cIK5q2vM6lfkaVPlVUV-3GkoKPL_jn8O2VQ6UP1Fv_sormgplo_6K6kylSizvTwPJgj7XXZXa5oXPipOtOQ8ahINEPTyM-4Vsygjhb0xdGxDZ_9gNGImv1lQgASIBA7LhWVY%3D; sso_uid_tt=9238686f1526b23e2f7f8f6452014bde; sso_uid_tt_ss=9238686f1526b23e2f7f8f6452014bde; toutiao_sso_user=fbb34bd50183e9071f4787cfd53b9b2c; toutiao_sso_user_ss=fbb34bd50183e9071f4787cfd53b9b2c; sid_ucp_sso_v1=1.0.0-KGM2OGMxNjYyYWY1OTcxNGE5NzFmNTk5NDY1MTFjNGI0MTkzZmUwYTQKIAjEouCdmfQ5EPWS3LUGGO8xIAww1p3l8AU4AkDvB0gGGgJobCIgZmJiMzRiZDUwMTgzZTkwNzFmNDc4N2NmZDUzYjliMmM; ssid_ucp_sso_v1=1.0.0-KGM2OGMxNjYyYWY1OTcxNGE5NzFmNTk5NDY1MTFjNGI0MTkzZmUwYTQKIAjEouCdmfQ5EPWS3LUGGO8xIAww1p3l8AU4AkDvB0gGGgJobCIgZmJiMzRiZDUwMTgzZTkwNzFmNDc4N2NmZDUzYjliMmM; uid_tt=2b631745fc5d041c67dc0a8ffb0a2934; uid_tt_ss=2b631745fc5d041c67dc0a8ffb0a2934; sid_tt=4f1178dd046fcfdf123d9873c21fff1c; sessionid=4f1178dd046fcfdf123d9873c21fff1c; sessionid_ss=4f1178dd046fcfdf123d9873c21fff1c; _bd_ticket_crypt_cookie=ea0aca8e2fe74d7fbb5cccff4fb8b1d7; __live_version__=%221.1.2.2711%22; hevc_supported=true; dy_swidth=1707; dy_sheight=960; SelfTabRedDotControl=%5B%5D; volume_info=%7B%22isUserMute%22%3Afalse%2C%22isMute%22%3Atrue%2C%22volume%22%3A0.6%7D; publish_badge_show_info=%220%2C0%2C0%2C1724933582101%22; download_guide=%223%2F20240829%2F0%22; pwa2=%220%7C0%7C3%7C0%22; sid_guard=4f1178dd046fcfdf123d9873c21fff1c%7C1725356351%7C5184000%7CSat%2C+02-Nov-2024+09%3A39%3A11+GMT; sid_ucp_v1=1.0.0-KDI5NGNlY2JjMjQ1YmJmZDA3ZTUzMGUyMTNjMTg5ZmYwNDk3YzBhOTEKGgjEouCdmfQ5EL-y27YGGO8xIAw4AkDvB0gEGgJobCIgNGYxMTc4ZGQwNDZmY2ZkZjEyM2Q5ODczYzIxZmZmMWM; ssid_ucp_v1=1.0.0-KDI5NGNlY2JjMjQ1YmJmZDA3ZTUzMGUyMTNjMTg5ZmYwNDk3YzBhOTEKGgjEouCdmfQ5EL-y27YGGO8xIAw4AkDvB0gEGgJobCIgNGYxMTc4ZGQwNDZmY2ZkZjEyM2Q5ODczYzIxZmZmMWM; WallpaperGuide=%7B%22showTime%22%3A1725356366826%2C%22closeTime%22%3A0%2C%22showCount%22%3A1%2C%22cursor1%22%3A12%2C%22cursor2%22%3A0%7D; douyin.com; xg_device_score=7.983585126412278; device_web_cpu_core=20; device_web_memory_size=8; architecture=amd64; stream_recommend_feed_params=%22%7B%5C%22cookie_enabled%5C%22%3Atrue%2C%5C%22screen_width%5C%22%3A1707%2C%5C%22screen_height%5C%22%3A960%2C%5C%22browser_online%5C%22%3Atrue%2C%5C%22cpu_core_num%5C%22%3A20%2C%5C%22device_memory%5C%22%3A8%2C%5C%22downlink%5C%22%3A10%2C%5C%22effective_type%5C%22%3A%5C%224g%5C%22%2C%5C%22round_trip_time%5C%22%3A50%7D%22; strategyABtestKey=%221725441549.442%22; csrf_session_id=8cd327bea9727e899ec565e22ce4471a; biz_trace_id=c5df32d0; stream_player_status_params=%22%7B%5C%22is_auto_play%5C%22%3A0%2C%5C%22is_full_screen%5C%22%3A0%2C%5C%22is_full_webscreen%5C%22%3A0%2C%5C%22is_mute%5C%22%3A1%2C%5C%22is_speed%5C%22%3A1%2C%5C%22is_visible%5C%22%3A0%7D%22; __ac_nonce=066d8261400e2f5181c1a; __ac_signature=_02B4Z6wo00f01W1xDGgAAIDAsQiT3iL8YNVtUQjAAD2I6b; IsDouyinActive=false; home_can_add_dy_2_desktop=%221%22; bd_ticket_guard_client_data=eyJiZC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwiYmQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJiZC10aWNrZXQtZ3VhcmQtcmVlLXB1YmxpYy1rZXkiOiJCRmZ0VW5yWEE2MDc1dkFUN3BhNXppQjBnajk1MTBIWitHMjZjU08zTHlFdjduZWkwYlYxTEJmWHJHc1lteElQaVFBdGNJcXFLMGhCbFpiL1FrVEZ3RlU9IiwiYmQtdGlja2V0LWd1YXJkLXdlYi12ZXJzaW9uIjoxfQ%3D%3D; odin_tt=ec7031dadb0fc1db2619642d3b41763498737731c4b56e617c33d2bc6c8518d50064c31e0d6f729d34118488be0a5488; passport_fe_beating_status=false'

    crawler = DouyinCommentCrawler()
    crawler.set(keyword=keyword, save_path=save_path, cookie=cookie)
    asyncio.run(crawler.run())

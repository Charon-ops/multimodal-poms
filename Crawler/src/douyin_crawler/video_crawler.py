import asyncio
import aiofiles
from playwright.async_api import async_playwright, BrowserContext
from typing import Dict, List
from lib.utils import get_time, get_data, get_headers, load_config, check_path, save_to_csv, validate_or_default

class DouyinVideoCrawler:
    def __init__(self):
        self.headers: Dict
        self.keyword: str
        self.save_path: str
        self.columns: List
        self.context: BrowserContext
        self.video_count: int = 0
        self.data_accumulator: List = []


    def set(self, keyword: str, save_path: str, cookie: str):
        if not keyword or not save_path or not cookie:
            self.keyword = self.save_path = self.headers = None
            return
        self.keyword = keyword
        self.save_path = save_path + f'/douyin/{keyword}'
        self.headers = get_headers(cookie, 'dy')

        self.columns = ["用户名", "粉丝数量", "发表时间", "视频描述", "视频链接", "视频时长", "点赞数量", "评论数量", "收藏数量", "下载数量", "分享数量"]

    async def save_video_info(self, video_data: Dict, aweme_id: str) -> None:
        try:
            author = video_data.get('author', {})
            user_name = author.get('nickname', '未知').strip()
            follower_count = author.get('follower_count', 0)

            create_time = get_time(video_data.get('create_time', 0))
            video_desc = video_data.get('desc', '').strip().replace('\n', '')

            video_link = f"https://www.douyin.com/video/{aweme_id}" if aweme_id else '视频ID缺失，无法生成视频链接'

            duration = video_data.get('video', {}).get('duration', 0)
            minutes = duration // 1000 // 60
            seconds = duration // 1000 % 60

            statistics = video_data.get('statistics', {})
            digg_count = statistics.get('digg_count', 0)
            collect_count = statistics.get('collect_count', 0)
            comment_count = statistics.get('comment_count', 0)
            download_count = statistics.get('download_count', 0)
            share_count = statistics.get('share_count', 0)

            video_dict = {
                "用户名": user_name,
                "粉丝数量": follower_count,
                "发表时间": create_time,
                "视频描述": video_desc,
                "视频链接": video_link,
                "视频时长": "{:02d}:{:02d}".format(minutes, seconds),
                "点赞数量": digg_count,
                "收藏数量": collect_count,
                "评论数量": comment_count,
                "下载数量": download_count,
                "分享数量": share_count
            }

            self.video_count += 1

            print(f"当前视频数量: {self.video_count}\n",
                f"用户名：{video_dict['用户名']}\n",
                f"粉丝数量：{video_dict['粉丝数量']}\n",
                f"发表时间：{video_dict['发表时间']}\n",
                f"视频描述：{video_dict['视频描述']}\n",
                f"视频链接：{video_dict['视频链接']}\n",
                f"视频时长：{video_dict['视频时长']}\n",
                f"点赞数量：{video_dict['点赞数量']}\n",
                f"评论数量：{video_dict['评论数量']}\n",
                f"收藏数量：{video_dict['收藏数量']}\n",
                f"下载数量：{video_dict['下载数量']}\n",
                f"分享数量：{video_dict['分享数量']}\n"
                )

            self.data_accumulator.append(video_dict)
            if len(self.data_accumulator) >= 50:
                file_name = f"{self.save_path}/{self.keyword}_videos.csv"
                await save_to_csv(self.data_accumulator, file_name, self.columns)
                self.data_accumulator = []

        except Exception as e:
            print(f"处理视频数据时出错: {e}")

    async def save_video(self, video_data: Dict, aweme_id: str) -> None:
        try:
            video_url = video_data.get('video', {}).get('download_addr', {}).get('url_list', [])
            if not video_url:
                print("视频下载地址不存在，跳过该视频")
                return
            video_url = video_url[-1]
            await asyncio.sleep(4)
            try:
                response = await get_data(context=self.context, url=video_url, headers=self.headers, type='src')
                video_content = await response.body()
            except Exception as e:
                print(f"下载视频失败 (aweme_id: {aweme_id}, url: {video_url}): {e}")
                return

            video_path = f'{self.save_path}/{aweme_id}'
            check_path(video_path)
            video_file_path = f'{video_path}/{aweme_id}.mp4'
            try:
                async with aiofiles.open(video_file_path, 'wb') as file:
                    await file.write(video_content)
                print(f"视频保存成功: {video_file_path}")
            except Exception as e:
                print(f"保存视频文件时出错 (aweme_id: {aweme_id}): {e}")
        except Exception as e:
            print(f"处理视频数据时出错: {e}")

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
                    print(f"未获取到有效的视频数据(offset: {offset})")
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
                            aweme_id = aweme_info.get('aweme_id').strip()
                            if not aweme_id:
                                print("视频ID缺失，跳过该条数据")
                                continue
                        else:
                            print(f"无效的视频信息数据类型：{aweme_info}")
                            continue
                        await self.save_video_info(aweme_info, aweme_id)
                        await self.save_video(aweme_info, aweme_id)
                    except Exception as e:
                        print(f"处理视频数据时出错: {e}")
                if json_data.get('has_more') == 0:
                    break
                offset += count
                count = 10
            except Exception as e:
                print(f"获取关键词搜索数据时出错 (offset: {offset}): {e}")
                offset += count
                count = 10
                continue
        if self.data_accumulator:
            try:
                await save_to_csv(self.data_accumulator, f"{self.save_path}/{self.keyword}_videos.csv", self.columns)
                self.data_accumulator = []
            except Exception as e:
                print(f"保存评论数据到CSV时出错: {e}")

    async def run(self) -> None:
        try:
            if not self.keyword or not self.save_path or not self.headers:
                print("请先设置关键字、保存路径和请求头。")
                return
            check_path(f'{self.save_path}')
            try:
                with open(f"{self.save_path}/{self.keyword}_videos.csv", "w", encoding="utf-8-sig", newline="") as f:
                    pass
            except Exception as e:
                print(f"创建文件{self.save_path}/{self.keyword}_videos.csv时出错: {e}")
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

    cookie = 'ttwid=1%7CiFrIbo5zacI4ggwR_lLWAeCSUmLqkBuXBX_qe8zBXoU%7C1722161376%7Ccba5a9e8bc0bb928681e50aa867d5b1e80d0ae5c0a129476bfb25031a2d36482; UIFID_TEMP=3c3e9d4a635845249e00419877a3730e2149197a63ddb1d8525033ea2b3354c200afd64d8c1da1ce41b1ee1bbb5f209dc6251ef96e5c67a33ee8bd729f6745cb5de701d949d74ff9264d56ea102b354e8af69aa3db82add4e64dada7f96d037c89e2362e6188ae17dc96c9eb19ac23b5; dy_swidth=1707; dy_sheight=960; fpk1=U2FsdGVkX18w6wg2y3GHFgRlPqr9PVTbe7Ec+iNqMr3KTqQE5cudpn9AmSAf5Gr2b+7+7ZbhU7XiDE1D6/TWaw==; fpk2=f1f6b29a6cc1f79a0fea05b885aa33d0; s_v_web_id=verify_lz5ef986_YtGvWi74_5grO_4C6Y_9uRD_nknO19CQX3E8; xgplayer_user_id=986392702227; passport_csrf_token=019173788f6bd671f34676b4620d19d2; passport_csrf_token_default=019173788f6bd671f34676b4620d19d2; bd_ticket_guard_client_web_domain=2; UIFID=3c3e9d4a635845249e00419877a3730e2149197a63ddb1d8525033ea2b3354c200afd64d8c1da1ce41b1ee1bbb5f209dc6251ef96e5c67a33ee8bd729f6745cb9c4d53ba7684c5d32c87d18c636e9cb41413e1dd3b742ea271cb21a96706b88b1f7bc42f45d10ebd560b036c3ad13331b2da0b3994918adb0761cfbfc9de149b2a0b8ae294f2c4fe944c3aa41928019aa2eb335a7d40698c8e3eddb7627f42e1121a35df8003c96abe71f8040ddff4cbe0b6c5493baaf7618018460655e2c133; live_use_vvc=%22false%22; FORCE_LOGIN=%7B%22videoConsumedRemainSeconds%22%3A180%2C%22isForcePopClose%22%3A1%7D; SEARCH_RESULT_LIST_TYPE=%22single%22; d_ticket=4837a1f785bad649ed04dcc018b52541d1a81; n_mh=9QQeuVUxM4mSMdUxjWaD4Jt2f1UXjlKvsPxugBOFshw; sso_auth_status=72f222ff4ada49b49a8d07fb936f8509; sso_auth_status_ss=72f222ff4ada49b49a8d07fb936f8509; _bd_ticket_crypt_doamin=2; __security_server_data_status=1; store-region=cn-jl; store-region-src=uid; is_staff_user=false; my_rd=2; passport_auth_status=5a552fab2ee073e90c03a5350df21ca5%2Ccba223b3a04022fb0ef128d0609fcf5d; passport_auth_status_ss=5a552fab2ee073e90c03a5350df21ca5%2Ccba223b3a04022fb0ef128d0609fcf5d; strategyABtestKey=%221723266438.532%22; publish_badge_show_info=%220%2C0%2C0%2C1723266443099%22; xgplayer_device_id=92915748857; volume_info=%7B%22isUserMute%22%3Afalse%2C%22isMute%22%3Atrue%2C%22volume%22%3A0.6%7D; stream_player_status_params=%22%7B%5C%22is_auto_play%5C%22%3A0%2C%5C%22is_full_screen%5C%22%3A0%2C%5C%22is_full_webscreen%5C%22%3A0%2C%5C%22is_mute%5C%22%3A1%2C%5C%22is_speed%5C%22%3A1%2C%5C%22is_visible%5C%22%3A1%7D%22; download_guide=%223%2F20240810%2F0%22; csrf_session_id=83e9cec3d90e17cd9efcfd09100bf98a; biz_trace_id=543f0a58; passport_assist_user=CkBKO39GjbcQt2Mf5b0x35LoEcUwfrO44SPO3_QWeomfmqyY82xbeFSfNIUMvyb8ui7cIK5q2vM6lfkaVPlVUV-3GkoKPL_jn8O2VQ6UP1Fv_sormgplo_6K6kylSizvTwPJgj7XXZXa5oXPipOtOQ8ahINEPTyM-4Vsygjhb0xdGxDZ_9gNGImv1lQgASIBA7LhWVY%3D; sso_uid_tt=9238686f1526b23e2f7f8f6452014bde; sso_uid_tt_ss=9238686f1526b23e2f7f8f6452014bde; toutiao_sso_user=fbb34bd50183e9071f4787cfd53b9b2c; toutiao_sso_user_ss=fbb34bd50183e9071f4787cfd53b9b2c; sid_ucp_sso_v1=1.0.0-KGM2OGMxNjYyYWY1OTcxNGE5NzFmNTk5NDY1MTFjNGI0MTkzZmUwYTQKIAjEouCdmfQ5EPWS3LUGGO8xIAww1p3l8AU4AkDvB0gGGgJobCIgZmJiMzRiZDUwMTgzZTkwNzFmNDc4N2NmZDUzYjliMmM; ssid_ucp_sso_v1=1.0.0-KGM2OGMxNjYyYWY1OTcxNGE5NzFmNTk5NDY1MTFjNGI0MTkzZmUwYTQKIAjEouCdmfQ5EPWS3LUGGO8xIAww1p3l8AU4AkDvB0gGGgJobCIgZmJiMzRiZDUwMTgzZTkwNzFmNDc4N2NmZDUzYjliMmM; uid_tt=2b631745fc5d041c67dc0a8ffb0a2934; uid_tt_ss=2b631745fc5d041c67dc0a8ffb0a2934; sid_tt=4f1178dd046fcfdf123d9873c21fff1c; sessionid=4f1178dd046fcfdf123d9873c21fff1c; sessionid_ss=4f1178dd046fcfdf123d9873c21fff1c; _bd_ticket_crypt_cookie=ea0aca8e2fe74d7fbb5cccff4fb8b1d7; sid_guard=4f1178dd046fcfdf123d9873c21fff1c%7C1723271545%7C5183999%7CWed%2C+09-Oct-2024+06%3A32%3A24+GMT; sid_ucp_v1=1.0.0-KGU5OGJiMDcxZmIzNzBkMTBlMzUwNjUxZjE2YTE5OTBlNzkwMWY4ZGYKGgjEouCdmfQ5EPmS3LUGGO8xIAw4AkDvB0gEGgJobCIgNGYxMTc4ZGQwNDZmY2ZkZjEyM2Q5ODczYzIxZmZmMWM; ssid_ucp_v1=1.0.0-KGU5OGJiMDcxZmIzNzBkMTBlMzUwNjUxZjE2YTE5OTBlNzkwMWY4ZGYKGgjEouCdmfQ5EPmS3LUGGO8xIAw4AkDvB0gEGgJobCIgNGYxMTc4ZGQwNDZmY2ZkZjEyM2Q5ODczYzIxZmZmMWM; pwa2=%220%7C0%7C3%7C0%22; __live_version__=%221.1.2.2711%22; webcast_local_quality=null; live_can_add_dy_2_desktop=%220%22; stream_recommend_feed_params=%22%7B%5C%22cookie_enabled%5C%22%3Atrue%2C%5C%22screen_width%5C%22%3A1707%2C%5C%22screen_height%5C%22%3A960%2C%5C%22browser_online%5C%22%3Atrue%2C%5C%22cpu_core_num%5C%22%3A20%2C%5C%22device_memory%5C%22%3A8%2C%5C%22downlink%5C%22%3A10%2C%5C%22effective_type%5C%22%3A%5C%224g%5C%22%2C%5C%22round_trip_time%5C%22%3A50%7D%22; home_can_add_dy_2_desktop=%221%22; bd_ticket_guard_client_data=eyJiZC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwiYmQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJiZC10aWNrZXQtZ3VhcmQtcmVlLXB1YmxpYy1rZXkiOiJCRmZ0VW5yWEE2MDc1dkFUN3BhNXppQjBnajk1MTBIWitHMjZjU08zTHlFdjduZWkwYlYxTEJmWHJHc1lteElQaVFBdGNJcXFLMGhCbFpiL1FrVEZ3RlU9IiwiYmQtdGlja2V0LWd1YXJkLXdlYi12ZXJzaW9uIjoxfQ%3D%3D; odin_tt=a87e86db22068aa9b1feb37220c56114545586bcd6a01f76ac0f7b9597d23549beba8917b6cbf85bd40cc795dbbb6450; WallpaperGuide=%7B%22showTime%22%3A1723266497064%2C%22closeTime%22%3A0%2C%22showCount%22%3A3%2C%22cursor1%22%3A106%2C%22cursor2%22%3A0%2C%22hoverTime%22%3A1722180561085%7D; passport_fe_beating_status=true; IsDouyinActive=false; __ac_nonce=066b74337001142102e7e; __ac_signature=_02B4Z6wo00f014dJTqAAAIDCWzDRFdUXJo-HaUoAAIdtee; __ac_referer=https://www.douyin.com/search/%E5%BC%A0%E6%9C%AC%E6%99%BA%E5%92%8C%E5%8F%91%E6%96%87%E5%9B%9E%E5%BA%94%E8%B4%A5%E7%BB%99%E6%A8%8A%E6%8C%AF%E4%B8%9C?type=video'

    crawler = DouyinVideoCrawler()
    crawler.set(keyword=keyword, save_path=save_path, cookie=cookie)
    asyncio.run(crawler.run())

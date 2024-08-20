import asyncio
import json
from typing import List, Dict
from playwright.async_api import async_playwright, BrowserContext
from lib.utils import get_headers, post_data, check_path, save_to_csv

class KuaishouUserCrawler:
    def __init__(self):
        self.headers: Dict
        self.keyword: str
        self.save_path: str
        self.user_count: int = 0
        self.context: BrowserContext
        self.data_accumulator: List = []
        self.columns: List

    def set(self, keyword: str, save_path: str, cookie: str):
        self.keyword = keyword
        self.save_path = save_path + f'/kuaishou/{keyword}'
        self.headers = get_headers(cookie, 'ks')
        self.headers['referer'] = ('https://www.kuaishou.com/search/video?searchKey=' + self.keyword).encode('utf-8').decode('latin1')
        self.columns = ["用户名", "用户主页", "性别", "作品数", "粉丝数", "关注数", "描述"]

    async def get_author_info(self, author_id):
        headers = self.headers
        headers['referer'] = ('https://www.kuaishou.com/profile/'+ author_id).encode('utf-8').decode('latin1'),
        data={
            'operationName': "visionProfile",
            'query': "query visionProfile($userId: String) {\n  visionProfile(userId: $userId) {\n    result\n    hostName\n    userProfile {\n      ownerCount {\n        fan\n        photo\n        follow\n        photo_public\n        __typename\n      }\n      profile {\n        gender\n        user_name\n        user_id\n        headurl\n        user_text\n        user_profile_bg_url\n        __typename\n      }\n      isFollowing\n      __typename\n    }\n    __typename\n  }\n}\n",
            'variables': {'userId': author_id}
        }
        data=json.dumps(data)
        json_data = await post_data(self.context, url='https://www.kuaishou.com/graphql', headers=headers, data=data)
        try:
            user_profile_data = json_data['data']['visionProfile']['userProfile']
            if user_profile_data == []:
                return None
            else:
                return  user_profile_data
        except:
            return None

    async def get_video_list(self, pcursor):
        data = {
            'operationName': "visionSearchPhoto",
            'query': "fragment photoContent on PhotoEntity {\n  __typename\n  id\n  duration\n  caption\n  originCaption\n  likeCount\n  viewCount\n  commentCount\n  realLikeCount\n  coverUrl\n  photoUrl\n  photoH265Url\n  manifest\n  manifestH265\n  videoResource\n  coverUrls {\n    url\n    __typename\n  }\n  timestamp\n  expTag\n  animatedCoverUrl\n  distance\n  videoRatio\n  liked\n  stereoType\n  profileUserTopPhoto\n  musicBlocked\n  riskTagContent\n  riskTagUrl\n}\n\nfragment recoPhotoFragment on recoPhotoEntity {\n  __typename\n  id\n  duration\n  caption\n  originCaption\n  likeCount\n  viewCount\n  commentCount\n  realLikeCount\n  coverUrl\n  photoUrl\n  photoH265Url\n  manifest\n  manifestH265\n  videoResource\n  coverUrls {\n    url\n    __typename\n  }\n  timestamp\n  expTag\n  animatedCoverUrl\n  distance\n  videoRatio\n  liked\n  stereoType\n  profileUserTopPhoto\n  musicBlocked\n  riskTagContent\n  riskTagUrl\n}\n\nfragment feedContent on Feed {\n  type\n  author {\n    id\n    name\n    headerUrl\n    following\n    headerUrls {\n      url\n      __typename\n    }\n    __typename\n  }\n  photo {\n    ...photoContent\n    ...recoPhotoFragment\n    __typename\n  }\n  canAddComment\n  llsid\n  status\n  currentPcursor\n  tags {\n    type\n    name\n    __typename\n  }\n  __typename\n}\n\nquery visionSearchPhoto($keyword: String, $pcursor: String, $searchSessionId: String, $page: String, $webPageArea: String) {\n  visionSearchPhoto(keyword: $keyword, pcursor: $pcursor, searchSessionId: $searchSessionId, page: $page, webPageArea: $webPageArea) {\n    result\n    llsid\n    webPageArea\n    feeds {\n      ...feedContent\n      __typename\n    }\n    searchSessionId\n    pcursor\n    aladdinBanner {\n      imgUrl\n      link\n      __typename\n    }\n    __typename\n  }\n}\n",
            'variables': {'keyword': self.keyword, 'pcursor': pcursor, 'page': "search"}
        }
        data = json.dumps(data)

        json_data = await post_data(self.context, url='https://www.kuaishou.com/graphql', headers=self.headers, data=data)
        try:
            vision_search_photo = json_data['data']['visionSearchPhoto']['feeds']
            pcursor = json_data['data']['visionSearchPhoto']['pcursor']
            if vision_search_photo == []:
                return None
            else:
                return vision_search_photo, pcursor
        except:
            return None

    async def save_user_data(self, user_data: Dict, author_id: str) -> None:
        user_data_dict = {
            "用户名": user_data['profile']['user_name'].strip(),
            "用户主页": f"https://www.kuaishou.com/profile/{author_id}",
            "性别": '男' if user_data['profile']['gender'] == 'M' else '女' if user_data['gender'] == 'F' else '未知',
            "作品数": user_data['ownerCount']['photo_public'],
            "粉丝数": user_data['ownerCount']['fan'],
            "关注数": user_data['ownerCount']['follow'],
            "描述": user_data['profile']['user_text'].strip().replace('\n', ''),
        }

        self.user_count += 1

        print(f"当前用户数量: {self.user_count}\n",
          f"用户名：{user_data_dict['用户名']}\n",
          f"用户主页：{user_data_dict['用户主页']}\n",
          f"性别：{user_data_dict['性别']}\n",
          f"作品数：{user_data_dict['作品数']}\n",
          f"粉丝数：{user_data_dict['粉丝数']}\n",
          f"关注数：{user_data_dict['关注数']}\n",
          f"描述：{user_data_dict['描述']}\n"
          )

        self.data_accumulator.append(user_data_dict)
        if len(self.data_accumulator) >= 30:
            file_name = f'{self.save_path}/{self.keyword}_user_infos.csv'
            save_to_csv(user_data_dict, file_name, self.columns)

    async def fetch(self, start_pcursor: str='') -> None:
        pcursor = start_pcursor
        while True:
            json_data, pcursor = await self.get_video_list(pcursor)
            if json_data == None or pcursor == 'no_more':
                break
            for video_data in json_data:
                author_id = video_data['author']['id']
                author_data = self.get_author_info(author_id)
                if author_data is not None:
                    self.save_user_data(author_data, author_id)
        if self.data_accumulator:
            save_to_csv(self.data_accumulator, f'{self.save_path}/{self.keyword}_user_infos.csv', self.columns)
            self.data_accumulator = []

    async def run(self) -> None:
        if not self.keyword:
            print("run after set")
            return
        check_path(f'{self.save_path}')
        with open(f"{self.save_path}/{self.keyword}_user_infos.csv", "w", encoding="utf-8-sig", newline="") as f:
            pass
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            self.context = await browser.new_context()
            await self.fetch()
            await browser.close()

if __name__ == '__main__':
    keyword = '我和我的祖国'
    save_path='./data'

    cookie = 'did=web_00f5e82e3707d01aa1da68b7dcebb92e; didv=1723028408260; kpf=PC_WEB; clientid=3; userId=2916759763; kuaishou.server.web_st=ChZrdWFpc2hvdS5zZXJ2ZXIud2ViLnN0EqAB19BSpMicCIEEDCgHDo3nTC41k37lSty4Sg307g_2tLu15fUgSBKei4Nn1iDiNFc8X4lnz4y_1xgC8g-bSRAKBZJcHcLHnBiIVWweTYb9k2bQNE10HIpP1pOPn1DSfXdkZ4naGiPSsPfvBOfmq6Gr7tiL-VarLDaLnX-9pHsn8YL52uOdxCqy4wUmkByVhnVbuoAi52iuXcgg82GdWuvrdRoS7YoRGiN2PM_7zCD1Dj9m5oYoIiB6geeuyUNbectt96g-_L_iloJGNX0EE4XlOE0HSSfV8ygFMAE; kuaishou.server.web_ph=3b4ba4ee8ac82ef9eb6a6c602d6b3d72324e; kpn=KUAISHOU_VISION'

    crawler = KuaishouUserCrawler()
    crawler.set(keyword=keyword, save_path=save_path, cookie=cookie)
    asyncio.run(crawler.run())

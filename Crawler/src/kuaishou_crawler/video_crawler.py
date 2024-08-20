import asyncio
import json
from typing import List, Dict
from playwright.async_api import async_playwright, BrowserContext
from lib.utils import get_headers, post_data, check_path, save_to_csv

class KuaishouVideoCrawler:
    def __init__(self):
        self.headers: Dict
        self.keyword: str
        self.save_path: str
        self.columns: List
        self.context: BrowserContext
        self.video_count: int = 0
        self.data_accumulator: List = []


    def set(self, keyword: str, save_path: str, cookie: str):
        self.keyword = keyword
        self.save_path = save_path + f'/kuaishou/{keyword}'
        self.headers = get_headers(cookie, 'ks')
        self.headers['referer'] = ('https://www.kuaishou.com/search/video?searchKey=' + self.keyword).encode('utf-8').decode('latin1')
        self.columns = ['视频标题', '视频点赞数', '视频浏览数', '视频时长', '视频链接', '视频作者']

    async def save_video(self, video_data: Dict) -> None:
        video_url = video_data['photo']['photoUrl']
        video_id = video_data['photo']['id']
        await asyncio.sleep(4)

        response = await self.context.request.get(video_url, headers=self.headers)
        video_content = await response.body()
        check_path(f'{self.save_path}/{video_id}')
        with open(f'{self.save_path}/{video_id}/{video_id}.mp4', 'wb') as file:
            file.write(video_content)

    async def save_video_info(self, video_data: Dict) -> None:
        minutes = video_data['photo']['duration'] // 1000 // 60
        seconds = video_data['photo']['duration'] // 1000 % 60

        video_dict = {
            "视频标题": video_data['photo']['caption'].strip(),
            "视频点赞数": video_data['photo']['likeCount'],
            "视频浏览数": video_data['photo']['viewCount'],
            "视频时长": "{:02d}:{:02d}".format(minutes, seconds),
            "视频链接": f"https://www.kuaishou.com/short-video/{video_data['photo']['id'].strip()}",
            "视频作者": video_data['author']['name'],
        }

        self.video_count += 1

        print(f"当前视频数量: {self.video_count}\n",
            f"视频标题：{video_dict['视频标题']}\n",
            f"视频点赞数：{video_dict['视频点赞数']}\n",
            f"视频浏览数：{video_dict['视频浏览数']}\n",
            f"视频时长：{video_dict['视频时长']}\n",
            f"视频链接：{video_dict['视频链接']}\n",
            f"视频作者：{video_dict['视频作者']}\n"
            )

        self.data_accumulator.append(video_dict)
        if len(self.data_accumulator) >= 30:
            file_name = f"{self.save_path}/{self.keyword}_videos.csv"
            save_to_csv(self.data_accumulator, file_name, self.columns)
            self.data_accumulator = []

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

    async def fetch(self, start_pcursor: str='') -> None:
        pcursor = start_pcursor
        while True:
            json_data, pcursor = await self.get_video_list(pcursor)
            if json_data == None or pcursor == 'no_more':
                break
            for video_data in json_data:
                await self.save_video_info(video_data)
                await self.save_video(video_data)

    async def run(self) -> None:
        if not self.keyword:
            print("run after set")
            return
        check_path(f'{self.save_path}')
        with open(f"{self.save_path}/{self.keyword}_videos.csv", "w", encoding="utf-8-sig", newline="") as f:
            pass
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            self.context = await browser.new_context()
            await self.fetch()
            await browser.close()
        if self.data_accumulator:
            save_to_csv(self.data_accumulator, f"{self.save_path}/{self.keyword}_videos.csv", self.columns)
            self.data_accumulator = []

if __name__ == '__main__':
    keyword = '我和我的祖国'
    save_path='./data'

    cookie = 'did=web_00f5e82e3707d01aa1da68b7dcebb92e; didv=1723028408260; kpf=PC_WEB; clientid=3; userId=2916759763; kpn=KUAISHOU_VISION; kuaishou.server.web_st=ChZrdWFpc2hvdS5zZXJ2ZXIud2ViLnN0EqABRoljcBOSF14_7Pr0C_x7wOCPaRCEHyMDPyRp0bX6B6uI35vhKDBDkipZtg-Idc5KQCr9-iWK6F2yDPnqtqGm2tD764AFeY3nWyUS4V-Lm5x412xiun0vOCRaiT4prdzucQJIgfAwl7TXV2iORY_V3CUshG3flnJSZNLV2Z6E67yEDiSI-abMzTFtuGWuEgjoP7K4DspVZ8oh92Efi93K5RoSTdCMiCqspRXB3AhuFugv61B-IiBMeidLMfKT8vkAfnlfdgRwK-g0B3wjAfQrGxrwYdOxGSgFMAE; kuaishou.server.web_ph=ad86d180ba9d173379861d86c031e78c6679',

    crawler = KuaishouVideoCrawler()
    crawler.set(keyword=keyword, save_path=save_path, cookie=cookie)
    asyncio.run(crawler.run())

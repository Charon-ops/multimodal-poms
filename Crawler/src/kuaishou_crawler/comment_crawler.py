import asyncio
import json
from typing import Dict, List
from playwright.async_api import async_playwright, BrowserContext
from lib.utils import get_time, get_headers, post_data, check_path, save_to_csv

class KuaishouCommentCrawler:
    def __init__(self):
        self.headers: Dict
        self.keyword: str
        self.save_path: str
        self.video_count: int = 0
        self.comment_count: int = 0
        self.context: BrowserContext
        self.data_accumulator: List = []
        self.columns: List


    def set(self, keyword: str, save_path: str, cookie: str):
        self.keyword = keyword
        self.save_path = save_path + f'/kuaishou/{keyword}'
        self.headers = get_headers(cookie, 'ks')
        self.columns = ['评论作者', '视频链接', '评论内容', '评论时间', '评论点赞数']

    async def save_comment_data(self, comment: Dict, video_id: str) -> None:
        comment_data_dict = {
            "评论作者": comment['authorName'],
            "视频链接": f"https://www.kuaishou.com/short-video/{video_id}",
            "评论内容": comment['content'].strip().replace('\n', ''),
            "评论时间": get_time(comment['timestamp']),
            "评论点赞数": comment['likedCount'],
        }

        self.comment_count += 1

        print(f"当前评论数: {self.comment_count}\n",
          f"评论作者：{comment_data_dict['评论作者']}\n",
          f"视频链接：{comment_data_dict['视频链接']}\n",
          f"评论内容：{comment_data_dict['评论内容']}\n",
          f"评论时间：{comment_data_dict['评论时间']}\n",
          f"评论点赞数：{comment_data_dict['评论点赞数']}\n"
          )

        self.data_accumulator.append(comment_data_dict)
        if len(self.data_accumulator) >= 30:
            file_name = f'{self.save_path}/{self.keyword}_comment.csv'
            save_to_csv(self.data_accumulator, file_name, self.columns)
            file_name = f'{self.save_path}/{video_id}/{video_id}_comment.csv'
            save_to_csv(self.data_accumulator, file_name, self.columns)
            self.data_accumulator = []

    async def get_comments_list(self, video_id):
        headers = self.headers
        headers['referer'] = ('https://www.kuaishou.com/search/video/'+ video_id).encode('utf-8').decode('latin1'),
        data={
            'operationName': "commentListQuery",
            'query': "query commentListQuery($photoId: String, $pcursor: String) {\n  visionCommentList(photoId: $photoId, pcursor: $pcursor) {\n    commentCount\n    pcursor\n    rootComments {\n      commentId\n      authorId\n      authorName\n      content\n      headurl\n      timestamp\n      likedCount\n      realLikedCount\n      liked\n      status\n      authorLiked\n      subCommentCount\n      subCommentsPcursor\n      subComments {\n        commentId\n        authorId\n        authorName\n        content\n        headurl\n        timestamp\n        likedCount\n        realLikedCount\n        liked\n        status\n        authorLiked\n        replyToUserName\n        replyTo\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n",
            'variables': {'photoId': video_id, 'pcursor':""}
        }
        data=json.dumps(data)
        json_data = await post_data(self.context, url='https://www.kuaishou.com/graphql', headers=headers, data=data)
        try:
            vision_comment_list = json_data['data']['visionCommentList']['rootComments']
            if vision_comment_list == []:
                return None
            else:
                return  vision_comment_list
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

    async def fetch(self, start_pcursor: str='') -> None:
        pcursor = start_pcursor
        while True:
            json_data, pcursor = await self.get_video_list(pcursor)
            if json_data == None or pcursor == 'no_more':
                break
            for video_data in json_data:
                photo_caption = video_data['photo']['caption'].strip().replace('\n', '')
                print(f"开始爬取 {photo_caption} 的评论\n")
                video_id = video_data['photo']['id']
                check_path(f'{self.save_path}/{video_id}')
                with open(f'{self.save_path}/{video_id}/{video_id}_comment.csv', 'wb') as file:
                    pass
                root_comments_list = self.get_comments_list(video_id)
                if root_comments_list is not None:
                    for comment in root_comments_list:
                        await self.save_comment_data(comment, video_id)
                if self.data_accumulator:
                    save_to_csv(self.data_accumulator, f'{self.save_path}/{self.keyword}_comment.csv', self.columns)
                    save_to_csv(self.data_accumulator, f'{self.save_path}/{video_id}/{video_id}_comment.csv', self.columns)
                    self.data_accumulator = []
                photo_caption = video_data['photo']['caption'].strip().replace('\n', '')
                print(f"{photo_caption} 的评论爬取完毕\n")
                await asyncio.sleep(1)
                self.video_count += 1

    async def run(self) -> None:
        if not self.keyword:
            print("run after set")
            return
        check_path(f'{self.save_path}')
        with open(f"{self.save_path}/{self.keyword}_comment.csv", "w", encoding="utf-8-sig", newline="") as f:
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

    crawler = KuaishouCommentCrawler()
    crawler.set(keyword=keyword, save_path=save_path, cookie=cookie)
    asyncio.run(crawler.run())

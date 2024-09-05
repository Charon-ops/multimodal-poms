from src import DouyinVideoCrawler, DouyinCommentCrawler, DouyinUserCrawler
from src import KuaishouVideoCrawler, KuaishouCommentCrawler, KuaishouUserCrawler
from src import WeiboTopicCrawler, WeiboCommentCrawler, WeiboForwardCrawler, WeiboUserCrawler

def create_crawler(platform: str, type: str):
    CRAWLERS = {
        'dy': {
            'video': DouyinVideoCrawler,
            'comment': DouyinCommentCrawler,
            'user': DouyinUserCrawler
        },
        'ks': {
            'video': KuaishouVideoCrawler,
            'comment': KuaishouCommentCrawler,
            'user': KuaishouUserCrawler
        },
        'wb': {
            'topic': WeiboTopicCrawler,
            'comment': WeiboCommentCrawler,
            'forward': WeiboForwardCrawler,
            'user': WeiboUserCrawler
        },
    }
    crawlers = CRAWLERS.get(platform)
    if not crawlers:
        raise ValueError('Invalid media platform.')
    crawler_name = crawlers.get(type)
    if not crawler_name:
        raise ValueError('Invalid crawler type.')
    return crawler_name()

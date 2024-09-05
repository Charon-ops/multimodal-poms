import os
from lib.crawler import create_crawler
import asyncio

if __name__ == '__main__':
    keyword = '#普京指责瓦格纳负责人叛国#'
    platform='wb'
    type='comment'
    save_path = './data'

    cookie = 'SINAGLOBAL=297582777855.3297.1725279736947; SCF=AnhPUX6pvDkaDmbB0sQg2Km37UKBoUPG6C56xBHp-QHN0racFqMbrkeNn0qYficWJKYdnUOQzmxQ8NR1eN8Wb7Y.; SUB=_2A25L0d44DeRhGeFJ41MR-S_MzjmIHXVor1_wrDV8PUNbmtAGLRDekW9Nfx8MjS5k7lqLa3j2p78C2jN-Rhrz2uKe; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFvc6eMrwWd.Ac9w4zliKDH5NHD95QNS0npeh.peh-fWs4DqcjMi--NiK.Xi-2Ri--ciKnRi-zNS0MReK54eK5fSBtt; ALF=02_1727871848; XSRF-TOKEN=urPu9eJORvznCVyBq14KyrDu; WBPSESS=UikGJsvacztU-grkctUpELN38J51-k1SFhygDH5fFjGpxK22wwcd4H_2LsYHnEfh90MpdNfMeuEqPw9pNZwCp5SXS6McPKzVt2W_HDMi5i3hKArJxoIC6fqi5yZFFtMWyJsfe8EZATKQtjxUQVOufA==; _s_tentry=weibo.com; Apache=2113674278474.1426.1725496749483; ULV=1725496749486:2:2:2:2113674278474.1426.1725496749483:1725279736960'

    crawler = create_crawler(platform=platform, type=type)
    crawler.set(keyword=keyword, save_path=save_path, cookie=cookie)

    asyncio.run(crawler.run())

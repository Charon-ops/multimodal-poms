**项目代码结构：**
Crawlers
├── src
│ ├── douyin_crawler
│ │ ├── video_crawler.py
│ │ ├── comment_crawler.py
│ │ └── user_crawler.py
│ ├── kuaishou_crawler
│ │ ├── video_crawler.py
│ │ ├── comment_crawler.py
│ │ └── user_crawler.py
│ └── weibo_crawler
│ │ ├── topic_crawler.py
│ │ ├── comment_crawler.py
│ │ ├── forward_crawler.py
│ │ └── user_crawler.py
├── config
│ └── config.yaml
├── lib
│ ├── X-Bogus.js
│ └── utils.py
└── main.py

**使用说明：**

修改 main.py 代码中参数，运行 main.py 即可

keyword：关键词；

platform：获取数据来源平台，抖音（dy）或快手（ks）或微博（wb）；

type：获取数据形式，抖音与快手平台支持视频及视频信息获取（video）、评论获取（comment）、用户信息获取（user）；微博支持主题帖获取（topic）、评论获取（comment）、转发信息获取（forward）、用户信息获取（user）；

save_path：保存数据路径

cookie：网页 cookie，需登录，其中抖音平台还需要手动刷新至出现滑块验证码后获取的 cookie 才有效。

注：微博平台评论获取、转发信息获取需要在已有主题帖信息 csv 文件基础上使用，用户信息获取功能为在已有信息 csv 文件上新增用户信息列，在不指定已有信息 csv 文件时，默认需要在已有主题帖信息 csv 文件，或评论信息 csv 文件，或转发信息 csv 文件基础上使用。

单个爬虫类代码的调用范例在其文件中的主程序入口点处给出。

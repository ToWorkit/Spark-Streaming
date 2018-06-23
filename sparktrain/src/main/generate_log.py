#coding=UTF-8

import random
import time

# 访问路径
url_paths = [
  "class/112.html",
  "class/128.html",
  "class/145.html",
  "class/146.html",
  "class/131.html",
  "class/130.html",
  "learn/821",
  "course/list"
]

# ip
ip_slices = [132, 156, 124, 10, 29, 167, 143, 187, 30, 46, 55, 63, 72, 87, 98]

# 搜索引擎跳转过来的
http_referers = [
  "http://www.baidu.com/s?wd={query}",
  "https://www.sogou.com/web?query={query}",
  "http://cn.bing.com/search/q={query}",
  "http://www.baidu.com/s?wd={query}",
  "https://search.yahoo.com/search?q={query}",
]

# 搜索关键字
search_keyword = [
  "Spark SQL实战",
  "Hadoop基础",
  "Storm实战",
  "Spark Streaming实战",
  "大数据面试"
]

# 状态码
status_codes = ["200", "404", "500"]

# 随机路径
def sample_url():
  # sample(seq, n) 从序列seq中选择n个随机且独立的元素
  return random.sample(url_paths, 1)[0]

# 随机ip
def sample_ip():
  slice_ = random.sample(ip_slices, 4)
  return ".".join([str(item) for item in slice_])

# 随机referer
def sample_referer():
  # random.random()函数是这个模块中最常用的方法了，它会生成一个随机的浮点数，范围是在0.0~1.0之间。
  # random.uniform()正好弥补了上面函数的不足，它可以设定浮点数的范围，一个是上限，一个是下限。
  if random.uniform(0, 1) > 0.2:
    return "-"
  refer_str = random.sample(http_referers, 1)
  query_str = random.sample(search_keyword, 1)
  return refer_str[0].format(query = query_str[0])

# 随机状态码
def sample_status_code():
  return random.sample(status_codes, 1)[0]


# 日志生成
def generate_log(count = 10):
  # 时间戳
  time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

  # 写入文件
  # f = open("/root/data/access.log", "w+")

  while count >= 1:
    query_log = "{ip}\t{local_time}\t\"GET /{url} HTTP/1.1\"\t{status_code}\t{referer}".format(local_time = time_str, url = sample_url(), ip = sample_ip(), referer = sample_referer(), status_code = sample_status_code()) 
    print(query_log)
    # f.write(query_log + "\n")
    count -= 1
  # f.close()

if __name__ == '__main__':
  generate_log()

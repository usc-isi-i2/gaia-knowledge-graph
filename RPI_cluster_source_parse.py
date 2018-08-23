"""
This file parse the visualization source website into a json object
key is source name used in N-triples
value is {url: url on the website, content: extracted text content}

Note:
    The extracted content is a bit off with the startOffset and endOffset.
"""
import re
from bs4 import BeautifulSoup
import requests
import json

pattern = re.compile(r'href="(.*?_(.*?)_.*?\.html)"')
url = "https://blender04.cs.rpi.edu/~zhangt13/aida_demo/visualize/"
space_remove = re.compile(r' +')
map_path = '/Users/eric/Git/gaia-knowledge-graph/files_from_mayank/source_map.json'


def main():
    map_ = {}
    f = get_content('https://blender04.cs.rpi.edu/~zhangt13/aida_demo/visualize/')
    for line in f.splitlines():
        m = pattern.search(line)
        if m:
            key = m.group(2)
            url_ = url + m.group(1)
            map_[key] = {
                'URL': url_,
                'content': get_and_parse_content(url_)
            }

    json.dump(map_, open(map_path, "w"))
    print(map_)


def get_and_parse_content(url):
    content = get_content(url)
    parsed_content = parse_content(content)
    return parsed_content


def parse_content(content):
    soup = BeautifulSoup(content, 'lxml')
    for elem in soup.find_all('div', class_='tooltip'):
        text = elem.contents[0].contents[1].strip()
        elem.replace_with(text)
    body = soup.body.get_text()
    return space_remove.sub(' ', body)


def get_content(url):
    r = requests.get(url)
    if r.status_code == 200:
        return r.text
    print('[ERROR] Cannot access %s' % url)
    return ''


class SourceContext:
    def __init__(self):
        self.map = json.load(open(map_path))

    def get_some_context(self, src, start, end, offset=50):
        if src not in self.map:
            return ''
        return self.__get_context_with_offset(self.map[src]['content'], start, end, offset)

    @staticmethod
    def __get_context_with_offset(content, start, end, offset):
        from_ = max((0, content.rfind('\n', 0, start+1)+1, start-offset))
        newline_ind = content.find('\n', end)
        if newline_ind != -1:
            to = min((len(content), newline_ind, end+offset))
        else:
            to = min(len(content), end+offset)
        result = content[from_:to].strip().replace('\n', ' ')
        if from_ == start-offset:
            result = '...'+result
        if to == end+offset:
            result += '...'
        return result


if __name__ == '__main__':
    main()


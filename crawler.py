import asyncio
import re
import shutil
from pathlib import Path

import aiofiles
import aiohttp
import lxml.html
from urllib.parse import urljoin, urlparse

from celery import Celery

celery_app = Celery('crawler', backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')

ARCHIVES_PATH = Path(__file__).parent.absolute() / 'static'  # папка, куда сохраняются архивы
DEPTH = 3  # глубина прохода по ссылкам


@celery_app.task(bind=True)
def save_web_site(self, url):
    c = Crawler()
    asyncio.run(c.run(url, self.request.id, DEPTH))
    dir_name = str(ARCHIVES_PATH / self.request.id)
    shutil.make_archive(dir_name, 'zip', dir_name)
    return c.errors


class Crawler:
    start_url = None
    task_id = None

    def __init__(self):
        self.visited = set()

    async def run(self, url, task_id, depth):
        self.errors = []
        self.visited.clear()
        self.start_url = urlparse(url)
        self.task_id = task_id
        async with aiohttp.ClientSession() as self.session:
            await self.save_url(url, depth)

    async def save_url(self, url, depth):
        """Сохраняет url в папку"""
        tasks = []
        try:
            async with self.session.get(url) as resp:
                final_url = str(resp.url)
                self.visited.add(final_url)
                parse_result = urlparse(final_url)
                if not resp.status == 200:
                    raise Exception(f'Bad status: {resp.status}')
                # загружаем файл
                content = await resp.read()

                # формируем путь
                file_path = self.make_file_path(parse_result)
                file_path.parent.mkdir(parents=True, exist_ok=True)
                # сохраняем файл
                async with aiofiles.open(file_path, mode='wb') as f:
                    await f.write(content)

                # собираем ссылки
                if depth and (not parse_result.path or
                              parse_result.path.endswith('/') or
                              parse_result.path.endswith('.html') or
                              parse_result.path.endswith('.php')):
                    text = content.decode(encoding=resp.get_encoding())
                    tasks = [asyncio.create_task(self.save_url(link, depth - 1)) for link in
                             self.collect_urls(url, text)]
                if tasks:
                    await asyncio.gather(*tasks)
        except Exception as err:
            self.errors.append((resp.url, err))

    def collect_urls(self, current_url, text):
        """Собирает валидные ссылки на файлы для сохранения"""
        html = lxml.html.fromstring(text)
        for element, attribute, link, _ in html.iterlinks():
            if not link.startswith('http') and re.match(r'\w+:', link):
                continue
            # если ссылка, проверяем, что локальная
            link_parse = urlparse(link)
            # пропускаем сложные ссылки
            if link_parse.fragment or link_parse.params or link_parse.query:
                continue
            if element.tag == 'a':
                # пропускаем ссылки на внешние домены
                if link_parse.netloc and link_parse.netloc != self.start_url.netloc:
                    continue
            absolute_link = link
            if not link_parse.netloc:
                absolute_link = urljoin(current_url, link)
            # пропускаем те, что уже загрузили
            if absolute_link in self.visited:
                continue
            self.visited.add(absolute_link)
            yield absolute_link

    def make_file_path(self, parse_result):
        # создаем путь файла id задачи/домен/файлы
        path = ARCHIVES_PATH / self.task_id / parse_result.netloc / parse_result.path.strip('/')
        if not parse_result.path or parse_result.path.endswith('/'):
            path /= 'index.html'
        return path


if __name__ == '__main__':
    c = Crawler()
    asyncio.run(c.run('http://timeweb.com', 'test', DEPTH))

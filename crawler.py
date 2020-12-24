import asyncio
import re
import shutil
from pathlib import Path
from typing import List, Iterator, Tuple

import aiofiles
import aiohttp
import lxml.html
from urllib.parse import urljoin, urlparse, ParseResult

from celery import Celery

ARCHIVES_PATH = Path(__file__).parent.absolute() / 'static'  # Папка, куда сохраняются архивы
DEPTH = 2  # Глубина прохода по ссылкам

celery_app = Celery('crawler', backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')


@celery_app.task(bind=True)
def save_web_site(self, url: str) -> Tuple[bool, List[str]]:
    """Обходит сайт, начиная с url и сохраняет файлы. Потом создает архив."""
    c = Crawler()
    asyncio.run(c.run(url, self.request.id, DEPTH))
    dir_name = ARCHIVES_PATH / self.request.id
    if dir_name.exists():
        shutil.make_archive(dir_name, 'zip', dir_name)
    return dir_name.exists(), c.errors


class Crawler:
    def __init__(self):
        self.start_url = None
        self.task_id = None
        self.visited = set()  # url страниц, которые уже обработаны
        self.errors = []  # Список ошибок

    async def run(self, url: str, task_id: str, depth: int) -> None:
        """Запуск паука"""
        self.errors.clear()
        self.visited.clear()
        self.start_url = urlparse(url)
        self.task_id = task_id
        async with aiohttp.ClientSession() as self.session:
            await self.save_url(url, depth)

    async def save_url(self, url: str, depth: int) -> None:
        """Сохраняет url в папку. Если html, то парсит ссылки на странице и запускает задачи по обработке ссылок."""
        tasks = []
        try:
            async with self.session.get(url) as resp:
                final_url = str(resp.url)
                self.visited.add(final_url)
                parse_result = urlparse(final_url)
                if not resp.status == 200:
                    raise Exception(f'Bad status: {resp.status}')

                # Загружаем файл
                content = await resp.read()

                # Формируем путь
                file_path = self.make_file_path(parse_result)
                file_path.parent.mkdir(parents=True, exist_ok=True)

                # Сохраняем файл
                async with aiofiles.open(file_path, mode='wb') as f:
                    await f.write(content)

                # Собираем ссылки
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
            try:
                bad_url = final_url
            except:
                bad_url = url
            self.errors.append(f'Url: {bad_url} Error: {str(err)}')

    def collect_urls(self, current_url: str, text: str) -> Iterator[str]:
        """Собирает валидные ссылки на файлы для сохранения"""
        html = lxml.html.fromstring(text)
        for element, attribute, link, _ in html.iterlinks():
            if not link.startswith('http') and re.match(r'\w+:', link):
                continue
            # Если ссылка, проверяем, что локальная
            link_parse = urlparse(link)
            # Пропускаем сложные ссылки
            if link_parse.fragment or link_parse.params or link_parse.query:
                continue
            if element.tag == 'a':
                # Пропускаем ссылки на внешние домены
                if link_parse.netloc and link_parse.netloc != self.start_url.netloc:
                    continue
            absolute_link = link
            if not link_parse.netloc:
                absolute_link = urljoin(current_url, link)
            # Пропускаем те, что уже загрузили
            if absolute_link in self.visited:
                continue
            self.visited.add(absolute_link)
            yield absolute_link

    def make_file_path(self, parse_result: ParseResult) -> Path:
        # Создает путь файла Папка архивов/id задачи/домен/файл
        path = ARCHIVES_PATH / self.task_id / parse_result.netloc / parse_result.path.strip('/')
        if not parse_result.path or parse_result.path.endswith('/'):
            path /= 'index.html'
        return path


if __name__ == '__main__':
    c = Crawler()
    asyncio.run(c.run('http://timewebx.com', 'test', DEPTH))

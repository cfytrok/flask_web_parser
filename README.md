# Тестовое задание для вакансии Python разработчик
Парсер сайтов на Flask.
 
Начинает обход с указанной в POST запросе страницы. Глубина обхода ограничена. Сохраняет html, js, css и медиафайлы. Затем создает архив сайта. Ссылку на архив можно получить GET запросом с указанием id задачи.

Использованы Celery и aiohttp.
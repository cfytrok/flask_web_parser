from flask import Flask, request, url_for, render_template
from crawler import save_web_site, celery_app

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def post_task() -> str:
    """Возвращает форму при Get. Создает задачу при Post и возвращает id задачи"""
    if request.method == 'GET':
        return app.send_static_file('index.html')
    task = save_web_site.delay(request.form['start_url'])
    return f"<a href = {task.task_id}>{task.task_id}</a>"


@app.route('/<task_id>', methods=['GET'])
def get_task(task_id: str) -> str:
    """Возвращает статус задачи по id. Если задача готова, возвращает ссылку на архив сайта"""
    result = celery_app.AsyncResult(task_id)
    if not result.ready():
        return 'Pending'
    folder_exists, errors = result.get()
    file_url = None
    if folder_exists:
        file_url = url_for('static', filename=task_id + '.zip')
    return render_template('task.html', file_url=file_url, errors=errors)


if __name__ == '__main__':
    app.run()

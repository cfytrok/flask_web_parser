from flask import Flask, request, url_for
from crawler import save_web_site, celery_app

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def post_task():
    if request.method == 'GET':
        return app.send_static_file('index.html')
    task = save_web_site.delay(request.form['start_url'])
    return task.task_id


@app.route('/<task_id>', methods=['GET'])
def get_task(task_id):
    res = celery_app.AsyncResult(task_id)
    if not res.ready():
        return 'Pending'
    file_url = url_for('static', filename=task_id + '.zip')
    return f"<a href = {file_url}>{file_url}</a>"


if __name__ == '__main__':
    app.run()

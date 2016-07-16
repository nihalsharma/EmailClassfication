from asyncio import coroutine
from concurrent import futures
import os
import asyncio
from jinja2 import PackageLoader, Environment

env = Environment(loader=PackageLoader('classification', ''))


@coroutine
def create_html_files(result_set, output_path):
    """
    create html files using the result set.
    :param result_set: 
    :param output_path: 
    """
    if output_path and result_set:
        template = env.get_template("email.html")
        for result in result_set:
            email_body = template.render(result)
            filename = result.get('service') + ".html"
            completeName = os.path.join(output_path, filename)
            yield from submit_task(completeName, email_body)


@coroutine
def submit_task(completeName, email_body):
    """
    submit the task of writing html file
    :param completeName: 
    :param email_body: 
    :return: 
    """
    with futures.ThreadPoolExecutor(max_workers=3) as executor:
        job = executor.submit(write_files, completeName, email_body)
        return (yield from asyncio.wrap_future(job))


def write_files(name, email_body):
    """
    write the html files
    :param name: 
    :param email_body: 
    """
    with open(name, "w") as file:
        file.write(email_body)

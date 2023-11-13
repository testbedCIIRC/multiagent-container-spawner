import json
import os
import tarfile

import docker
import pika

from config_handler import app_config_dic
from celery import Celery


app = Celery(app_config_dic['Celery_app_name'], broker=app_config_dic['Celery_broker_url'],
             backend=app_config_dic['Celery_backend_url'])

all_known_tasks_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'all_known_tasks')

client = docker.from_env()
containers = dict()


def on_message_get(ch, method, properties, body):
    s = body.decode("utf-8")
    j_dict = json.loads(s)

    if j_dict['command'] == 'create_worker':
        create_and_configure_worker(j_dict)

    elif j_dict['command'] == 'stop_worker':
        app.control.broadcast('shutdown', destination=['celery@' + j_dict['worker_name']])

    elif j_dict['command'] == 'send_task':
        """Result handler x not used yet"""
        x = app.send_task(name=j_dict['task_name'],
                          exchange=app_config_dic['Celery_exchange'], routing_key=j_dict['worker_name'],
                          queue=j_dict['worker_name'], exchange_type='direct')

    elif j_dict['command'] == 'kill_worker':
        kill_celery_worker(j_dict['worker_name'])

    elif j_dict['command'] == 'list_workers':
        pass

    elif j_dict['command'] == 'task_ready':
        pass


def listen_for_commands():
    connection_params = pika.ConnectionParameters(app_config_dic['Master_broker_url'])
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    channel.queue_declare(queue=app_config_dic['Master_queue'], durable=True)
    channel.basic_consume(queue=app_config_dic['Master_queue'], auto_ack=True, on_message_callback=on_message_get)

    channel.start_consuming()


def create_and_configure_worker(config_json_dict):
    container = client.containers.run(app_config_dic['Celery_worker_image'],
                                      name=config_json_dict['worker_name'],
                                      network_mode='host',
                                      environment={'WORKER_NAME': config_json_dict['worker_name']},
                                      detach=True,
                                      remove=True)

    config_json_dict['Celery_app_name'] = app_config_dic['Celery_app_name']
    config_json_dict['Celery_broker_url'] = app_config_dic['Celery_broker_url']
    config_json_dict['Celery_backend_url'] = app_config_dic['Celery_backend_url']
    config_json_dict['Celery_exchange'] = app_config_dic['Celery_exchange']

    config_json_dict['Reporting_broker_url'] = app_config_dic['Reporting_broker_url']
    config_json_dict['Reporting_exchange'] = app_config_dic['Reporting_exchange']
    config_json_dict['Reporting_routing_key'] = app_config_dic['Reporting_routing_key'] + '.' + \
                                                config_json_dict['worker_name']

    containers[config_json_dict['worker_name']] = container

    """Last config.json is left in the folder"""
    with open('config.json', 'w') as file:
        json.dump(config_json_dict, file)

    copy_to(src='config.json', dst='/', container=container)

    os.chdir(all_known_tasks_path)

    for task in config_json_dict['tasks']:
        copy_to(src=str(task['executable_path']), dst='/executables/', container=container)

    return container


def copy_to(src, dst, container):
    """Copied from
    https://stackoverflow.com/questions/46390309/how-to-copy-a-file-from-host-to-container-using-docker-py-docker-sdk
    """
    if os.path.isfile(src + '.tar'):
        os.remove(src + '.tar')

    tar = tarfile.open(src + '.tar', mode='x:gz')

    try:
        tar.add(src)
    finally:
        tar.close()

    data = open(src + '.tar', 'rb').read()
    container.put_archive(dst, data)

    os.remove(src + '.tar')


def kill_celery_worker(name):
    pass


if __name__ == '__main__':
    print('backend_docker_main')

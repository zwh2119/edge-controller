import asyncio
import copy
import json
import os
import shutil

import requests
from fastapi import FastAPI, BackgroundTasks, UploadFile, File, Form

from fastapi.routing import APIRoute
from starlette.responses import JSONResponse
from starlette.requests import Request
from fastapi.middleware.cors import CORSMiddleware

from utils import *

local_port = 9002

service_ports_dict = {'car_detection': 9001}
distribute_ip = '114.212.81.11'
distribute_port = 5713


class ControllerServer:
    def __init__(self):
        self.app = FastAPI(routes=[
            APIRoute('/submit_task',
                     self.deal_response,
                     response_class=JSONResponse,
                     methods=['POST']

                     ),
        ], log_level='trace', timeout=6000)

        self.local_address = get_merge_address(get_host_ip(), port=local_port, path='submit_task')
        self.distribute_address = get_merge_address(distribute_ip, port=distribute_port, path='distribute')

        self.app.add_middleware(
            CORSMiddleware, allow_origins=["*"], allow_credentials=True,
            allow_methods=["*"], allow_headers=["*"],
        )

    def service_transmit(self, data, file):

        data = json.loads(data)

        # get json data
        source_id = data['source_id']
        task_id = data['task_id']
        pipeline = data['pipeline_flow']
        tmp_data = data['tmp_data']
        index = data['cur_flow_index']
        scenario = data['scenario_data']
        content = data['content_data']

        print(f'controller get data from source {source_id}')

        # get file data(video)
        tmp_path = f'tmp_receive_source_{source_id}_task_{task_id}.mp4'
        with open(tmp_path, 'wb') as buffer:
            shutil.copyfileobj(file.file, buffer)
            del file

        # end record transmit time
        tmp_data, transmit_time = record_time(tmp_data, f'transmit_time_{index}')
        assert transmit_time != -1
        pipeline[index]['execute_data']['transmit_time'] = transmit_time

        # execute pipeline
        # TODO: merge service deal and distribute
        while index < len(pipeline)-1:
            cur_service = pipeline[index]

            # transfer to another controller
            if cur_service['execute_address'] != self.local_address:
                tmp_data, transmit_time = record_time(tmp_data, f'transmit_time_{index}')
                assert transmit_time == -1

                data['pipeline_flow'] = pipeline
                data['tmp_data'] = tmp_data
                data['cur_flow_index'] = index
                data['content_data'] = content
                data['scenario_data'] = scenario

                # post to other controllers
                requests.post(cur_service['execute_address'], data={'data': json.dumps(data)},
                              files={'file': (f'tmp_{source_id}.mp4',
                                              open(tmp_path, 'rb'),
                                              'video/mp4')})

                print(f'controller post data from source {source_id} to other controller')
                os.remove(tmp_path)
                return

            # start record service time
            tmp_data, service_time = record_time(tmp_data, f'service_time_{index}')
            assert service_time == -1

            # post to service
            service_name = pipeline[index]['service_name']
            assert service_name in service_ports_dict
            service_address = get_merge_address(get_host_ip(), port=service_ports_dict[service_name],
                                                path='predict')
            service_response = requests.post(service_address,
                                             data={'data': json.dumps(content)},
                                             files={
                                                 'file': (f'tmp_{source_id}.mp4', open(tmp_path, 'rb'), 'video/mp4')
                                             })
            service_return = service_response.json()

            # end record service time
            tmp_data, service_time = record_time(tmp_data, f'service_time_{index}')
            assert service_time != -1
            pipeline[index]['execute_data']['service_time'] = service_time
            print(f'service_time:{service_time}s')

            # deal with service result
            if 'parameters' in service_return:
                scenario.update(service_return['parameters'])
            content = copy.deepcopy(service_return['result'])

            index += 1

        # start record transmit time
        tmp_data, transmit_time = record_time(tmp_data, f'transmit_time_{index}')
        assert transmit_time == -1

        data['pipeline_flow'] = pipeline
        data['tmp_data'] = tmp_data
        data['cur_flow_index'] = index
        data['content_data'] = content
        data['scenario_data'] = scenario

        # post to distributor
        requests.post(self.distribute_address, json=data)
        print(f'controller post data from source {source_id} to distributor')

        os.remove(tmp_path)

    async def deal_response(self, backtask: BackgroundTasks, file: UploadFile = File(...), data: str = Form(...)):
        backtask.add_task(self.service_transmit, data, file)
        return {'msg': 'data send success!'}


app = ControllerServer().app

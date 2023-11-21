import asyncio
import copy

from fastapi import FastAPI, BackgroundTasks

from fastapi.routing import APIRoute
from starlette.responses import JSONResponse
from starlette.requests import Request
from fastapi.middleware.cors import CORSMiddleware

from utils import *


class ControllerServer:
    def __init__(self):
        self.app = FastAPI(routes=[
            APIRoute('/submit_task',
                     self.deal_response,
                     response_class=JSONResponse,
                     methods=['POST']

                     ),
        ], log_level='trace', timeout=6000)

        self.local_address = ''

        self.app.add_middleware(
            CORSMiddleware, allow_origins=["*"], allow_credentials=True,
            allow_methods=["*"], allow_headers=["*"],
        )

    def service_transmit(self, data: dict):
        # TODO: transmit task to service

        pipeline = data['pipeline_flow']
        tmp_data = data['tmp_data']
        index = data['cur_flow_index']
        content = data['content_data']
        scenario = data['scenario_data']

        # record transmit time
        tmp_data, transmit_time = record_time(tmp_data, f'transmit_time_{index}')
        assert transmit_time != 0
        pipeline[index]['execute_data']['transmit_time'] = transmit_time

        while index < len(pipeline):
            cur_service = pipeline[index]
            if cur_service['execute_address'] != self.local_address:
                tmp_data, transmit_time = record_time(tmp_data, f'transmit_time_{index}')
                assert transmit_time == 0

                data['pipeline_flow'] = pipeline
                data['tmp_data'] = tmp_data
                data['cur_flow_index'] = index
                data['content_data'] = content
                data['scenario_data'] = scenario

                # TODO: post to other controllers

                return

            tmp_data, service_time = record_time(tmp_data, f'service_time_{index}')
            assert service_time == 0

            # TODO: post to service
            service_return = {}

            tmp_data, service_time = record_time(tmp_data, f'service_time_{index}')
            assert service_time != 0
            pipeline[index]['execute_data']['service_time'] = service_time
            scenario.update(service_return['parameters'])
            content = copy.deepcopy(service_return['result'])

            index += 1

        data['pipeline_flow'] = pipeline
        data['tmp_data'] = tmp_data
        data['cur_flow_index'] = index
        data['content_data'] = content
        data['scenario_data'] = scenario
        # TODO: post to distributor

    async def deal_response(self, request: Request, backtask: BackgroundTasks):
        data = await request.json()
        backtask.add_task(self.service_transmit, data)
        return {'msg': 'data send success!'}


app = ControllerServer().app

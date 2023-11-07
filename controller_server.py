import asyncio

from fastapi import FastAPI, BackgroundTasks

from fastapi.routing import APIRoute
from starlette.responses import JSONResponse
from starlette.requests import Request
from fastapi.middleware.cors import CORSMiddleware


class ControllerServer:
    def __init__(self):
        self.app = FastAPI(routes=[
            APIRoute('/submit_task',
                     self.deal_data,
                     response_class=JSONResponse,
                     methods=['POST']

                     ),
        ], log_level='trace', timeout=6000)

        self.app.add_middleware(
            CORSMiddleware, allow_origins=["*"], allow_credentials=True,
            allow_methods=["*"], allow_headers=["*"],
        )

    async def service_transmit(self, data: dict):
        pass

    async def deal_data(self, request: Request, backtask: BackgroundTasks):
        data = await request.json()
        backtask.add_task(self.service_transmit, data)
        return {'mag': 'data send success!'}


app = ControllerServer().app

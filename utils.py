import time


def record_time(data: dict, time_sign: str):
    if time_sign in data:
        start_time = data[time_sign]
        end_time = time.time()
        return data, end_time-start_time
    else:
        data[time_sign] = time.time()
        return data, 0

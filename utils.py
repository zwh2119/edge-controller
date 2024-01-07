import fcntl
import re
import socket
import struct
import time


def record_time(data: dict, time_sign: str):
    if time_sign in data:
        start_time = data[time_sign]
        end_time = time.time()
        del data[time_sign]
        return data, end_time - start_time
    else:
        data[time_sign] = time.time()
        return data, -1


def get_merge_address(ip, protocal='http', port=None, path=None):
    """
    merge address from {protocal, ip, port, path}
    eg: http://127.0.0.1:9000/submit
    """

    port_divider = '' if port is None else ':'
    path_divider = '' if path is None else '/'

    port = '' if port is None else port
    path = '' if path is None else path

    return f'{protocal}://{ip}{port_divider}{port}{path_divider}{path}'


def extract_ip_from_address(address):
    ip_pattern = r"https?:\/\/([\d\.]+):\d+"
    ip_match = re.search(ip_pattern, address)
    return ip_match.group(1) if ip_match else None


# def get_host_ip():
#     """
#     get ip of current local host
#     """
#
#     ip = '127.0.0.1'
#     s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#     try:
#         s.connect(("8.8.8.8", 80))
#         ip = s.getsockname()[0]
#     except Exception:
#         pass
#     finally:
#         s.close()
#
#     return ip

def get_host_ip():
    """
    get ip of current local host
    in docker container
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ip_address = socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', b'docker0'[:15])
    )[20:24])
    return ip_address

from datetime import datetime
from zoneinfo import ZoneInfo
from monitoring_lib.producer_kafka import kafka_logger
import time
import json

def get_client_ip(request):
    ip = request.META.get('HTTP_X_FORWARDED_FOR')
    if ip:
        ip = ip.split(',')[0].strip()
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip

def log_to_kafka(view_class):
    original_dispatch = view_class.dispatch

    def wrapper(self, request, *args, **kwargs):
        start = time.time()
        status_code = None
        error = None
        response = None
        request_data = {}


        if request.method in {'POST', 'PUT', 'PATCH'}:
            try:
                if request.content_type == 'application/json':
                    request_data = json.loads(request.body.decode('utf-8'))
                else:
                    request_data = dict(request.data)
            except Exception as e:
                request_data = {}

        try:
            response = original_dispatch(self, request, *args, **kwargs)
            status_code = getattr(response, 'status_code', 500)
            return response

        except Exception as e:
            status_code = 500
            error = str(e)
            raise

        finally:
            end = time.time()

            secret_keys = {'password', 'token', 'access_token', 'refresh_token'}
            data = {}
            for k, v in request_data.items():
                if k not in secret_keys:
                    data[k] = v

            log_data = {
                "method": request.method,
                "path": request.path,
                "status": status_code if status_code else "unknown",
                "duration": round(end - start, 10),
                "user_id": request.user.id if request.user.is_authenticated else None,
                "timestamp": datetime.now(ZoneInfo("UTC")).isoformat(),
                "ip_address": get_client_ip(request),
                "request_data": data,
            }

            if error:
                log_data["error"] = error

            kafka_logger.send_log(log_data)


    view_class.dispatch = wrapper
    return view_class

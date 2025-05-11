from datetime import datetime
from monitoring_lib.producer_kafka import kafka_logger
import time

def get_client_ip(request):
    ip = request.META.get('HTTP_X_FORWARDED_FOR')
    if ip:
      ip = ip.split(',')[0].strip()
    else:
      ip = request.META.get('REMOTE_ADDR')
    return ip


def log_to_kafka(view_func):
  def wrapper(self, request, *args, **kwargs):
    start = time.time()
    response = view_func(self, request, *args, **kwargs)
    end = time.time()

    log_data = {
      "method": request.method,
      "path": request.path,
      "status": response.status_code,
      "duration": round(end  - start, 3),
      "user_id": request.user.id if request.user.is_authenticated else None,
      "timestamp":datetime.now().isoformat() + "Z",
      "ip_address": get_client_ip(request),

    }

    kafka_logger.send_log(log_data)
    return response
  return wrapper






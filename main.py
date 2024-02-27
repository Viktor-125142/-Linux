import socket
import json
import multiprocessing
from datetime import datetime
import threading


def worker_process(queue, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", port))

    while True:
        data, addr = sock.recvfrom(1024)
        try:
            message = json.loads(data)
            queue.put(message)
        except json.JSONDecodeError:
            continue


def aggregate_metrics(queue, interval, filename):
    def aggregate():
        nonlocal metrics
        timestamp = int(datetime.now().timestamp())
        with open(filename, "a") as f:
            record = {
                "A1": sum([m["A1"] for m in metrics]),
                "A2": max([m["A2"] for m in metrics], default=0),
                "A3": min([m["A3"] for m in metrics], default=0)
            }
            f.write(json.dumps(record) + "\n")
        metrics.clear()
        threading.Timer(interval, aggregate).start()

    metrics = []
    threading.Timer(interval, aggregate).start()

    while True:
        message = queue.get()
        metrics.append(message)


def main():
    PORT = 12345
    queue = multiprocessing.Queue()

    worker = multiprocessing.Process(target=worker_process, args=(queue, PORT))
    worker.start()

    aggregate_process_10s = multiprocessing.Process(target=aggregate_metrics, args=(queue, 10, "metrics_10s.json"))
    aggregate_process_10s.start()

    aggregate_process_60s = multiprocessing.Process(target=aggregate_metrics, args=(queue, 60, "metrics_60s.json"))
    aggregate_process_60s.start()


if __name__ == "__main__":
    main()

import threading


class Server(threading.Thread):
    def __init__(self):
        super(Server, self).__init__()

    def run(self) -> None:
        self._run_ea()

    def join(self, **kwargs) -> None:
        threading.Thread.join(self)

    def _run_ea(self):
        pass


if __name__ == '__main__':
    server = Server()
    server.start()

    try:
        server.join()
    except Exception as ex:
        print("Exception: ", ex)

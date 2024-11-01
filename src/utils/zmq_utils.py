import zmq

class ZMQUtils:
    def __init__(self, dispatcher_ip, pub_port, sub_port, rep_port, pull_port):
        self.context = zmq.Context()
        self.dispatcher_ip = dispatcher_ip
        self.pub_port = pub_port
        self.sub_port = sub_port
        self.rep_port = rep_port
        self.pull_port = pull_port
        self.publisher = None
        self.subscriber = None
        self.requester = None
        self.responder = None
        self.puller = None
        self.pusher = None

    def bind_pub_socket(self):
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind(f"tcp://*:{self.pub_port}")
        return self.publisher
    
    def bind_rep_socket(self):
        self.responder = self.context.socket(zmq.REP)
        self.responder.bind(f"tcp://*:{self.rep_port}")
        return self.responder

    def connect_sub(self, topic=""):
        self.subscriber = self.context.socket(zmq.SUB)
        self.subscriber.connect(f"tcp://{self.dispatcher_ip}:{self.sub_port}")
        self.subscriber.setsockopt_string(zmq.SUBSCRIBE, topic)
        return self.subscriber

    def connect_req(self):
        self.requester = self.context.socket(zmq.REQ)
        self.requester.connect(f"tcp://{self.dispatcher_ip}:{self.rep_port}")
        return self.requester
    
    def bind_pull_socket(self):
        self.puller = self.context.socket(zmq.PULL)
        self.puller.bind(f"tcp://*:{self.pull_port}")
        return self.puller

    def connect_push(self):
        self.pusher = self.context.socket(zmq.PUSH)
        self.pusher.connect(f"tcp://{self.dispatcher_ip}:{self.pull_port}")
        return self.pusher
    
    def disconnect_pub(self):
        if self.publisher:
            self.publisher.disconnect(f"tcp://{self.dispatcher_ip}:{self.pub_port}")

    def disconnect_sub(self):
        if self.subscriber:
            self.subscriber.disconnect(f"tcp://{self.dispatcher_ip}:{self.sub_port}")

    def close_req(self):
        if self.requester:
            self.requester.close()
    
    def disconnect_rep(self):
        if self.responder:
            self.responder.close()

    def close(self):
        if self.publisher:
            self.publisher.close()
        if self.subscriber:
            self.subscriber.close()
        if self.requester:
            self.requester.close()
        if self.responder:
            self.responder.close()
        if self.puller:
            self.puller.close()
        if self.pusher:
            self.pusher.close()
        self.context.term()

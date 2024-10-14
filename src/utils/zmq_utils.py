import zmq

class ZMQUtils:
    def __init__(self, dispatcher_ip, pub_port, sub_port, rep_port):
        self.context = zmq.Context()
        self.dispatcher_ip = dispatcher_ip
        self.pub_port = pub_port
        self.sub_port = sub_port
        self.rep_port = rep_port
        self.publisher = None
        self.subscriber = None
        self.requester = None
        self.responder = None

    def bind_pub_socket(self):
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind(f"tcp://*:{self.pub_port}")
        return self.publisher

    def bind_sub_socket(self, topic=""):
        self.subscriber = self.context.socket(zmq.SUB)
        self.subscriber.bind(f"tcp://*:{self.sub_port}")
        self.subscriber.setsockopt_string(zmq.SUBSCRIBE, topic)
        return self.subscriber
    
    def bind_rep_socket(self):
        self.responder = self.context.socket(zmq.REP)
        self.responder.bind(f"tcp://*:{self.rep_port}")
        return self.responder
    
    def connect_pub(self):
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.connect(f"tcp://{self.dispatcher_ip}:{self.pub_port}")
        return self.publisher

    def connect_sub(self, topic=""):
        self.subscriber = self.context.socket(zmq.SUB)
        self.subscriber.connect(f"tcp://{self.dispatcher_ip}:{self.sub_port}")
        self.subscriber.setsockopt_string(zmq.SUBSCRIBE, topic)
        return self.subscriber

    def connect_req(self):
        self.requester = self.context.socket(zmq.REQ)
        self.requester.connect(f"tcp://{self.dispatcher_ip}:{self.rep_port}")
        return self.requester
    
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
        self.context.term()
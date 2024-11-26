import zmq
import threading

class ZMQUtils:
    def __init__(self, dispatcher_ip, pub_port, sub_port, rep_port, pull_port, heartbeat_port, heartbeat_2_port):
        self.context = zmq.Context()
        self.dispatcher_ip = dispatcher_ip
        self.pub_port = pub_port
        self.sub_port = sub_port
        self.rep_port = rep_port
        self.pull_port = pull_port
        self.heartbeat_port = heartbeat_port
        self.heartbeat_2_port = heartbeat_2_port
        self.publisher = None
        self.subscriber = None
        self.requester = None
        self.responder = None
        self.puller = None
        self.pusher = None
        self.heartbeat_puller = None
        self.heartbeat_2_puller = None
        self.heartbeat_pusher = None
        self.heartbeat_2_pusher = None
        self.heartbeat_responder = None
        self.socket_ready = threading.Condition()
        self.socket_initialized = False

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

    def bind_pull_heartbeat_socket(self):
        self.heartbeat_puller = self.context.socket(zmq.PULL)
        self.heartbeat_puller.bind(f"tcp://*:{self.heartbeat_port}")
        return self.heartbeat_puller

    def connect_push_heartbeat(self):
        self.heartbeat_pusher = self.context.socket(zmq.PUSH)
        self.heartbeat_pusher.connect(f"tcp://{self.dispatcher_ip}:{self.heartbeat_port}")
        return self.heartbeat_pusher

    def bind_pull_heartbeat_2_socket(self):
        self.heartbeat_puller = self.context.socket(zmq.PULL)
        self.heartbeat_puller.bind(f"tcp://*:{self.heartbeat_2_port}")
        return self.heartbeat_puller

    def connect_push_heartbeat_2(self):
        self.heartbeat_pusher = self.context.socket(zmq.PUSH)
        self.heartbeat_pusher.connect(f"tcp://{self.dispatcher_ip}:{self.heartbeat_2_port}")
        return self.heartbeat_pusher
    
    def bind_rep_user_request_socket(self, port):
        socket = self.context.socket(zmq.REP)
        socket.bind(f"tcp://*:{port}")
        return socket
    
    def bind_rep_heartbeat_socket(self):
        self.heartbeat_responder = self.context.socket(zmq.REP)
        self.heartbeat_responder.bind(f"tcp://*:{self.heartbeat_3_port}")
        return self.heartbeat_responder
    
    # def publish_assignment(self, message):
    #     pub_socket = self.context.socket(zmq.PUB)
    #     pub_socket.connect(f"tcp://{self.dispatcher_ip}:{self.pub_port}")
    #     pub_socket.send_string(message)
    #     pub_socket.close()
    def publish_assignment(self, message):
        pub_socket = self.context.socket(zmq.PUB)
        pub_socket.bind(f"tcp://*:{self.pub_port}")
        pub_socket.send_string(message)
        pub_socket.close()
    
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

    def recreate_all_sockets(self, taxi_id=None, topic=None):
        """Closes all existing sockets and recreates fresh ones."""
        self.close()  # Ensure all existing sockets are properly closed

        # Reinitialize the context
        self.context = zmq.Context()

        # Reinitialize all socket attributes to None
        self.publisher = None
        self.subscriber = None
        self.requester = None
        self.responder = None
        self.puller = None
        self.pusher = None
        self.heartbeat_puller = None
        self.heartbeat_pusher = None
        self.heartbeat_2_puller = None
        self.heartbeat_2_pusher = None

        # Recreate sockets
        self.connect_push()
        if topic is not None:
            self.connect_sub(topic=topic)
        self.connect_push_heartbeat()

        # Notify all waiting threads that sockets are ready
        with self.socket_ready:
            self.socket_initialized = True
            self.socket_ready.notify_all()

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
        if self.heartbeat_puller:
            self.heartbeat_puller.close()
        if self.heartbeat_pusher:
            self.heartbeat_pusher.close()
        if self.heartbeat_2_puller:
            self.heartbeat_2_puller.close()
        if self.heartbeat_2_pusher:
            self.heartbeat_2_pusher.close()
        self.context.term()

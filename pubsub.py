import socket
import thread
import time

class SocketClient:
    """
    =================
    Pub Sub Generic Client
    =================
    Description: This is a generic client implementation. All interaction
    with the broker is done through this class. It continuously listens
    for published messages in a thread, provides api for publishing mess-
    ages. A client can subscribe to more than one channels at a time.

    API:
        publish(channel_name, message)
        uses broker's PUB API.

        subscribe(channel_name)
        uses broker's SUB API.

        exiter()
        uses broker's EXIT API.

        set_callback(function)
        function will be triggered with the message, ie. function(message)
        ,when a message is received from subscribed channel.

    """
    def __init__(self, host, port):
        """
        Initializes client with host and port. Starts a new thread for li-
        stening to incoming messages.
        """
        self.host = host
        self.port = port
        self.callback = None
        self.sock = socket.socket()
        self.sock.connect((host, port))
        thread.start_new_thread(SocketClient.clientthread,(self.sock, self.__message_received_callback))

    @staticmethod
    def clientthread(conn, callback):
        """
        Listens for incoming message.
        Raises RuntimeError, if server connection breaks abruptly.
        """
        while True:
            try:
                data = conn.recv(1024)
                callback(data)
            except:
                raise RuntimeError("Server crashed")
                conn.close()

    def __message_received_callback(self, msg):
        """
        Triggers callback function if its set.
        """
        if self.callback:
            self.callback(msg)

    def __send(self, data):
        """
        Send function, sleep after sending to avoid socket combining con-
        secutive messages.
        """
        self.sock.send(data)
        time.sleep(0.01)

    def set_callback(self, fn):
        """
        Api for setting callback function.
        """
        self.callback = fn

    def publish(self, channel, msg):
        """
        Api for publishing message.
        """
        send_data = "PUB %s %s"%(channel, msg)
        self.__send(send_data)

    def subscribe(self, channel):
        """
        Api for subscribing to a channel.
        """
        send_data = "SUB %s"%(channel)
        self.__send(send_data)

    def exiter(self):
        """
        Api for closing connection.
        """
        send_data = "EXIT "
        self.__send(send_data)

class Publisher:
    """
    =================
    Pub Sub Publisher
    =================
    Description: This is a wrapper over client implementation, for publisher
    specific events. Publisher is initialized with a channel name. All mess-
    ages are published only on this channel.

    API:
        send(message)
        publishes message on the channel.

        stop()
        stop connection.
    """
    def __init__(self, channel, host = "localhost", port = 52000):
        self.socket_client = SocketClient(host, port)
        self.channel = channel

    def send(self, message):
        self.socket_client.publish(self.channel, message)

    def stop(self):
        self.socket_client.exiter()

class Subscriber:
    """
    =================
    Pub Sub Subscriber
    =================
    Description: This is a wrapper over client implementation, for subscrib-
    er specific events. Subscriber is initialized with a channel name. All
    messages received will only be from this channel. This class also provi-
    des api for setting callback. If callback is not set, messages received
    are stored in a message queue. Subsequent calls to recv(), will dequeue
    messages one at a time. It is recommended to use recv() and set_callback
    exclusively.

    API:
        recv()
        Checks if there are any messages in message queue. If callback is s-
        et this api will return None.

        set_callback(function)
        triggers `function(message)`.

        stop()
        disconnect and stop receiving messages.
    """
    def __init__(self, channel, host = "localhost", port = 52000):
        self.socket_client = SocketClient(host, port)
        self.socket_client.set_callback(self.__on_recv)
        self.socket_client.subscribe(channel)
        self.callback = None
        self.channel = channel
        self.message_queue = []

    def __on_recv(self, message):
        if self.callback:
            self.callback(message)
        else:
            self.message_queue.append(message)

    def set_callback(self, fn):
        self.callback = fn

    def recv(self):
        # pop message queue
        if self.message_queue:
            ret = self.message_queue[0]
            self.message_queue = self.message_queue[1:]
            return ret
        return None

    def stop(self):
        self.callback = None
        self.socket_client.exiter()

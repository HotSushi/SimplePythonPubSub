import socket
import thread
import sys

def main():
    """
    usage:
    python broker.py [host_ip] [port_no]
    """
    if len(sys.argv) <= 1:
        sock = SocketServer('localhost',52000)
    elif len(sys.argv) == 2:
        sock = SocketServer(sys.argv[1],52000)
    else:
        sock = SocketServer(sys.argv[1],sys.argv[2])

class SocketServer():
    """
    =================
    Pub Sub Broker
    =================
    Description: This is a server implementation which provides an api
    for publishing messages to channels and subscribing to those chan-
    nel over a socket connection. This broker supports multiple subsc-
    ribers, publishers and channels

    API:
        PUB [channel_name] [message]
        publishes 'message' on 'channel_name', all subscribers to this
        channel will receive 'message'

        SUB [channel_name]
        subscribe to 'channel_name', once subscribed there is no api
        for unsubscribing. Client will have to close connection.

        EXIT
        API for cleanly closing a client connection. The subscriptions
        of this client will be permanantly removed.

    """
    subscribers = {}

    def __init__(self,host,port):
        """
        Initializes a pub sub broker, a new thread is spawned for every
        new connection.
        """
        self.host = host
        self.port = port

        self.sock = socket.socket()
        self.sock.bind((host, port))
        self.sock.listen(3)
        while True:
            conn, addr = self.sock.accept()
            thread.start_new_thread(SocketServer.clientthread,(conn,))
        self.sock.close()

    @staticmethod
    def clientthread(conn):
        """
        Code for a new connection thread. Continuously listen for messa-
        ges.  Handover the message to appropriate function.
        """
        while True:
            try:
                data = conn.recv(1024)
            except socket.error:
                SocketServer.cleanup(conn)
            if not data: continue
            data = data.split()
            event = data[0]
            if event == 'EXIT': break
            channel = data[1]
            message = "".join(data[2:])
            if event == 'PUB':
                SocketServer.publish(channel, message)
            elif event == 'SUB':
                SocketServer.subscribe(channel, conn)
        SocketServer.cleanup(conn)
        conn.close()

    @staticmethod
    def publish(channel, message):
        """
        Prints publish event. Publishes the message to a channel. Inter-
        nally, message will be sent to all connections listed for a cha-
        nnel.
        """
        print(channel, "publish", message)
        if channel in SocketServer.subscribers:
            for conn in SocketServer.subscribers[channel]:
                conn.send(message)

    @staticmethod
    def cleanup(conn):
        """
        Removes all instances of a connection from subscriber dictionary.
        """
        for key in SocketServer.subscribers:
            if conn in SocketServer.subscribers[key]:
                SocketServer.subscribers[key].remove(conn)

    @staticmethod
    def subscribe(channel, conn):
        """
        Prints subscribe event. Adds a connection as a subscriber for a
        channel. Internally, a dictionary is kept for subscribers with
        channel_name as the key, and list of connections as value.
        """
        print(channel, "subscribe")
        if channel in SocketServer.subscribers:
            if conn not in SocketServer.subscribers[channel]:
                SocketServer.subscribers[channel].append(conn)
        else:
            SocketServer.subscribers[channel] = [conn]

main()


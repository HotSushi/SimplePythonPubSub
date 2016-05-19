import pubsub
import sys
import time

__author__ = "Sushant Raikar"
__email__ = "sushantraikar123@yahoo.com"

"""
    usage:
        python test.py PUB
            tests publisher implementation.

        python test.py SUB
            tests subscriber implementation. This uses Polling technique.
            calls recv() every second.

        python test.py SUBCB
            tests subscriber callback implementation. A print function is
            set as callback, which will be triggered when a message is
            receieved.
"""
def main():
    if len(sys.argv) <= 1:
        print("please provide test case through command line")
    else:
        arg = sys.argv[1]
        if arg == 'PUB':
            publisher_test()
        elif arg == 'SUB':
            subscriber_test()
        elif arg == 'SUBCB':
            subscriber_callback_test()
        else:
            print("args not recognized")

def publisher_test():
    pub = pubsub.Publisher(channel='foo')
    for i in range(10):
        pub.send("Sending %s" % i)
    pub.stop()

def subscriber_test():
    sub = pubsub.Subscriber(channel='foo')
    while True:
        time.sleep(1)
        print(sub.recv())

def subscriber_callback_test():
    sub = pubsub.Subscriber(channel='foo')
    def print_message(x): print(x)
    sub.set_callback(print_message)
    #keep program alive
    while(True):
        time.sleep(1)

main()
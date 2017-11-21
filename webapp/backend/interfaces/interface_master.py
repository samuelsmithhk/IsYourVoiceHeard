from tornado.web import Application

from interfaces.congressman_interface import CongressmanInterface
from interfaces.state_interface import StateInterface


def launch_interfaces(port):

    instance = Application([
        (r"/states/(.*)", StateInterface),
        (r"/congressmen/(\d*)", CongressmanInterface)
    ])

    try:
        instance.listen(port)
        print("IsYourVoiceHeard backend running on %d" % port)
    except OSError:
        print("%d already bound, terminating process" % port)
        exit(1)
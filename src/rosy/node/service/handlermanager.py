from rosy.node.callbackmanager import CallbackManager
from rosy.types import Service, ServiceCallback

ServiceHandlerManager = CallbackManager[Service, ServiceCallback]

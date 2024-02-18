from enum import Enum
from typing import Optional, Union, List

from docker.types import EndpointConfig


class DockerDrivers(Enum):
    """Enum for Docker driver type"""
    Disable = "none"
    Host = 'host'
    Bridge = 'bridge'


class ExegolNetworkMode(Enum):
    """Enum for user display"""
    disable = 'none'
    host = 'host'
    docker = 'bridge'
    nat = 'NAT'  # need pre-process
    attached = 'external'  # need pre-process


class ExegolNetwork:
    def __init__(self, net_mode: ExegolNetworkMode = ExegolNetworkMode.host, net_name: Optional[str] = None):
        self.__net_mode: ExegolNetworkMode = net_mode
        self.__net_name: str = net_name if net_name is not None else net_mode.value
        try:
            self.__docker_net_mode: DockerDrivers = DockerDrivers(self.__net_name)
        except ValueError:
            self.__docker_net_mode = DockerDrivers.Bridge

    @classmethod
    def instance_network(cls, mode: Union[ExegolNetworkMode, str], container_name: str):
        if type(mode) is str:
            return cls(net_mode=ExegolNetworkMode.attached, net_name=mode)
        else:
            if mode in [ExegolNetworkMode.host, ExegolNetworkMode.docker]:
                return cls(net_mode=mode)
            elif mode == ExegolNetworkMode.nat:
                raise NotImplementedError("WIP")
                return cls(net_mode=mode, net_name=container_name)
            elif mode == ExegolNetworkMode.disable:
                raise ValueError("Network disable cannot be created")
        raise NotImplementedError("This network type is not implemented yet.")

    @classmethod
    def parse_networks(cls, networks: dict, container_name: str) -> List["ExegolNetwork"]:
        results = []
        for network, config in networks.items():
            try:
                net_mode = ExegolNetworkMode(network)
            except ValueError:
                net_mode = ExegolNetworkMode.nat if network == container_name else ExegolNetworkMode.attached
            results.append(cls(net_mode=net_mode, net_name=network))

        return results

    def getNetworkConfig(self) -> (str, str):
        return self.__net_name, self.__docker_net_mode.value

    def getNetworkMode(self) -> ExegolNetworkMode:
        return self.__net_mode

    def getTextNetworkMode(self) -> str:
        if self.__net_mode is ExegolNetworkMode.attached:
            return self.__net_name
        return self.__net_mode.name

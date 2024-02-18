import os
from typing import Optional

from rich.prompt import Prompt

from exegol.config.EnvInfo import EnvInfo
from exegol.model.ContainerConfig import ContainerConfig
from exegol.model.ExegolImage import ExegolImage


class ExegolContainerTemplate:
    """Exegol template class used to create a new container"""

    def __init__(self, name: Optional[str], image: ExegolImage, config: Optional[ContainerConfig] = None, hostname: Optional[str] = None):
        if name is None:
            name = Prompt.ask("[bold blue][?][/bold blue] Enter the name of your new exegol container", default="default")
        assert name is not None
        if (EnvInfo.isWindowsHost() or EnvInfo.isMacHost()) and not name.startswith("exegol-"):
            # Force container as lowercase because the filesystem of windows / mac are case-insensitive => https://github.com/ThePorgs/Exegol/issues/167
            name = name.lower()
        container_name: str = name if name.startswith("exegol-") else f'exegol-{name}'
        self.name: str = name.replace('exegol-', '')
        self.image: ExegolImage = image
        self.config: ContainerConfig = config if config is not None else ContainerConfig(container_name=container_name, hostname=hostname)

    def __str__(self):
        """Default object text formatter, debug only"""
        return f"{self.name} - {self.image.getName()}{os.linesep}{self.config}"

    def prepare(self):
        """Prepare the model before creating the docker container"""
        self.config.prepareShare(self.name)

    def rollback(self):
        """Rollback change in case of container creation fail."""
        self.config.rollback_preparation(self.name)

    def getContainerName(self):
        return self.config.container_name

    def getDisplayName(self) -> str:
        """Getter of the container's name for TUI purpose"""
        if self.getContainerName() != self.config.hostname:
            return f"{self.name} [bright_black]({self.config.hostname})[/bright_black]"
        return self.name

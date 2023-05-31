"""
Artifact object module.
"""

from sdk.client.client import Client
from sdk.utils.utils import get_uiid
from sdk.entities.base_entity import Entity
from sdk.utils.common import API_CREATE, DTO_ARTF


class Artifact(Entity):
    """
    A class representing a artifact.
    """

    def __init__(
        self,
        project: str,
        name: str,
        key: str = None,
        path: str = None,
    ) -> None:
        """Initialize the Artifact instance."""
        self.project = project
        self.name = name
        self.key = key
        self.path = path
        self.id = get_uiid()
        self._api_create = API_CREATE.format(self.name, DTO_ARTF)

    def save(self, client: Client, overwrite: bool = False) -> dict:
        """
        Save artifact into backend.

        Returns
        -------
        dict
            Mapping representaion of Artifact from backend.

        """
        obj = {
            "name": self.name,
            "project": self.project,
            "kind": "",
            "spec": {
                "type": "artifact",
                "target": self.key,
                "source": self.path,
            },
            "type": "",
        }
        return self.save_object(client, obj, self._api_create, overwrite)

    def export(self, filename: str = None) -> None:
        """
        Export object as a YAML file.

        Parameters
        ----------
        filename : str, optional
            Name of the export YAML file. If not specified, the default value is used.

        Returns
        -------
        None

        """
        obj = self.to_dict()
        filename = filename if filename is not None else f"artifact_{self.name}.yaml"
        return self.export_object(filename, obj)

    def download(self, reader) -> str:
        ...

    def upload(self, writer) -> str:
        ...

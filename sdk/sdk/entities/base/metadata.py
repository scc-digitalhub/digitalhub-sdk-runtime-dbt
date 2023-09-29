"""
Entity metadata module.
"""
from sdk.entities.base.base import ModelObj


class Metadata(ModelObj):
    """
    A class representing the metadata of an entity.
    """

    def __init__(
        self, project: str, created: str | None = None, updated: str | None = None
    ) -> None:
        """
        Constructor.

        Parameters
        ----------
        project : str
            Name of the project.
        created : str
            Created date.
        updated : str
            Updated date.
        """
        self.project = project
        self.created = created
        self.updated = updated

    @classmethod
    def from_dict(cls, obj: dict | None = None) -> "Metadata":
        """
        Return entity metadata object from dictionary.

        Parameters
        ----------
        obj : dict
            A dictionary containing the attributes of the entity metadata.

        Returns
        -------
        Metadata
            An entity metadata object.
        """
        if obj is None:
            obj = {}
        return cls(**obj)

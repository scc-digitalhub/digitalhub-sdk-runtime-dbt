"""
Abstract entity module.
"""
from __future__ import annotations

from abc import ABCMeta, abstractmethod

from digitalhub_core.entities._base.base import ModelObj


class Entity(ModelObj, metaclass=ABCMeta):
    """
    Abstract class for entities.
    """

    # Attributes to render as dict. Need to be expanded in subclasses.
    _obj_attr = ["kind", "metadata", "spec", "status"]

    @abstractmethod
    def save(self, update: bool = False) -> dict:
        """
        Abstract save method.
        """

    @abstractmethod
    def export(self, filename: str | None = None) -> None:
        """
        Abstract save method.
        """

    def to_dict(
        self,
        include_all_non_private: bool = False,
    ) -> dict:
        """
        Return object as dict with all keys.

        Parameters
        ----------
        include_all_non_private : bool
            Whether to include all non-private attributes. If False, only
            attributes in the _obj_attr list will be included.

        Returns
        -------
        dict
            A dictionary containing the attributes of the entity instance.
        """
        dict_ = super().to_dict()
        if include_all_non_private:
            return dict_
        return {k: v for k, v in dict_.items() if k in self._obj_attr}

    @classmethod
    def from_dict(
        cls,
        obj: dict,
        validate: bool = True,
    ) -> "Entity":
        """
        Create object instance from a dictionary.

        Parameters
        ----------
        obj : dict
            Dictionary to create object from.
        validate : bool
            Flag to indicate if arguments validation must be ignored.

        Returns
        -------
        Self
            Self instance.
        """
        parsed_dict = cls._parse_dict(
            obj,
            validate=validate,
        )
        return cls(**parsed_dict)

    @staticmethod
    @abstractmethod
    def _parse_dict(
        obj: dict,
        validate: bool = True,
    ) -> dict:
        """
        Get dictionary and parse it to a valid entity dictionary.
        """

    def __repr__(self) -> str:
        """
        Return string representation of the entity object.

        Returns
        -------
        str
            A string representing the entity instance.
        """
        return str(self.to_dict(include_all_non_private=True))

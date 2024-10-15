from __future__ import annotations

from digitalhub.entities._base.material.spec import MaterialSpec, MaterialValidator


class ArtifactSpec(MaterialSpec):
    """
    Artifact specification.
    """


class ArtifactValidator(MaterialValidator):
    """
    Artifact base parameters.
    """
package it.smartcommunitylabdhub.core.models.entities.run.metadata;

import it.smartcommunitylabdhub.core.models.base.metadata.BaseMetadata;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class RunBaseMetadata extends BaseMetadata {

    @NotNull
    String name;
}
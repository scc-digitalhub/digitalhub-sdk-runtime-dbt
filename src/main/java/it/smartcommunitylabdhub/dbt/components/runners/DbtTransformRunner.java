package it.smartcommunitylabdhub.dbt.components.runners;

import it.smartcommunitylabdhub.core.annotations.infrastructure.RunnerComponent;
import it.smartcommunitylabdhub.core.components.infrastructure.factories.runnables.Runnable;
import it.smartcommunitylabdhub.core.components.infrastructure.factories.runners.Runner;
import it.smartcommunitylabdhub.core.components.infrastructure.factories.specs.SpecRegistry;
import it.smartcommunitylabdhub.core.components.infrastructure.runnables.K8sJobRunnable;
import it.smartcommunitylabdhub.core.models.accessors.utils.RunAccessor;
import it.smartcommunitylabdhub.core.models.accessors.utils.RunUtils;
import it.smartcommunitylabdhub.core.models.base.interfaces.Spec;
import it.smartcommunitylabdhub.core.models.entities.function.specs.FunctionDbtSpec;
import it.smartcommunitylabdhub.core.models.entities.run.RunDTO;
import it.smartcommunitylabdhub.core.models.entities.run.specs.RunRunSpec;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.Optional;


@RunnerComponent(runtime = "dbt", task = "transform")
public class DbtTransformRunner implements Runner {

    @Autowired
    SpecRegistry<? extends Spec> specRegistry;

    @Override
    public Runnable produce(RunDTO runDTO) {

        return Optional.ofNullable(runDTO)
                .map(this::validateRunDTO)
                .orElseThrow(() -> new IllegalArgumentException("Invalid runDTO"));

    }

    private K8sJobRunnable validateRunDTO(RunDTO runDTO) {

        // Retrieve run spec from registry
        RunRunSpec runRunSpec = (RunRunSpec) specRegistry.createSpec(
                runDTO.getKind(), runDTO.getSpec()
        );
        // Create accessor for run
        RunAccessor runAccessor = RunUtils.parseRun(runRunSpec.getTask());

        // Retrieve function spec from registry
        FunctionDbtSpec functionDbtSpec = (FunctionDbtSpec) specRegistry.createSpec(
                runAccessor.getRuntime(), runDTO.getSpec()
        );

        // Check for valid parameters image, command and args
        if (functionDbtSpec.getImage() == null) {
            throw new IllegalArgumentException(
                    "Invalid argument: image not found in runDTO spec");
        }

        if (functionDbtSpec.getCommand() == null) {
            throw new IllegalArgumentException(
                    "Invalid argument: command not found in runDTO spec");
        }

        if (functionDbtSpec.getExtraSpecs() == null) {
            throw new IllegalArgumentException(
                    "Invalid argument: args not found in runDTO spec");
        }

        K8sJobRunnable k8sJobRunnable = K8sJobRunnable.builder()
                .runtime(runAccessor.getRuntime())
                .task(runAccessor.getTask())
                .image(functionDbtSpec.getImage())
                .command(functionDbtSpec.getCommand())
                .args(functionDbtSpec.getArgs().toArray(String[]::new))
                .envs(Map.of(
                        "PROJECT_NAME", runDTO.getProject(),
                        "RUN_ID", runDTO.getId()))
                .state(runDTO.getState())
                .build();

        k8sJobRunnable.setId(runDTO.getId());
        k8sJobRunnable.setProject(runDTO.getProject());

        return k8sJobRunnable;
    }


}

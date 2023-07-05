package it.smartcommunitylabdhub.core.components.runnables.pollers.workflows.functions;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import it.smartcommunitylabdhub.core.components.fsm.StateMachine;
import it.smartcommunitylabdhub.core.components.fsm.enums.RunEvent;
import it.smartcommunitylabdhub.core.components.fsm.enums.RunState;
import it.smartcommunitylabdhub.core.components.fsm.types.RunStateMachine;
import it.smartcommunitylabdhub.core.components.runnables.pollers.workflows.factory.Workflow;
import it.smartcommunitylabdhub.core.components.runnables.pollers.workflows.factory.WorkflowFactory;
import it.smartcommunitylabdhub.core.exceptions.StopPoller;
import it.smartcommunitylabdhub.core.models.dtos.RunDTO;
import it.smartcommunitylabdhub.core.services.interfaces.LogService;
import it.smartcommunitylabdhub.core.services.interfaces.RunService;
import it.smartcommunitylabdhub.core.utils.MapUtils;

@Component
public class RunWorkflowBuilder extends BaseWorkflowBuilder {

    @Value("${mlrun.api.run-url}")
    private String runUrl;

    @Value("${mlrun.api.log-url}")
    private String logUrl;

    private final RunService runService;
    private final LogService logService;
    private final RunStateMachine runStateMachine;
    private final RestTemplate restTemplate;
    private StateMachine<RunState, RunEvent, Map<String, Object>> stateMachine;

    ObjectMapper objectMapper = new ObjectMapper();

    public RunWorkflowBuilder(
            RunService runService,
            LogService logService,
            RunStateMachine runStateMachine) {
        this.runService = runService;
        this.logService = logService;
        this.restTemplate = new RestTemplate();
        this.runStateMachine = runStateMachine;
    }

    @SuppressWarnings("unchecked")
    public Workflow buildWorkflow(RunDTO runDTO) {
        Function<Object[], Object> getRunUpdate = params -> {

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>(headers);

            String requestUrl = params[0].toString()
                    .replace("{project}", ((RunDTO) params[1]).getProject())
                    .replace("{uid}", ((RunDTO) params[1]).getExtra()
                            .get("mlrun_run_uid").toString());

            ResponseEntity<Map<String, Object>> response = restTemplate
                    .exchange(requestUrl, HttpMethod.GET, entity,
                            responseType);

            try {
                System.out.println(objectMapper.writeValueAsString(response));
            } catch (Exception e) {
            }

            return Optional.ofNullable(response.getBody()).map(body -> {
                Map<String, Object> status = (Map<String, Object>) ((Map<String, Object>) body.get("data"))
                        .get("status");

                if (!stateMachine.getCurrentState()
                        .equals(RunState.valueOf(status.get("state").toString().toUpperCase()))) {

                    String mlrunState = status.get("state").toString();
                    stateMachine.processEvent(
                            Optional.ofNullable(RunEvent.valueOf(mlrunState.toUpperCase()))
                                    .orElseGet(() -> RunEvent.ERROR),
                            Optional.empty());

                    // Update run state
                    runDTO.setState(stateMachine.getCurrentState().name());

                    // Store change
                    this.runService.save(runDTO);

                } else if (stateMachine.getCurrentState().equals(RunState.COMPLETED)) {
                    System.out.println("Poller complete SUCCESSFULLY. Get log and stop poller now");

                    // TODO: Store log from mlrun.
                    Optional.ofNullable(response.getBody()).ifPresent(b -> {
                        // Get run uid from mlrun.
                        MapUtils.getNestedFieldValue(b, "data").ifPresent(data -> {
                            MapUtils.getNestedFieldValue(data, "metadata").ifPresent(metadata -> {
                                String uid = (String) metadata.get("uid");

                                // Call mlrun api to get log of specific run uid.
                                ResponseEntity<String> logResponse = restTemplate
                                        .exchange(
                                                logUrl.replace("{project}", runDTO.getProject()).replace("{uid}", uid),
                                                HttpMethod.GET, entity,
                                                String.class);

                                // Create and store log
                                System.out.println(logResponse.getBody());

                                // TODO: Store log information in logs table
                                // FIXME
                            });
                        });

                    });

                    throw new StopPoller("Poller complete successful!");
                } else if (stateMachine.getCurrentState().equals(RunState.ERROR)) {
                    System.out.println("Poller complete with ERROR. Get log and stop poller now");

                    // TODO: Store log from mlrun.
                    throw new StopPoller("Error state reached stop poller");

                }
                return null;
            }).orElseGet(() -> null);

        };

        // Init run state machine considering current state and context.
        stateMachine = runStateMachine.create(RunState.valueOf(runDTO.getState()), new HashMap<>());
        // (Map<String, Object>) runDTO.getExtra().get("context"));

        stateMachine.processEvent(RunEvent.PREPARE, Optional.empty());

        // Define workflow steps
        return WorkflowFactory.builder().step(getRunUpdate, runUrl, runDTO).build();
    }

}

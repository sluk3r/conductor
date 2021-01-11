package com.netflix.conductor.core.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.mapper.*;
import com.netflix.conductor.dao.MetadataDAO;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DebugTask {
    private static ObjectMapper objectMapper = new ObjectMapper();
    private DeciderService deciderService;
    private ParametersUtils parametersUtils;


    @Before
    public void setUp() {
        MetadataDAO metadataDAO = mock(MetadataDAO.class);

        TaskDef taskDef = new TaskDef();
        WorkflowDef workflowDef = new WorkflowDef();
        when(metadataDAO.getTaskDef(any())).thenReturn(taskDef);
        when(metadataDAO.getLatest(any())).thenReturn(workflowDef);


        parametersUtils = new ParametersUtils();
        Map<String, TaskMapper> taskMappers = new HashMap<>();
        taskMappers.put("DECISION", new DecisionTaskMapper());
        taskMappers.put("DYNAMIC", new DynamicTaskMapper(parametersUtils, metadataDAO));
        taskMappers.put("FORK_JOIN", new ForkJoinTaskMapper());
        taskMappers.put("JOIN", new JoinTaskMapper());
        taskMappers.put("FORK_JOIN_DYNAMIC", new ForkJoinDynamicTaskMapper(parametersUtils, objectMapper));
        taskMappers.put("USER_DEFINED", new UserDefinedTaskMapper(parametersUtils, metadataDAO));
        taskMappers.put("SIMPLE", new SimpleTaskMapper(parametersUtils, metadataDAO));
        taskMappers.put("SUB_WORKFLOW", new SubWorkflowTaskMapper(parametersUtils, metadataDAO));
        taskMappers.put("EVENT", new EventTaskMapper(parametersUtils)); //这些Mapper是解析JSON串时， 使用的么？
        taskMappers.put("WAIT", new WaitTaskMapper(parametersUtils));

        deciderService = new DeciderService(metadataDAO, taskMappers);
    }



    @Test
    public void testFork() throws Exception {
        InputStream stream = TestDeciderOutcomes.class.getResourceAsStream("/test.json");
        Workflow workflow = objectMapper.readValue(stream, Workflow.class);

        InputStream defs = TestDeciderOutcomes.class.getResourceAsStream("/def.json");
        WorkflowDef def = objectMapper.readValue(defs, WorkflowDef.class);



        DeciderService.DeciderOutcome outcome = deciderService.decide(workflow, def);
        assertFalse(outcome.isComplete);
        assertEquals(5, outcome.tasksToBeScheduled.size());
        assertEquals(1, outcome.tasksToBeUpdated.size());
    }




    @Test
    public void test_work_flow_defintion() throws Exception {
        InputStream stream = TestDeciderOutcomes.class.getResourceAsStream("/workflow_definition.json");
        Workflow workflow = objectMapper.readValue(stream, Workflow.class);

        InputStream defs = TestDeciderOutcomes.class.getResourceAsStream("/def.json");
        WorkflowDef def = objectMapper.readValue(defs, WorkflowDef.class);

        DeciderService.DeciderOutcome outcome = deciderService.decide(workflow, def);
        assertFalse(outcome.isComplete);
        assertEquals(5, outcome.tasksToBeScheduled.size());
        assertEquals(1, outcome.tasksToBeUpdated.size());
    }
}


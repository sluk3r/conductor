/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.tests.wxcTest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskDef.RetryLogic;
import com.netflix.conductor.common.metadata.tasks.TaskDef.TimeoutPolicy;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.*;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask.Type;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.Workflow.WorkflowStatus;
import com.netflix.conductor.core.WorkflowContext;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.SystemTaskType;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.WorkflowSweeper;
import com.netflix.conductor.core.execution.tasks.SubWorkflow;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.service.ExecutionService;
import com.netflix.conductor.service.MetadataService;
import com.netflix.conductor.tests.utils.TestRunner;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.util.CollectionUtil;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.netflix.conductor.common.metadata.tasks.Task.Status.COMPLETED;
import static org.junit.Assert.*;

/**
 * @author Viren
 */
@RunWith(TestRunner.class)
public class WorkflowServiceWxcTest {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowServiceWxcTest.class);
    private static ObjectMapper objectMapper = new ObjectMapper();


    private static final int RETRY_COUNT = 1;

    @Inject
    private WorkflowExecutor workflowExecutor;

    @Inject
    private ExecutionService workflowExecutionService;


    @Inject
    private MetadataService metadataService;


    @Before
    public void setUp() {
        registerTaskDef(); //需要先注册下名字， 尽管从表面上看起来， 这里的注册并没有触发多大实际的业务操作。
    }

    private void registerTaskDef() {
        String[] taskNames = {"junit_task_1", "junit_task_2"};

        for (int i = 0; i < taskNames.length; i++) {
            TaskDef task = new TaskDef();
            task.setName(taskNames[i]);
            task.setTimeoutSeconds(120);
            task.setRetryCount(RETRY_COUNT);

            metadataService.registerTaskDef(Collections.singletonList(task));
        }
    }


    @Test
    public void test_workflow_run() throws Exception {
//        String filePath = "wxcDumpedJsonDefinition/junit_test_wf-definition.json";
        String filePath = "F:\\openSrc\\conductor\\test-harness\\src\\test\\resources\\wxcDumpedJsonDefinition\\junit_test_wf-definition.json";

        File file = new File(filePath);
        assertTrue(file.exists());

//        List<String> lines = IOUtils.readLines(new FileReader(file));

//        InputStream stream = WorkflowServiceWxcTest.class.getResourceAsStream(filePath);
        InputStream stream = new FileInputStream(file);
        WorkflowDef workflowDef = objectMapper.readValue(stream, WorkflowDef.class);

        LinkedList<WorkflowTask> tasks = workflowDef.getTasks();
        assertFalse(tasks.isEmpty());
        assertTrue("junit_task_1".equals(tasks.get(0).getName()));

        assertEquals("junit_test_wf", workflowDef.getName());

        //必须先注册， 否则会抛错
        metadataService.updateWorkflowDef(workflowDef);

        //业务参数准备
        Map<String, Object> input = new HashMap<>();
        String inputParam1 = "p1 value";
        input.put("param1", inputParam1);
        input.put("param2", "p2 value");

        String correlationId = "unit_test_1";
        String workflowInstanceId = workflowExecutor.startWorkflow(workflowDef.getName(), 1, correlationId, input);  //wxc 2018-7-12:9:04:40 终于看到这里的业务数据输入了。

        Workflow es = workflowExecutionService.getExecutionStatus(workflowInstanceId, true);
        assertNotNull(es);
        System.out.println("ReasonForIncompletion: " + es.getReasonForIncompletion());
        assertEquals(es.getReasonForIncompletion(), WorkflowStatus.RUNNING, es.getStatus());

    }
    
}

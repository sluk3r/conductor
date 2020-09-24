/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.counductor.integration.test

import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.run.Workflow
import com.netflix.conductor.core.execution.WorkflowExecutor
import com.netflix.conductor.core.execution.WorkflowRepairService
import com.netflix.conductor.dao.ExecutionDAO
import com.netflix.conductor.dao.QueueDAO
import com.netflix.conductor.service.ExecutionService
import com.netflix.conductor.test.util.MockQueueDAOModule
import com.netflix.conductor.test.util.WorkflowTestUtil
import spock.guice.UseModules
import spock.lang.Specification

import javax.inject.Inject

import static com.netflix.conductor.test.util.WorkflowTestUtil.verifyPolledAndAcknowledgedTask

@UseModules(MockQueueDAOModule)
class TaskResiliencyExecutionDAOSpec extends Specification {

    @Inject
    ExecutionService workflowExecutionService

    @Inject
    WorkflowExecutor workflowExecutor

    @Inject
    WorkflowTestUtil workflowTestUtil

    @Inject
    WorkflowRepairService workflowRepairService

    @Inject
    QueueDAO queueDAO

    @Inject
    ExecutionDAO executionDAO

    def SIMPLE_TWO_TASK_WORKFLOW = 'integration_test_wf'

    def setup() {
        workflowTestUtil.taskDefinitions()
        workflowTestUtil.registerWorkflows(
                'simple_workflow_1_integration_test.json'
        )
    }

    def cleanup() {
        workflowTestUtil.clearWorkflows()
    }

    def "Test Verify that a workflow recovers and completes on schedule task failure from persistence failure"() {
        // TODO WIP
        when: "Start a simple workflow"
        def workflowInstanceId = workflowExecutor.startWorkflow(SIMPLE_TWO_TASK_WORKFLOW, 1,
                '', [:], null, null, null)

        then: "Retrieve the workflow"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        and: "The decider queue has one task that is ready to be polled"
        queueDAO.getSize(WorkflowExecutor.DECIDER_QUEUE) == 1

        // Simulate persistence failure when creating a new task, after completing first task
        when: "The first task 'integration_task_1' is polled and completed"
        def task1Try1 = workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker')

        then: "Verify that the task was polled and acknowledged"
        1 * executionDAO.createTasks(_) >> { throw new IllegalStateException("Create tasks failed from Spy") }
        verifyPolledAndAcknowledgedTask(task1Try1)

        and: "Ensure that the workflow is RUNNING, but next task is not SCHEDULED"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
        }

        when: "workflow is decided again without persistence failures"
        workflowExecutor.decide(workflowInstanceId)

        then: "Ensure next task is scheduled"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 2
            tasks[1].taskType == 'integration_task_2'
            tasks[1].status == Task.Status.SCHEDULED
        }
    }

    def "Verify that a workflow recovers and completes on schedule task failure from persistence failure"() {
        // TODO WIP
        when: "Start a simple workflow"
        def workflowInstanceId = workflowExecutor.startWorkflow(SIMPLE_TWO_TASK_WORKFLOW, 1,
                '', [:], null, null, null)

        then: "Retrieve the workflow"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.RUNNING
            tasks.size() == 1
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.SCHEDULED
        }

        and: "The decider queue has one task that is ready to be polled"
        queueDAO.getSize(WorkflowExecutor.DECIDER_QUEUE) == 1

        // Simulate persistence failure when creating a new task, after completing first task
        when: "The first task 'integration_task_1' is polled and completed"
//        dynamicFailureProbability.setMethodName("createTasks")
//        dynamicFailureProbability.setFailureProbability(1)
        def task1Try1 = workflowTestUtil.pollAndCompleteTask('integration_task_1', 'task1.integration.worker')

        then: "Verify that the task was polled and acknowledged"
        verifyPolledAndAcknowledgedTask(task1Try1)

        and: "Ensure that the workflow is TERMINATED"
        with(workflowExecutionService.getExecutionStatus(workflowInstanceId, true)) {
            status == Workflow.WorkflowStatus.FAILED
            tasks[0].taskType == 'integration_task_1'
            tasks[0].status == Task.Status.COMPLETED
        }
    }
}

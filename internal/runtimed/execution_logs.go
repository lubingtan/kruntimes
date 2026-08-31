package runtimed

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"time"

	pb "github.com/kruntimes/kruntimes/api/runtime/v1"
	"github.com/kruntimes/kruntimes/api/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type executionOutput struct {
	stdout string
	stderr string
}

type executionLogLine struct {
	RunUID               string `json:"run_uid"`
	AssignedPodUID       string `json:"assigned_pod_uid,omitempty"`
	RunName              string `json:"run_name"`
	Namespace            string `json:"namespace"`
	Runtime              string `json:"runtime"`
	Pod                  string `json:"pod"`
	Stream               string `json:"stream"`
	Message              string `json:"message"`
	InvocationID         string `json:"invocation_id,omitempty"`
	Operation            string `json:"operation,omitempty"`
	Outcome              string `json:"outcome,omitempty"`
	StatusCode           string `json:"status_code,omitempty"`
	ExitCode             *int32 `json:"exit_code,omitempty"`
	TimedOut             bool   `json:"timed_out,omitempty"`
	DurationMilliseconds int64  `json:"duration_milliseconds,omitempty"`
}

func outputFromStatus(resp *pb.StatusResponse) executionOutput {
	if resp == nil {
		return executionOutput{}
	}
	return executionOutput{stdout: resp.Stdout, stderr: resp.Stderr}
}

func (c *Controller) emitExecutionOutput(run *v1alpha1.Run, output executionOutput) {
	if run == nil {
		return
	}
	writer := c.ExecutionLogWriter
	if writer == nil {
		writer = os.Stdout
	}

	c.logMu.Lock()
	defer c.logMu.Unlock()
	c.emitStream(writer, run, "stdout", output.stdout)
	c.emitStream(writer, run, "stderr", output.stderr)
}

func (c *Controller) emitStream(writer io.Writer, run *v1alpha1.Run, stream, content string) {
	for _, message := range strings.Split(strings.TrimSuffix(content, "\n"), "\n") {
		if message == "" {
			continue
		}
		writeExecutionLogLine(writer, executionLogLineFor(run, c.PodName, stream, strings.TrimSuffix(message, "\r")))
	}
}

func executionLogLineFor(run *v1alpha1.Run, pod, stream, message string) executionLogLine {
	return executionLogLine{
		RunUID:         string(run.UID),
		AssignedPodUID: run.Status.AssignedPodUID,
		RunName:        run.Name,
		Namespace:      run.Namespace,
		Runtime:        run.Spec.Runtime,
		Pod:            pod,
		Stream:         stream,
		Message:        message,
	}
}

func writeExecutionLogLine(writer io.Writer, line executionLogLine) {
	encoded, err := json.Marshal(line)
	if err != nil {
		return
	}
	_, _ = writer.Write(append(encoded, '\n'))
}

func (c *Controller) emitFunctionInvocationAudit(run *v1alpha1.Run, invocationID string, invokeErr error, duration time.Duration) {
	if run == nil {
		return
	}
	writer := c.ExecutionLogWriter
	if writer == nil {
		writer = os.Stdout
	}
	line := executionLogLineFor(run, c.PodName, "audit", "function invocation completed")
	line.InvocationID = invocationID
	line.Operation = "function_invoke"
	line.DurationMilliseconds = duration.Milliseconds()
	if invokeErr == nil {
		line.Outcome = "succeeded"
	} else {
		line.StatusCode = status.Code(invokeErr).String()
		switch status.Code(invokeErr) {
		case codes.Canceled:
			line.Outcome = "cancelled"
		case codes.DeadlineExceeded:
			line.Outcome = "timed_out"
		default:
			line.Outcome = "failed"
		}
	}
	c.logMu.Lock()
	defer c.logMu.Unlock()
	writeExecutionLogLine(writer, line)
}

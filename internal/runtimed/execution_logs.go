package runtimed

import (
	"encoding/json"
	"io"
	"os"
	"strings"

	pb "github.com/kruntimes/kruntimes/api/runtime/v1"
	"github.com/kruntimes/kruntimes/api/v1alpha1"
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

package benchrun

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestWave6OrchestratorStatusDoesNotTreatMissingPIDAsRunning(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("status is intentionally Linux-only")
	}
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(parent, "state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(parent, "output")
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(state, "input")
	if err := os.Mkdir(input, 0o700); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(input, "live.json")
	if err := os.WriteFile(config, []byte(`{"pair_id":"p","seed":17,"family":"H-append","scale":300,"ramp_seconds":30,"settle_seconds":30,"warmup_seconds":60,"warmup_mutations":0,"measure_seconds":180,"grace_seconds":30,"targets":[]}`), 0o400); err != nil {
		t.Fatal(err)
	}
	smoke := filepath.Join(input, "benchsmoke")
	if err := os.WriteFile(smoke, []byte("#!/bin/sh\nprintf '%s\\n' '{\"valid\":false,\"pending_evidence\":true,\"completed_attempts\":0,\"failed_attempts\":0,\"total_attempts\":21}'\n"), 0o500); err != nil {
		t.Fatal(err)
	}
	identity := "state_root=" + state + "\nconfig_snapshot=" + config + "\nconfig_sha256=" + fileSHA256(t, config) + "\nbenchsmoke_snapshot=" + smoke + "\nbenchsmoke_sha256=" + fileSHA256(t, smoke) + "\nfull_output=" + output + "\noutput_lock_path=" + output + ".wave6-controller.lock\ncollector_namespace=net:[4026533000]\ntarget_v1_namespace=net:[4026533000]\ntarget_v2_reference_namespace=net:[4026533000]\ntarget_v2_current_namespace=net:[4026533000]\ninitial_namespace=net:[4026533001]\n"
	lockPath := output + ".wave6-controller.lock"
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "identity.txt"), []byte(identity), 0o600); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join("..", "..", "benchmarks", "scripts", "wave6-orchestrate.sh")
	cmd := exec.Command("bash", script, "status", "--json")
	cmd.Env = append(os.Environ(), "WAVE6_STATE_DIR="+state)
	b, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	var status struct {
		Phase   string `json:"phase"`
		Running bool   `json:"running"`
	}
	if err := json.Unmarshal(b, &status); err != nil {
		t.Fatal(err)
	}
	if status.Running || status.Phase != "not_started" {
		t.Fatalf("missing PID reported as active: %+v", status)
	}
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	cmd = exec.Command("bash", script, "status", "--json")
	cmd.Env = append(os.Environ(), "WAVE6_STATE_DIR="+state)
	if b, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(b), "output lock") {
		t.Fatalf("deleted output lock was accepted: err=%v output=%s", err, b)
	}
}

func TestWave6OrchestratorUsesStableSnapshotsAndOutputLaunchLock(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "benchmarks", "scripts", "wave6-orchestrate.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(b)
	for _, required := range []string{"flock -n", "-snapshot-source", "output_lock_path=", "Recheck"} {
		if !strings.Contains(script, required) {
			t.Fatalf("orchestrator does not contain required %q contract", required)
		}
	}
}

func TestWave6StatusUsesLiveWrapperDuringChildPublication(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "benchmarks", "scripts", "wave6-orchestrate.sh"))
	if err != nil {
		t.Fatal(err)
	}
	script := string(b)
	for _, required := range []string{"wrapper_running", "running=$wrapper_running", "child publication"} {
		if !strings.Contains(script, required) {
			t.Fatalf("status lifecycle does not preserve live-wrapper state: missing %q", required)
		}
	}
}

func TestWave6StatusAndProgressFailClosedOffLinux(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "benchmarks", "scripts", "wave6-orchestrate.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "status requires Linux") {
		t.Fatal("status does not fail closed off Linux")
	}
}

func TestWave6OrchestratorOutputLockSerializesLaunch(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux dirfd lock helper")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "output")
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(root, "benchsmoke")
	build := exec.Command("go", "build", "-o", helper, "../../tools/benchsmoke")
	if outputBytes, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build lock helper: %v: %s", err, outputBytes)
	}
	lockPath := output + ".wave6-controller.lock"
	marker := filepath.Join(root, "lock-held")
	hold := `"$1" -lock-output "$2" -- /bin/sh -c 'touch "$1"; sleep 1' lock-holder "$3"`
	first := exec.Command("bash", "-c", hold, "wave6-lock-test", helper, lockPath, marker)
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	defer first.Wait()
	// The first process has to reach the secure helper before the contender is started.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, statErr := os.Stat(marker); statErr == nil {
			break
		}
		if time.Now().After(deadline) {
			first.Process.Kill()
			t.Fatal("first controller did not acquire its output lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	second := exec.Command("bash", "-c", hold, "wave6-lock-test", helper, lockPath, filepath.Join(root, "second-marker"))
	if outputBytes, err := second.CombinedOutput(); err == nil {
		t.Fatalf("concurrent controller acquired output lock: %s", outputBytes)
	}
}

func TestWave6StatusKeepsRunningPhaseDuringChildLaunchWindow(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("status is intentionally Linux-only")
	}
	state, _ := wave6StatusFixture(t)
	wrapper, exe, ticks := startStatusProcess(t, "5")
	defer func() {
		_ = wrapper.Process.Kill()
		_ = wrapper.Wait()
	}()
	writeStatusWrapperIdentity(t, state, wrapper.Process.Pid, exe, ticks)
	if err := os.WriteFile(filepath.Join(state, "full.started_at_epoch"), []byte("1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	status := runWave6Status(t, state)
	if status.Phase != "running" || !status.Running {
		t.Fatalf("live wrapper during child launch was reported as interrupted: %+v", status)
	}
}

func TestWave6StatusKeepsRunningPhaseDuringChildExitPublicationWindow(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("status is intentionally Linux-only")
	}
	state, _ := wave6StatusFixture(t)
	wrapper, wrapperExe, wrapperTicks := startStatusProcess(t, "5")
	defer func() {
		_ = wrapper.Process.Kill()
		_ = wrapper.Wait()
	}()
	writeStatusWrapperIdentity(t, state, wrapper.Process.Pid, wrapperExe, wrapperTicks)
	child, childExe, childTicks := startStatusProcess(t, "0.1")
	childPID := child.Process.Pid
	if err := child.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "full.child.pid"), []byte(fmt.Sprintf("%d\n", childPID)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "full.child.start_ticks"), []byte(childTicks+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "full.child.pgid"), []byte(fmt.Sprintf("%d\n", childPID)), 0o600); err != nil {
		t.Fatal(err)
	}
	// The child identity uses the configured benchrun executable. The process
	// is gone, so its saved executable evidence represents the exit-publication
	// window without requiring an actual benchmark child.
	_ = childExe
	if err := os.WriteFile(filepath.Join(state, "full.started_at_epoch"), []byte("1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	status := runWave6Status(t, state)
	if status.Phase != "running" || !status.Running {
		t.Fatalf("live wrapper during child exit publication was reported as interrupted: %+v", status)
	}
}

type wave6StatusResult struct {
	Phase   string `json:"phase"`
	Running bool   `json:"running"`
}

func wave6StatusFixture(t *testing.T) (string, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(root, "state")
	output := filepath.Join(root, "output")
	input := filepath.Join(state, "input")
	for _, dir := range []string{state, output, input} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	config := filepath.Join(input, "live.json")
	configJSON := `{"pair_id":"p","seed":17,"family":"H-append","scale":300,"ramp_seconds":30,"settle_seconds":30,"warmup_seconds":60,"warmup_mutations":0,"measure_seconds":180,"grace_seconds":30,"targets":[]}`
	if err := os.WriteFile(config, []byte(configJSON), 0o400); err != nil {
		t.Fatal(err)
	}
	smoke := filepath.Join(input, "benchsmoke")
	if err := os.WriteFile(smoke, []byte("#!/bin/sh\nprintf '%s\\n' '{\"valid\":false,\"pending_evidence\":true,\"completed_attempts\":0,\"failed_attempts\":0,\"total_attempts\":21}'\n"), 0o500); err != nil {
		t.Fatal(err)
	}
	lock := output + ".wave6-controller.lock"
	if err := os.WriteFile(lock, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	benchrunPath := "/usr/bin/sleep"
	identity := "state_root=" + state + "\nconfig_snapshot=" + config + "\nconfig_sha256=" + fileSHA256(t, config) + "\nbenchsmoke_snapshot=" + smoke + "\nbenchsmoke_sha256=" + fileSHA256(t, smoke) + "\nbenchrun_snapshot=" + benchrunPath + "\nbenchrun_sha256=" + fileSHA256(t, benchrunPath) + "\nfull_output=" + output + "\noutput_lock_path=" + lock + "\ncollector_namespace=net:[4026533000]\ntarget_v1_namespace=net:[4026533000]\ntarget_v2_reference_namespace=net:[4026533000]\ntarget_v2_current_namespace=net:[4026533000]\ninitial_namespace=net:[4026533001]\n"
	if err := os.WriteFile(filepath.Join(state, "identity.txt"), []byte(identity), 0o600); err != nil {
		t.Fatal(err)
	}
	return state, output
}

func startStatusProcess(t *testing.T, duration string) (*exec.Cmd, string, string) {
	t.Helper()
	cmd := exec.Command("setsid", "sleep", duration)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		exe, exeErr := os.Readlink(fmt.Sprintf("/proc/%d/exe", cmd.Process.Pid))
		stat, statErr := os.ReadFile(fmt.Sprintf("/proc/%d/stat", cmd.Process.Pid))
		fields := strings.Fields(string(stat))
		if exeErr == nil && statErr == nil && len(fields) > 21 {
			return cmd, exe, fields[21]
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal("status process did not publish proc identity")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func writeStatusWrapperIdentity(t *testing.T, state string, pid int, exe, ticks string) {
	t.Helper()
	for name, value := range map[string]string{
		"full.wrapper.pid":         fmt.Sprintf("%d\n", pid),
		"full.wrapper.start_ticks": ticks + "\n",
		"full.wrapper.exe":         exe + "\n",
		"full.wrapper.exe_sha256":  fileSHA256(t, exe) + "\n",
	} {
		if err := os.WriteFile(filepath.Join(state, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func runWave6Status(t *testing.T, state string) wave6StatusResult {
	t.Helper()
	script, err := filepath.Abs(filepath.Join("..", "..", "benchmarks", "scripts", "wave6-orchestrate.sh"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", script, "status", "--json")
	cmd.Env = append(os.Environ(), "WAVE6_STATE_DIR="+state)
	b, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("status failed: %v: %s", err, b)
	}
	var status wave6StatusResult
	if err := json.Unmarshal(b, &status); err != nil {
		t.Fatal(err)
	}
	return status
}

func fileSHA256(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

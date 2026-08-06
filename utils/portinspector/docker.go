package portinspector

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// dockerContainerInfo is the minimal read-only view of a container.
type dockerContainerInfo struct {
	ID   string
	Name string
}

// dockerClient talks to the docker Engine API over the unix socket with
// GET requests only (spec §5.3-4: read-only, no shell, no exec).
type dockerClient struct {
	baseURL        string
	socket         string
	timeout        time.Duration
	dial           func(ctx context.Context, network, addr string) (net.Conn, error)
	containerNames map[string]string // id -> first name
}

func newDockerClient() *dockerClient {
	return &dockerClient{
		baseURL: "http://docker",
		socket:  "/var/run/docker.sock",
		timeout: 2 * time.Second,
	}
}

func (d *dockerClient) httpClient() *http.Client {
	dial := d.dial
	if dial == nil {
		dial = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return (&net.Dialer{Timeout: d.timeout}).DialContext(ctx, "unix", d.socket)
		}
	}
	return &http.Client{
		Timeout: d.timeout,
		Transport: &http.Transport{
			DialContext: dial,
		},
	}
}

func (d *dockerClient) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
}

type dockerContainerListEntry struct {
	ID    string   `json:"Id"`
	Names []string `json:"Names"`
}

type dockerContainerInspect struct {
	State struct {
		Pid     int  `json:"Pid"`
		Running bool `json:"Running"`
	} `json:"State"`
}

// containerByPID returns a pid -> container map by listing containers and
// inspecting each one (GET /containers/json + GET /containers/{id}/json).
// Any failure (daemon absent, permission denied) yields an empty map.
//
// The docker list API only returns running containers, and only running
// containers own a pid (inspect.State.Pid) with visible sockets: a
// stopped/exited container has no /proc entry and cannot own a listener,
// so no container-state health handling is needed here (spec §5.4;
// extending the Docker API is out of scope for this batch).
func (d *dockerClient) containerByPID(ctx context.Context) (map[int32]dockerContainerInfo, error) {
	out := map[int32]dockerContainerInfo{}
	raw, err := d.get(ctx, "/containers/json")
	if err != nil || raw == nil {
		return out, err
	}
	var list []dockerContainerListEntry
	if err := json.Unmarshal(raw, &list); err != nil {
		return out, err
	}
	for _, c := range list {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		infoRaw, err := d.get(ctx, "/containers/"+c.ID+"/json")
		if err != nil || infoRaw == nil {
			continue
		}
		var inspect dockerContainerInspect
		if err := json.Unmarshal(infoRaw, &inspect); err != nil {
			continue
		}
		if !inspect.State.Running || inspect.State.Pid <= 0 {
			continue
		}
		name := ""
		if len(c.Names) > 0 {
			name = strings.TrimPrefix(c.Names[0], "/")
		}
		out[int32(inspect.State.Pid)] = dockerContainerInfo{ID: c.ID, Name: name}
	}
	return out, nil
}

package portinspector

// ServiceEmbeddedFRPS marks observations whose socket is owned by the
// fatedier frps server EMBEDDED in the Server Agent process (spec §8.1
// step 9: after an apply, the public port must be served by OUR frps, not
// by an external process). The Server Agent sets it only for observations
// whose PID is the agent's own process AND whose state is
// LISTENING/ACTIVE; the Master's apply verifier requires the marker before
// accepting a verification, so a foreign listener can never pass.
const ServiceEmbeddedFRPS = "frp-panel-frps"

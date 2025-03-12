#!/bin/bash
# auto_eploy.sh
# Usage: ./auto_deploy.sh <number_of_instances>
# This script generates configuration files for Sparrow schedulers and node monitors as well backends,
# launches them as separate processes, and then cleans up the launched processes and generated
# configuration files upon termination
# It also tests it with a single preconfigured frontend conf file

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <number_of_instances>"
    exit 1
fi

# Remove previous log files
echo "Removing old audit logs..."
rm -f ./sparrow_audit.*.log
echo "Old audit logs removed."

# Compile and package
echo "Compiling and packaging the application..."
mvn compile || { echo "Compilation failed"; exit 1; }
mvn package -Dmaven.test.skip=true || { echo "Packaging failed"; exit 1; }

NUM_INSTANCES=$1
BASE_BACKEND_PORT=20101

# For daemons, we derive ports using the following formulas:
# schedulerPort = (i+1)*10000 + 503
# getTaskPort     = schedulerPort - 4
# agentPort       = schedulerPort - 2
# internalAgentPort = schedulerPort - 1

# Create temporary directories for generated configs
DAEMON_CONF_DIR=$(mktemp -d -t sparrow-daemon-conf-XXXX)
BACKEND_CONF_DIR=$(mktemp -d -t sparrow-backend-conf-XXXX)

# File to store PIDs of launched processes
pid_file=$(mktemp)

# Cleanup function to kill processes and remove generated configs
cleanup() {
    echo "Cleaning up processes..."
    if [ -f "$pid_file" ]; then
        while read -r pid; do
            if ps -p "$pid" > /dev/null 2>&1; then
                echo "Terminating process $pid..."
                kill "$pid" 2>/dev/null || true
            fi
        done < "$pid_file"
        rm -f "$pid_file"
    fi
    echo "Removing generated configuration files..."
    rm -rf "$DAEMON_CONF_DIR"
    rm -rf "$BACKEND_CONF_DIR"
    echo "Cleanup complete."
}
trap cleanup SIGINT SIGTERM EXIT

echo "Generating $NUM_INSTANCES daemon and backend configurations..."

# Build a comma-separated list of node monitor addresses using the internal_agent.thrift.ports for all daemons
node_monitors_list=""

# Generate daemon configuration files
for (( i=1; i<=NUM_INSTANCES; i++ )); do
    # Calculate ports for this daemon.
    schedulerPort=$(( (i+1)*10000 + 503 ))        # e.g., instance 1: 20503, instance 2: 30503, etc.
    getTaskPort=$(( schedulerPort - 4 ))          # e.g., 20499, 30499, etc.
    agentPort=$(( schedulerPort - 2 ))            # e.g., 20501, 30501, etc.
    internalAgentPort=$(( schedulerPort - 1 ))    # e.g., 20502, 30502, etc.
    
    # Append this daemon's internal agent port to the node monitors list
    if [ -z "$node_monitors_list" ]; then
      node_monitors_list="localhost:$internalAgentPort"
    else
      node_monitors_list="$node_monitors_list,localhost:$internalAgentPort"
    fi
    
    # Create daemon conf file for instance i
    daemonConf="$DAEMON_CONF_DIR/daemon_$i.conf"
    cat > "$daemonConf" <<EOF
deployment.mode = configbased
static.node_monitors = localhost:$internalAgentPort
get_task.port = $getTaskPort
scheduler.thrift.port = $schedulerPort
agent.thrift.ports = $agentPort
internal_agent.thrift.ports = $internalAgentPort
static.app.name = sleepApp
sample.ratio = 1.0
EOF
    echo "Created $daemonConf"
done

# Update each daemon conf with the complete node_monitors_list
for conf in "$DAEMON_CONF_DIR"/*.conf; do
    sed -i "s/^static.node_monitors = .*/static.node_monitors = $node_monitors_list/" "$conf"
done

# Generate backend configuration files
for (( i=1; i<=NUM_INSTANCES; i++ )); do
    backendListenPort=$(( BASE_BACKEND_PORT + i - 1 ))
    
    # The backend should connect to the node monitor using the daemon's agent.thrift.ports.
    schedulerPort=$(( (i+1)*10000 + 503 ))
    agentPort=$(( schedulerPort - 2 )) 
    
    backendConf="$BACKEND_CONF_DIR/backend_$i.conf"
    cat > "$backendConf" <<EOF
listen_port=$backendListenPort
node_monitor_host=localhost
node_monitor_port=$agentPort
EOF
    echo "Created $backendConf"
done

echo "Launching processes..."

CLASS_PATH="target/sparrow-1.0-SNAPSHOT.jar"

# Launch each daemon
for conf in "$DAEMON_CONF_DIR"/*.conf; do
    start_cmd="java -cp $CLASS_PATH edu.berkeley.sparrow.daemon.SparrowDaemon -c $conf"
    echo "Starting daemon with: $start_cmd"
    eval "$start_cmd > logs/$(basename $conf .conf)_daemon.log 2>&1 &"
    pid=$!
    echo $pid >> "$pid_file"
done

# Give daemons some time to start
sleep 5

# Launch each backend
for conf in "$BACKEND_CONF_DIR"/*.conf; do
    start_cmd="java -cp $CLASS_PATH edu.berkeley.sparrow.examples.SimpleBackend -c $conf"
    echo "Starting backend with: $start_cmd"
    eval "$start_cmd > logs/$(basename $conf .conf)_backend.log 2>&1 &"
    pid=$!
    echo $pid >> "$pid_file"
done

# Given backend time to start
sleep 5

# Launch a frontend with a fixed configuration
echo "Launching frontend..."
java -cp $CLASS_PATH edu.berkeley.sparrow.examples.SimpleFrontend -c configs/frontend.conf &
frontend_pid=$!
echo "$frontend_pid" >> "$pid_file"

echo "All processes launched. Press Ctrl+C to terminate or wait for the frontend to exit..."

# Wait for the frontend process to finish
wait $frontend_pid

echo "Frontend process exited. Cleaning up..."
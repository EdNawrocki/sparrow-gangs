#!/bin/bash

# Set up error handling
set -e  # Exit immediately if a command fails

# Function to check if a process is running and capture its output
start_and_verify_process() {
  local description=$1
  local command=$2
  local log_file="./logs/$(echo $description | tr ' ' '_').log"
  
  # Create logs directory if it doesn't exist
  mkdir -p ./logs
  
  echo "Starting $description..."
  # Start the process and redirect output to log file
  eval "$command > $log_file 2>&1 &"
  local pid=$!
  
  # Wait a moment and check if process is still running
  sleep 3
  if ps -p $pid > /dev/null; then
    echo "$description started successfully (PID: $pid)"
    echo $pid >> $pid_file
    return 0
  else
    echo "ERROR: $description failed to start or crashed immediately."
    echo "Last few lines of log:"
    tail -n 10 $log_file
    return 1
  fi
}

# Cleanup function to terminate processes on interrupts or script completion
cleanup() {
  echo "Cleaning up processes..."
  if [ -f "$pid_file" ]; then
    while read -r pid; do
      if ps -p "$pid" > /dev/null; then
        echo "Terminating process $pid..."
        kill "$pid" 2>/dev/null || true
      fi
    done < "$pid_file"
    rm -f "$pid_file"
  fi
  echo "Cleanup complete."
}

trap cleanup SIGINT SIGTERM EXIT

# Remove previous log files
echo "Removing old audit logs..."
rm -f ./sparrow_audit.*.log
echo "Old audit logs removed."

# Clean up existing processes
echo "Cleaning up any existing processes..."
pkill -f "SparrowDaemon" 2>/dev/null || true
pkill -f "SimpleBackend" 2>/dev/null || true
pkill -f "SimpleFrontend" 2>/dev/null || true
sleep 2

# Compile and package
echo "Compiling and packaging the application..."
mvn compile || { echo "Compilation failed"; exit 1; }
mvn package -Dmaven.test.skip=true || { echo "Packaging failed"; exit 1; }

# Store PIDs for later cleanup
pid_file=$(mktemp)
echo "PIDs will be stored in $pid_file"

# Define the class path
CLASS_PATH="target/sparrow-1.0-SNAPSHOT.jar"

# Start the main Sparrow daemon (scheduler + node monitor)
start_and_verify_process "Main Sparrow daemon" "java -cp $CLASS_PATH edu.berkeley.sparrow.daemon.SparrowDaemon -c configs/sparrow_sleep_app.conf" || exit 1
sleep 5  # Give some additional time to fully initialize

# Start the second Sparrow daemon (node monitor only)
start_and_verify_process "Secondary Sparrow daemon" "java -cp $CLASS_PATH edu.berkeley.sparrow.daemon.SparrowDaemon -c configs/nm-1.conf" || exit 1
sleep 5  # Give some additional time to fully initialize

# Start the first backend
start_and_verify_process "First backend" "java -cp $CLASS_PATH edu.berkeley.sparrow.examples.SimpleBackend -c configs/backend.conf" || exit 1
sleep 3

# Start the second backend
start_and_verify_process "Second backend" "java -cp $CLASS_PATH edu.berkeley.sparrow.examples.SimpleBackend -c configs/backend-2.conf" || exit 1
sleep 3

# Start the frontend (in foreground)
echo "Starting frontend..."
java -cp $CLASS_PATH edu.berkeley.sparrow.examples.SimpleFrontend -c configs/frontend.conf &
frontend_pid=$!
echo "$frontend_pid" >> "$pid_file" 

# Opens up a background terminal to monitor logs live
# Too fast to keep up with anyway, can just use logs after completion but left here as an option
# echo "Opening a new terminal to monitor logs..."
# if [[ "$OSTYPE" == "linux-gnu"* ]]; then
#   gnome-terminal -- bash -c "cd $(pwd); tail -f ./logs/Main_Sparrow_daemon.log; exec bash" &
# elif [[ "$OSTYPE" == "darwin"* ]]; then
#   osascript -e "tell application \"Terminal\" to do script \"cd $(pwd); tail -f ./logs/Main_Sparrow_daemon.log\"" &
# elif [[ "$OSTYPE" == "msys"* || "$OSTYPE" == "cygwin"* ]]; then
#   mintty -e bash -c "cd $(pwd); tail -f ./logs/Main_Sparrow_daemon.log" &
# else
#   echo "Manual log monitoring required."
# fi

wait $frontend_pid

echo "Frontend process exited. Cleaning up..."

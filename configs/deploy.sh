
# start the scheduler and node monitor
java -cp ../target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.daemon.SparrowDaemon -c ./sparrow_sleep_app.conf

# start the backend
java -cp ../target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.examples.SimpleBackend -c backend.conf

# start the frontend
java -cp ../target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.examples.SimpleFrontend -c frontend.conf

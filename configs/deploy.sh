# each instance of sparrow is both the node monitor and the scheduler, they are inseparable
# When you register a backend to an instance of sparrow, it is acting as the node monitor
# when you register a frontend to an instance of sparrow, it is acting as the scheduler
# the same instance of sparrow can be acting as both a scheduler and a node monitor, but these two
# components just *happen* to be on the same machine, they don't directly communicate

# With this in mind, we need to start two instances of sparrow on our local machine, one will act as both
# a scheduler and a node monitor, the other will just serve as a node monitor

# this starts the instance of sparrow that we will register the frontend and a backend to
java -cp target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.daemon.SparrowDaemon -c configs/sparrow_sleep_app.conf

# this starts the instance of sparrow that will only be acting as a node monitor
java -cp target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.daemon.SparrowDaemon -c configs/nm-1.conf

# this will register a backend to nm-1
java -cp target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.examples.SimpleBackend -c configs/backend.conf

# this will register a backend to sparrow_sleep_app
java -cp target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.examples.SimpleBackend -c configs/backend-2.conf

# this will register the frontend to sparrow_sleep_app and job scheduling will begin
java -cp target/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.examples.SimpleFrontend -c configs/frontend.conf
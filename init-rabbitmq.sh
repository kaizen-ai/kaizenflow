#!/bin/bash
# Wait until RabbitMQ service is fully operational
# sleep 10
# rabbitmqctl wait /var/lib/rabbitmq/mnesia/rabbit@$HOSTNAME.pid

# ( rabbitmqctl wait --timeout 10 $RABBITMQ_PID_FILE ; \
# rabbitmqctl add_user $RABBITMQ_DEFAULT_PASS $RABBITMQ_DEFAULT_PASS 2>/dev/null ; \
# rabbitmqctl set_user_tags $RABBITMQ_DEFAULT_USER administrator ; \
# rabbitmqctl set_permissions -p / $RABBITMQ_DEFAULT_USER  ".*" ".*" ".*" ; \
# echo "*** User '$RABBITMQ_DEFAULT_USER' with password '$RABBITMQ_DEFAULT_PASS' completed. ***" ; \
# echo "*** Log in the WebUI at port 15672 (example: http:/localhost:15672) ***" ; \
# echo "Hello: $RABBITMQ_DEFAULT_USER and pwd: $RABBITMQ_DEFAULT_PASS")

# # Create the RabbitMQ user
# # rabbitmqctl add_user user user  # Both username and password are 'user'
# # rabbitmqctl set_user_tags user administrator
# # rabbitmqctl set_permissions -p / user ".*" ".*" ".*"
# # echo "*** User 'user' with password 'user' completed. ***"
# # echo "*** Log in the WebUI at port 15672 (example: http://localhost:15672) ***"

# echo "HELLO HI"
# echo rabbitmqctl status

# # Run RabbitMQ server with any additional arguments
# exec rabbitmq-server $@

# ----------------------------------------------------------------------------------------

#!/bin/bash

# Wait until RabbitMQ is fully operational
echo "Waiting for RabbitMQ to start..."
rabbitmqctl wait --timeout 20 $RABBITMQ_PID_FILE

# Add a new user and set permissions
echo "Configuring RabbitMQ user..."
rabbitmqctl add_user $RABBITMQ_DEFAULT_USER $RABBITMQ_DEFAULT_PASS
rabbitmqctl set_user_tags $RABBITMQ_DEFAULT_USER administrator
rabbitmqctl set_permissions -p / $RABBITMQ_DEFAULT_USER ".*" ".*" ".*"

echo "*** User '$RABBITMQ_DEFAULT_USER' with password '$RABBITMQ_DEFAULT_PASS' completed. ***"
echo "*** Log in the WebUI at port 15672 (example: http://localhost:15672) ***"

exec rabbitmq-server $@

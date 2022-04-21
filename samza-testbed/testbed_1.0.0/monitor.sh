top -p $(jps | grep LocalContainer | awk '{print $1}')

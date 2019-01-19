for pid in $(ps -ef | grep "airflow scheduler" | awk '{print $2}'); do kill -9 $pid; done

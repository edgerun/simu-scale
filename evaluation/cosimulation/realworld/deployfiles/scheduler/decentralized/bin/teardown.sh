#!/usr/bin/env bash
storage_local_schedulers='{"zone-a": "scheduler-zone-a", "zone-b": "scheduler-zone-b"}'

export STORAGE_LOCAL_SCHEDULERS="$storage_local_schedulers"

for VAR in zone-a zone-b zone-c
do
	export ZONE=$VAR
	export SCHEDULER_NAME="scheduler-$VAR"
    export GLOBAL_SCHEDULER_NAME="global-scheduler"
	cat local-scheduler-component.yaml | envsubst > /tmp/local-scheduler-component.yaml
	kubectl delete -f /tmp/local-scheduler-component.yaml --grace-period=0 --force

done

export ZONE=zone-c
export SCHEDULER_NAME="scheduler-zone-c"
cat global-scheduler-component.yaml | envsubst > /tmp/global-scheduler-component.yaml
kubectl delete -f /tmp/global-scheduler-component.yaml --grace-period=0 --force

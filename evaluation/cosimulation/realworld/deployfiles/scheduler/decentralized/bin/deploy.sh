#!/usr/bin/env bash
. $1
storage_local_schedulers='{"zone-a": "scheduler-zone-a", "zone-b": "scheduler-zone-b", "zone-c": "scheduler-zone-c"}'
export FUNCTION_MAX_SCALE=$(expr "$FUNCTION_MAX_SCALE" / 3)
export STORAGE_LOCAL_SCHEDULERS="$storage_local_schedulers"

kubectl create configmap global-scheduler-json-cm --from-file=$GLOBAL_SCHEDULER_JSON_PATH

for VAR in zone-a zone-b zone-c
do
	export ZONE=$VAR
	export SCHEDULER_NAME="scheduler-$VAR"
	export APP_NAME="local-scheduler-$VAR"
	export GLOBAL_SCHEDULER_NAME="global-scheduler"
	cat local-scheduler-component.yaml | envsubst > /tmp/local-scheduler-component.yaml
	kubectl apply -f /tmp/local-scheduler-component.yaml
    kubectl wait --for=condition=ready pod -l app="$APP_NAME" --timeout=5m

done

export ZONE=zone-c
export SCHEDULER_NAME="global-scheduler"
cat $GLOBAL_SCHEDULER_YAML_FILE | envsubst > /tmp/global-scheduler-component.yaml
kubectl apply -f /tmp/global-scheduler-component.yaml
kubectl wait --for=condition=ready pod -l app=global-scheduler --timeout=5m

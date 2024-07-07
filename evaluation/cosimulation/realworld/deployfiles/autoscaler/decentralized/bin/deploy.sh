#!/usr/bin/env bash
. $1
export FUNCTION_MAX_SCALE=$(expr "$FUNCTION_MAX_SCALE" / 3)

kubectl create configmap autoscaler-json-cm --from-file=$AUTOSCALER_JSON_PATH

for VAR in zone-a zone-b zone-c
do
	export ZONE=$VAR
	export SCHEDULER_NAME="global-scheduler"
	export APP_NAME="autoscaler-$VAR"
	cat $AUTOSCALER_YAML_FILE | envsubst > /tmp/autoscaler-component.yaml
	kubectl apply -f /tmp/autoscaler-component.yaml
	echo "Waiting for $APP_NAME pod to spawn"
    kubectl wait --for=condition=ready pod -l app="$APP_NAME" --timeout=5m

done



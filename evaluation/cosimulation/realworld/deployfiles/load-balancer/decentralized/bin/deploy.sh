#!/usr/bin/env bash
. $1
for VAR in zone-a zone-b zone-c
do
	export ZONE=$VAR
	export SCHEDULER_NAME="scheduler-$VAR"
	export APP_NAME="loadbalancer-$VAR"
	cat lb-component.yaml | envsubst > /tmp/lb-component.yaml
	kubectl apply -f /tmp/lb-component.yaml
    kubectl wait --for=condition=ready pod -l app="$APP_NAME" --timeout=5m
done



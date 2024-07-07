#!/usr/bin/env bash
for VAR in zone-a zone-b zone-c
do
	export CLUSTER=$VAR
	cat $AUTOSCALER_YAML_FILE | envsubst > /tmp/autoscaler-component.yaml
	kubectl delete -f /tmp/autoscaler-component.yaml --grace-period=0 --force
done

kubectl delete configmap autoscaler-json-cm



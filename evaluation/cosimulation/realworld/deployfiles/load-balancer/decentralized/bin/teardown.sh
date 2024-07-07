#!/usr/bin/env bash
for VAR in zone-a zone-b zone-c
do
	export ZONE=$VAR
	cat lb-component.yaml | envsubst > /tmp/lb-component.yaml
	kubectl delete -f /tmp/lb-component.yaml --grace-period=0 --force
done



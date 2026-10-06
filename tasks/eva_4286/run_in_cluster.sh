#!/usr/bin/env bash
# Run check_endpoints.py from a temporary pod inside the Kubernetes cluster and copy the responses back locally.
#
# Each URL path prefix is mapped to a namespace in namespaces.tsv. The service of each namespace is discovered with
# kubectl and reached from the test pod through its cluster DNS name (<service>.<namespace>.svc.cluster.local).
#
# Usage:
#   ./run_in_cluster.sh [-n <pod_namespace>] [-c <kube_context>] [-i <image>] [-o <output.json>] [-- <extra args>]
#
#   -n  namespace where the temporary test pod is created (default: eva-server)
#   Everything after "--" is passed to "check_endpoints.py query". A "--service PREFIX=URL" given there takes
#   precedence over the discovered service for that prefix, e.g.
#   ./run_in_cluster.sh -o k8s.json -- --service /eva/webservices/rest=http://my-svc.eva-server:8080
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POD_NAMESPACE="eva-server"
CONTEXT=""
IMAGE="python:3.12-slim"
OUTPUT="k8s_responses.json"
POD_NAME="eva-endpoint-check-$(date +%s)"

while getopts "n:c:i:o:" opt; do
  case $opt in
    n) POD_NAMESPACE=$OPTARG ;;
    c) CONTEXT=$OPTARG ;;
    i) IMAGE=$OPTARG ;;
    o) OUTPUT=$OPTARG ;;
    *) sed -n '2,14p' "$0"; exit 1 ;;
  esac
done
shift $((OPTIND - 1))
[[ "${1:-}" == "--" ]] && shift

KUBECTL=(kubectl)
[[ -n "$CONTEXT" ]] && KUBECTL+=(--context "$CONTEXT")

# Prefixes the user already mapped explicitly with --service
OVERRIDDEN_PREFIXES=" "
previous_arg=""
for arg in "$@"; do
  [[ "$previous_arg" == "--service" ]] && OVERRIDDEN_PREFIXES+="${arg%%=*} "
  previous_arg=$arg
done

# Discover the service of each namespace whose prefix is used in urls.txt
SERVICE_ARGS=()
while IFS=$'\t' read -r prefix namespace; do
  [[ -z "$prefix" || "$prefix" == \#* ]] && continue
  grep -Eq "^[A-Za-z]+[[:space:]]+${prefix}[/?]" "$SCRIPT_DIR/urls.txt" || continue
  [[ "$OVERRIDDEN_PREFIXES" == *" $prefix "* ]] && continue

  services=$("${KUBECTL[@]}" -n "$namespace" get svc \
    -o jsonpath='{range .items[*]}{.metadata.name}:{.spec.ports[0].port}{"\n"}{end}')
  if [[ $(echo "$services" | grep -c .) -ne 1 ]]; then
    echo "Expected exactly one service in namespace $namespace (for $prefix) but found:" >&2
    echo "${services:-  none}" | sed 's/^/  /' >&2
    echo "Choose one with: -- --service $prefix=http://<service>.$namespace.svc.cluster.local:<port>" >&2
    exit 1
  fi
  service_url="http://${services%%:*}.$namespace.svc.cluster.local:${services##*:}"
  echo "$prefix -> $service_url"
  SERVICE_ARGS+=(--service "$prefix=$service_url")
done < "$SCRIPT_DIR/namespaces.tsv"

cleanup() {
  "${KUBECTL[@]}" -n "$POD_NAMESPACE" delete pod "$POD_NAME" --ignore-not-found --wait=false > /dev/null
}
trap cleanup EXIT

echo "Starting pod $POD_NAME ($IMAGE) in namespace $POD_NAMESPACE"
"${KUBECTL[@]}" -n "$POD_NAMESPACE" run "$POD_NAME" --image="$IMAGE" --restart=Never --command -- sleep 3600 > /dev/null
"${KUBECTL[@]}" -n "$POD_NAMESPACE" wait --for=condition=Ready "pod/$POD_NAME" --timeout=180s > /dev/null

"${KUBECTL[@]}" -n "$POD_NAMESPACE" cp "$SCRIPT_DIR/check_endpoints.py" "$POD_NAME:/tmp/check_endpoints.py"
"${KUBECTL[@]}" -n "$POD_NAMESPACE" cp "$SCRIPT_DIR/urls.txt" "$POD_NAME:/tmp/urls.txt"
# Credentials referenced in the AUTH column of urls.txt (git-ignored file, optional)
if [[ -f "$SCRIPT_DIR/credentials.txt" ]]; then
  "${KUBECTL[@]}" -n "$POD_NAMESPACE" cp "$SCRIPT_DIR/credentials.txt" "$POD_NAME:/tmp/credentials.txt"
fi

# User supplied --service arguments come first so that they win over the discovered ones.
# Keep going on failure so that the responses are still copied back.
EXIT_CODE=0
"${KUBECTL[@]}" -n "$POD_NAMESPACE" exec "$POD_NAME" -- python /tmp/check_endpoints.py query \
  --urls /tmp/urls.txt --output /tmp/responses.json --credentials /tmp/credentials.txt \
  "$@" ${SERVICE_ARGS[@]+"${SERVICE_ARGS[@]}"} || EXIT_CODE=$?

"${KUBECTL[@]}" -n "$POD_NAMESPACE" cp "$POD_NAME:/tmp/responses.json" "$OUTPUT" > /dev/null \
  && echo "Responses copied to $OUTPUT"
exit $EXIT_CODE

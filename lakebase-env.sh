# Source this file to point charglt at a Databricks Lakebase Autoscaling project:
#
#   . ./lakebase-env.sh [databricks.properties]
#
# Reads databricks.host and databricks.token from the properties file
# (default ../databricks-faadata-demo/databricks.properties; DATABRICKS_HOST and
# DATABRICKS_TOKEN override), then exports PG_HOST, PG_USER, PG_PASSWORD,
# PG_SSLMODE and PGPASSWORD etc. for both charglt and psql.
#
# PG_PASSWORD is a short-lived OAuth token (1 hour max). Connections opened
# with it stay up after it expires, but new logins need a fresh one: source
# this file again.
#
# LAKEBASE_PROJECT (default charglt), LAKEBASE_BRANCH (default production) and
# LAKEBASE_ENDPOINT (default primary) select the endpoint.

_lb_props=${1:-../databricks-faadata-demo/databricks.properties}
_lb_prop() { grep "^$1=" "$_lb_props" 2>/dev/null | cut -d= -f2-; }

_lb_host=${DATABRICKS_HOST:-$(_lb_prop databricks.host)}
_lb_token=${DATABRICKS_TOKEN:-$(_lb_prop databricks.token)}
_lb_endpoint="projects/${LAKEBASE_PROJECT:-charglt}/branches/${LAKEBASE_BRANCH:-production}/endpoints/${LAKEBASE_ENDPOINT:-primary}"

if [ -z "$_lb_host" ] || [ -z "$_lb_token" ]; then
    echo "lakebase-env.sh: no databricks.host/databricks.token in $_lb_props" >&2
    return 1 2>/dev/null || exit 1
fi

_lb_api() {
    curl -sf -H "Authorization: Bearer $_lb_token" -H 'Content-Type: application/json' "https://$_lb_host/api/2.0$@"
}

export PG_HOST=$(_lb_api "/postgres/$_lb_endpoint" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"]["hosts"]["host"])')
export PG_USER=$(_lb_api /preview/scim/v2/Me | python3 -c 'import json,sys; print(json.load(sys.stdin)["userName"])')
export PG_PASSWORD=$(_lb_api /postgres/credentials -X POST -d "{\"endpoint\":\"$_lb_endpoint\"}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')
export PG_SSLMODE=require

# For psql.
export PGHOST=$PG_HOST PGUSER=$PG_USER PGPASSWORD=$PG_PASSWORD PGSSLMODE=require

if [ -n "$PG_HOST" ] && [ -n "$PG_USER" ] && [ -n "$PG_PASSWORD" ]; then
    echo "Lakebase: $PG_USER@$PG_HOST ($_lb_endpoint)"
else
    echo "lakebase-env.sh: failed to look up $_lb_endpoint" >&2
fi

unset -f _lb_prop _lb_api
unset _lb_props _lb_host _lb_token _lb_endpoint

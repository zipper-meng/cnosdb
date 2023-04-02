#!/bin/bash

set -o errexit

# This script is at $PROJ_DIR/meta/cluster.sh
PROJ_DIR=$(
  cd $(dirname $0)
  cd ..
  pwd
)
ARG_RUN_RELEASE=0
ARG_SKIP_BUILD=0
ARG_SKIP_CLEAN=0

function usage() {
  echo "Start and initialize three CnosDB Metadata Servers as a cluster."
  echo
  echo "USAGE:"
  echo "    ${0} [OPTIONS]"
  echo
  echo "OPTIONS:"
  echo "    --release            Run release version of CnosDB Metadata Server"
  echo "    -sb, --skip-build    Skip building before running CnosdDB Metadata Server"
  echo "    -sc, --skip-clean    Clean data directory before running CnosdDB Metadata Server"
  echo
}

while [[ $# -gt 0 ]]; do
  key=${1}
  case ${key} in
  --release)
    ARG_RUN_RELEASE=1
    shift 1
    ;;
  -sb | --skip-build)
    ARG_SKIP_BUILD=1
    shift 1
    ;;
  -sc | --skip-clean)
    ARG_SKIP_CLEAN=1
    shift 1
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    usage
    exit 1
    ;;
  esac
done

echo "=== CnosDB Metadata Server (cluster) ==="
echo "ARG_RUN_RELEASE = ${ARG_RUN_RELEASE}"
echo "ARG_SKIP_BUILD  = ${ARG_SKIP_BUILD}"
echo "ARG_SKIP_CLEAN  = ${ARG_SKIP_CLEAN}"
echo "----------------------------------------"

EXE_PATH="cnosdb-meta"

if [ ${ARG_SKIP_BUILD} -eq 0 ]; then
  if [ ${ARG_RUN_RELEASE} -eq 0 ]; then
    cargo build --package meta --bin cnosdb-meta
    EXE_PATH=${PROJ_DIR}"/target/debug/cnosdb-meta"
  else
    cargo build --package meta --bin cnosdb-meta --release
    EXE_PATH=${PROJ_DIR}"/target/release/cnosdb-meta"
  fi
else
  if [ ${ARG_RUN_RELEASE} -eq 0 ]; then
    EXE_PATH=${PROJ_DIR}"/target/debug/cnosdb-meta"
  else
    EXE_PATH=${PROJ_DIR}"/target/release/cnosdb-meta"
  fi
fi
if [ ${ARG_SKIP_CLEAN} -eq 0 ]; then
  rm -rf /tmp/cnosdb/meta
fi

kill() {
  if [ "$(uname)" = "Darwin" ]; then
    SERVICE='cnosdb-meta'
    if pgrep -xq -- "${SERVICE}"; then
      pkill -f "${SERVICE}"
    fi
  else
    set +e # killall will error if finds no process to kill
    killall cnosdb-meta
    set -e
  fi
}

rpc() {
  local uri=$1
  local body="$2"

  echo '---'" rpc(:$uri, $body)"
  {
    if [ ".$body" = "." ]; then
      curl --silent "127.0.0.1:$uri"
    else
      curl --silent "127.0.0.1:$uri" -H "Content-Type: application/json" -d "$body"
    fi
  } | {
    echo -n '--- '
    if type jq >/dev/null 2>&1; then
      jq
    else
      cat
    fi
  }
  echo
  echo
}

#export RUST_LOG=debug
echo "Killing all running cnosdb-meta"

kill
sleep 1

echo "Start 3 uninitialized cnosdb-meta servers..."

mkdir -p /tmp/cnosdb/logs

nohup ${EXE_PATH} --config ${PROJ_DIR}/meta/config/config_21001.toml &>/tmp/cnosdb/logs/meta_node.1.log &
echo "Server 1 started"
sleep 1

nohup ${EXE_PATH} --config ${PROJ_DIR}/meta/config/config_21002.toml &>/tmp/cnosdb/logs/meta_node.2.log &
echo "Server 2 started"
sleep 1

nohup ${EXE_PATH} --config ${PROJ_DIR}/meta/config/config_21003.toml &>/tmp/cnosdb/logs/meta_node.3.log &
echo "Server 3 started"
sleep 1

echo "Initialize server 1 as a single-node cluster"
rpc 21001/init '{}'

echo "Server 1 is a leader now"
sleep 1

echo "Adding node 2 and node 3 as learners, to receive log from leader node 1"

rpc 21001/add-learner '[2, "127.0.0.1:21002"]'
echo "Node 2 added as leaner"
sleep 1

rpc 21001/add-learner '[3, "127.0.0.1:21003"]'
echo "Node 3 added as leaner"
sleep 1

echo "Changing membership from [1] to 3 nodes cluster: [1, 2, 3]"
echo
rpc 21001/change-membership '[1, 2, 3]'
sleep 1
echo "Membership changed"
sleep 1

echo "Get metrics from the leader"
rpc 21001/metrics
sleep 1

#####################################################################

# echo "Get metrics from the leader again"
# sleep 1
# echo
# rpc 21001/metrics
# sleep 1

# echo "Write data on leader 1"
# sleep 1
# echo
# rpc 21001/write '{"Set":{"key":"foo3","value":"bar3"}}'
# sleep 1
# echo "Data written"
# sleep 1

# echo "Read on every node, including the leader"
# sleep 1
# echo "Read from node 1"
# echo
# rpc 21001/debug
# echo "Read from node 2"
# echo
# rpc 21002/debug
# echo "Read from node 3"
# echo
# rpc 21003/debug

# kill

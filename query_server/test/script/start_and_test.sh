#!/usr/bin/env bash
set -e

# define environment
export HTTP_HOST=${HTTP_HOST:-"127.0.0.1:31001"}
export URL="http://${HTTP_HOST}/api/v1/ping"
source "$HOME/.cargo/env"

# This script is at $PROJ_DIR/query_server/test/script/start_and_test.sh
PROJ_DIR=$(
  cd $(dirname $0)
  cd ../../..
  pwd
)
ARG_SKIP_BUILD=0

function usage() {
  echo "Start a new cnosdb instance and run integration test."
  echo
  echo "USAGE:"
  echo "    ${0} [OPTIONS]"
  echo
  echo "OPTIONS:"
  echo "    -sb, --skip-build    Skip building before test"
  echo
}

while [[ $# -gt 0 ]]; do
  key=${1}
  case ${key} in
  -sb | --skip-build)
    ARG_SKIP_BUILD=1
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

function start_cnosdb() {
  DATA_DIR="/tmp/cnosdb/1001"
  LOG_PATH="/tmp/cnosdb/logs/start_and_test.data_node.31001.log"
  EXE_PATH="${PROJ_DIR}/target/test-ci/cnosdb"
  CONFIG_PATH="${PROJ_DIR}/config/config_31001.toml"

  if [ -e ${DATA_DIR} ]; then
    rm -rf ${DATA_DIR}
  fi
  if [ ${ARG_SKIP_BUILD} -eq 1 ]; then
    echo "Running CnosDB executable at \'${EXE_PATH}\'."
    if [ -e ${EXE_PATH} ]; then
      nohup ${EXE_PATH} run --config ${CONFIG_PATH} >${LOG_PATH} 2>&1 &
    else
      echo "CnosDB executable not found: \'${EXE_PATH}\'."
      exit -1
    fi
  else
    echo "Running CnosDB using cargo."
    nohup cargo run --profile test-ci -- run --config ${CONFIG_PATH} >${LOG_PATH} 2>&1 &
  fi
  return $!
}

function wait_start() {
  while [ "$(curl -s ${URL})" == "" ] && kill -0 ${PID}; do
    sleep 2
  done
}

function test() {
  echo "Testing query/test" &&
    cargo run --package test &&
    echo "Testing e2e_test" &&
    cargo test --package e2e_test
}

echo "Starting cnosdb"
start_cnosdb
PID=$?

echo "Wait for pid=${PID} startup to complete"

(wait_start && test) || EXIT_CODE=$?

echo "Test complete, killing ${PID}"

kill ${PID}

exit ${EXIT_CODE:-0}

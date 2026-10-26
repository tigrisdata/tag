#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
runner="${repo_root}/deploy/native/run.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
mock_dir="${tmp_dir}/mock-bin"
mkdir -p "${mock_dir}"
config_file="${tmp_dir}/config.yaml"
cat >"${config_file}" <<'CONFIG'
upstream:
  max_idle_conns_per_host: 256
CONFIG

cat >"${mock_dir}/lsof" <<'MOCK'
#!/bin/sh
exit 1
MOCK
cat >"${mock_dir}/sleep" <<'MOCK'
#!/bin/sh
exit 0
MOCK
cat >"${mock_dir}/curl" <<'MOCK'
#!/bin/sh
output=""
while [ "$#" -gt 0 ]; do
    if [ "$1" = "-o" ]; then
        output="$2"
        shift 2
    else
        shift
    fi
done
if [ -n "${output}" ]; then
    cat >"${output}" <<'BINARY'
#!/bin/sh
capture_tmp="${TAG_RUN_CAPTURE}.tmp"
printf '%s\n%s\n' "${TAG_MAX_IDLE_CONNS_PER_HOST-<unset>}" "$*" >"${capture_tmp}"
mv "${capture_tmp}" "${TAG_RUN_CAPTURE}"
BINARY
    chmod +x "${output}"
else
    capture="${TAG_RUN_CAPTURE:?TAG_RUN_CAPTURE is required for the mock health check}"
    attempts=0
    while [ ! -f "${capture}" ]; do
        if [ "${attempts}" -ge 100 ]; then
            echo "mock health check timed out waiting for launch capture" >&2
            exit 28
        fi
        attempts=$((attempts + 1))
        command -p sleep 0.01
    done
fi
exit 0
MOCK
chmod +x "${mock_dir}/lsof" "${mock_dir}/sleep" "${mock_dir}/curl"

run_case() {
    local name="$1"
    local expected_pool="$2"
    local bin_dir="${tmp_dir}/bin-${name}"
    local data_dir="${tmp_dir}/data-${name}"
    local capture="${tmp_dir}/capture-${name}"
    if [ "${name}" = "unset" ]; then
        env -u TAG_MAX_IDLE_CONNS_PER_HOST \
            PATH="${mock_dir}:${PATH}" \
            AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
            TAG_RUN_CAPTURE="${capture}" TAG_VERSION=probe \
            BIN_DIR="${bin_dir}" DATA_DIR="${data_dir}" TAG_PORT=18081 \
            TAG_START_TIMEOUT=1 TAG_CONFIG_FILE="${config_file}" \
            bash "${runner}" start >/dev/null
    else
        PATH="${mock_dir}:${PATH}" \
            AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
            TAG_MAX_IDLE_CONNS_PER_HOST="${expected_pool}" TAG_RUN_CAPTURE="${capture}" \
            TAG_VERSION=probe BIN_DIR="${bin_dir}" DATA_DIR="${data_dir}" \
            TAG_PORT=18081 TAG_START_TIMEOUT=1 TAG_CONFIG_FILE="${config_file}" \
            bash "${runner}" start >/dev/null
    fi
    local actual_pool actual_args
    actual_pool="$(sed -n '1p' "${capture}")"
    actual_args="$(sed -n '2p' "${capture}")"
    if [ "${actual_pool}" != "${expected_pool}" ]; then
        echo "launcher pool override for ${name}: got '${actual_pool}', want '${expected_pool}'" >&2
        return 1
    fi
    if [[ "${actual_args}" != *"--config ${config_file}"* ]]; then
        echo "launcher did not pass its configured YAML to TAG" >&2
        return 1
    fi
}

run_case unset ""
run_case explicit 128
help="$(env -u TAG_MAX_IDLE_CONNS_PER_HOST bash "${runner}" help)"
if [[ "${help}" != *"unset: YAML value or 1024"* ]]; then
    echo "launcher help does not describe the config/default idle cap" >&2
    exit 1
fi

bash -n "${runner}"

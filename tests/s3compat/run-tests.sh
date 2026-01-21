#!/bin/bash
# S3 Compatibility Tests Runner for TAG
# Modeled after tigris-os gateway/tests/tests.sh

set -ex

# Handle Ctrl+C to exit the entire script
trap 'echo -e "\nInterrupted. Exiting..."; exit 130' INT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check for required environment variables
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "Error: AWS credentials not set."
    echo "  export AWS_ACCESS_KEY_ID=<your-key>"
    echo "  export AWS_SECRET_ACCESS_KEY=<your-secret>"
    exit 1
fi

# Clone s3-tests repo if not present
[ -d s3-tests ] || git clone https://github.com/ceph/s3-tests.git

# Remove problematic git-lfs repository that causes 403 errors (Linux CI only)
if [ -f /etc/apt/sources.list.d/github_git-lfs.list ]; then
    sudo rm -f /etc/apt/sources.list.d/github_git-lfs.list
fi

# Set up virtual environment for Python dependencies
VENV_DIR="$SCRIPT_DIR/.venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Install tox in the virtual environment if not available
if ! command -v tox >/dev/null 2>&1; then
    echo "Installing tox in virtual environment..."
    pip install tox
fi

# Generate s3tests.conf with actual credentials from environment variables
# The template uses __AWS_ACCESS_KEY_ID__ and __AWS_SECRET_ACCESS_KEY__ as placeholders
S3TEST_CONF="$SCRIPT_DIR/s3tests.conf.generated"
sed -e "s|__AWS_ACCESS_KEY_ID__|${AWS_ACCESS_KEY_ID}|g" \
    -e "s|__AWS_SECRET_ACCESS_KEY__|${AWS_SECRET_ACCESS_KEY}|g" \
    "$SCRIPT_DIR/s3tests.conf" > "$S3TEST_CONF"
export S3TEST_CONF

cd s3-tests

# Create tox.ini for running tests
cat <<EOF >tox.ini
[tox]
envlist = py

[testenv]
deps = -rrequirements.txt
passenv =
    S3TEST_CONF
    S3_USE_SIGV4
commands = pytest {posargs}

[pytest]
addopts = -W ignore::DeprecationWarning
EOF

# If specific test path is provided as argument, run that
if [ $# -ge 1 ]; then
    tox -- "s3tests/functional/$1"
    exit
fi

# Test arrays - curated list of tests relevant for TAG
# Based on tigris-os gateway/tests/tests.sh

# Header validation tests
test_headers=(
    "test_object_create_bad_md5_invalid_short"
    "test_object_create_bad_md5_bad"
    "test_object_create_bad_md5_empty"
    "test_object_create_bad_md5_none"
    "test_object_create_bad_expect_empty"
    "test_object_create_bad_expect_none"
    "test_object_create_bad_contentlength_empty"
    "test_object_create_bad_contentlength_negative"
    "test_object_create_bad_contenttype_invalid"
    "test_object_create_bad_contenttype_empty"
    "test_object_create_bad_contenttype_none"
    "test_object_create_date_and_amz_date"
    "test_object_create_amz_date_and_no_date"
    "test_bucket_create_contentlength_none"
    "test_object_acl_create_contentlength_none"
    "test_bucket_create_bad_expect_empty"
    "test_bucket_create_bad_contentlength_negative"
    "test_bucket_create_bad_contentlength_none"
)

# Core S3 operations tests
test_s3=(
    "test_bucket_list_empty"
    "test_bucket_list_distinct"
    "test_bucket_list_many"
    "test_bucket_listv2_many"
    "test_basic_key_count"
    "test_bucket_list_delimiter_basic"
    "test_bucket_listv2_delimiter_basic"
    "test_bucket_listv2_encoding_basic"
    "test_bucket_list_encoding_basic"
    "test_bucket_list_delimiter_prefix"
    "test_bucket_listv2_delimiter_prefix"
    "test_bucket_listv2_delimiter_prefix_ends_with_delimiter"
    "test_bucket_list_delimiter_prefix_ends_with_delimiter"
    "test_bucket_list_delimiter_alt"
    "test_bucket_listv2_delimiter_alt"
    "test_bucket_list_delimiter_prefix_underscore"
    "test_bucket_listv2_delimiter_prefix_underscore"
    "test_bucket_list_delimiter_percentage"
    "test_bucket_listv2_delimiter_percentage"
    "test_bucket_list_delimiter_whitespace"
    "test_bucket_listv2_delimiter_whitespace"
    "test_bucket_list_delimiter_dot"
    "test_bucket_listv2_delimiter_dot"
    "test_bucket_list_delimiter_unreadable"
    "test_bucket_listv2_delimiter_unreadable"
    "test_bucket_list_delimiter_empty"
    "test_bucket_listv2_delimiter_empty"
    "test_bucket_list_delimiter_none"
    "test_bucket_listv2_delimiter_none"
    "test_bucket_list_delimiter_not_exist"
    "test_bucket_listv2_delimiter_not_exist"
    "test_bucket_list_prefix_basic"
    "test_bucket_listv2_prefix_basic"
    "test_bucket_list_prefix_alt"
    "test_bucket_listv2_prefix_alt"
    "test_bucket_list_prefix_empty"
    "test_bucket_listv2_prefix_empty"
    "test_bucket_list_prefix_none"
    "test_bucket_listv2_prefix_none"
    "test_bucket_list_prefix_not_exist"
    "test_bucket_listv2_prefix_not_exist"
    "test_bucket_list_prefix_unreadable"
    "test_bucket_listv2_prefix_unreadable"
    "test_bucket_list_prefix_delimiter_basic"
    "test_bucket_listv2_prefix_delimiter_basic"
    "test_bucket_list_prefix_delimiter_alt"
    "test_bucket_listv2_prefix_delimiter_alt"
    "test_bucket_list_maxkeys_one"
    "test_bucket_listv2_maxkeys_one"
    "test_bucket_list_maxkeys_zero"
    "test_bucket_listv2_maxkeys_zero"
    "test_bucket_list_maxkeys_none"
    "test_bucket_listv2_maxkeys_none"
    "test_bucket_list_marker_none"
    "test_bucket_list_marker_empty"
)

# Object operations tests
test_objects=(
    "test_object_write_to_nonexist_bucket"
    "test_object_head_zero_bytes"
    "test_object_write_check_etag"
    "test_object_write_cache_control"
    "test_object_write_expires"
    "test_object_write_read_update_read_delete"
    "test_object_metadata_replaced_on_put"
    "test_object_set_get_metadata_none_to_good"
    "test_object_set_get_metadata_none_to_empty"
    "test_object_set_get_metadata_overwrite_to_empty"
)

# Bucket operations tests
test_buckets=(
    "test_bucket_create_naming_bad_starts_nonalpha"
    "test_bucket_create_naming_bad_short_one"
    "test_bucket_create_naming_bad_short_two"
    "test_bucket_create_naming_good_long_60"
    "test_bucket_create_naming_good_long_61"
    "test_bucket_create_naming_good_long_62"
    "test_bucket_create_naming_good_long_63"
    "test_bucket_create_naming_bad_ip"
    "test_bucket_create_naming_dns_underscore"
    "test_bucket_create_naming_dns_long"
    "test_bucket_create_naming_dns_dash_at_end"
    "test_bucket_create_naming_dns_dot_dot"
    "test_bucket_create_naming_dns_dot_dash"
    "test_bucket_create_naming_dns_dash_dot"
    "test_bucket_get_location"
    "test_bucket_delete_nonempty"
    "test_bucket_create_delete"
)

# Multipart upload tests
test_multipart=(
    "test_multipart_upload_empty"
    "test_multipart_upload_small"
    "test_multipart_upload"
    "test_multipart_upload_contents"
    "test_multipart_upload_overwrite_existing_object"
    "test_abort_multipart_upload"
    "test_abort_multipart_upload_not_found"
    "test_list_multipart_upload"
)

# Copy object tests
test_copy=(
    "test_object_copy_zero_size"
    "test_object_copy_same_bucket"
    "test_object_copy_verify_contenttype"
    "test_object_copy_to_itself"
    "test_object_copy_to_itself_with_metadata"
    "test_object_copy_diff_bucket"
    "test_object_copy_canned_acl"
    "test_object_copy_retaining_metadata"
    "test_object_copy_replacing_metadata"
)

# Run header validation tests
echo "Running header validation tests..."
for test in "${test_headers[@]}"; do
    echo "  Running: $test"
    tox -- "s3tests/functional/test_headers.py::$test" || true
done

# Run core S3 operations tests
echo "Running core S3 operations tests..."
for test in "${test_s3[@]}"; do
    echo "  Running: $test"
    tox -- "s3tests/functional/test_s3.py::$test" || true
done

# Run object operations tests
echo "Running object operations tests..."
for test in "${test_objects[@]}"; do
    echo "  Running: $test"
    tox -- "s3tests/functional/test_s3.py::$test" || true
done

# Run bucket operations tests
echo "Running bucket operations tests..."
for test in "${test_buckets[@]}"; do
    echo "  Running: $test"
    tox -- "s3tests/functional/test_s3.py::$test" || true
done

# Run multipart upload tests
echo "Running multipart upload tests..."
for test in "${test_multipart[@]}"; do
    echo "  Running: $test"
    tox -- "s3tests/functional/test_s3.py::$test" || true
done

# Run copy object tests
echo "Running copy object tests..."
for test in "${test_copy[@]}"; do
    echo "  Running: $test"
    tox -- "s3tests/functional/test_s3.py::$test" || true
done

echo "S3 compatibility tests completed!"

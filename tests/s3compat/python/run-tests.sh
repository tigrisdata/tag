#!/bin/bash
# S3 Compatibility Tests Runner for TAG
# Modeled after tigris-os gateway/tests/tests.sh

# Track test failures
FAILED_TESTS=()
PASSED_COUNT=0

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
if [ ! -d s3-tests ]; then
    echo "Cloning s3-tests repository..."
    if ! git clone https://github.com/ceph/s3-tests.git; then
        echo "Error: Failed to clone s3-tests repository"
        exit 1
    fi
fi

# Remove problematic git-lfs repository that causes 403 errors (Linux CI only)
if [ -f /etc/apt/sources.list.d/github_git-lfs.list ]; then
    sudo rm -f /etc/apt/sources.list.d/github_git-lfs.list
fi

# Set up virtual environment for Python dependencies
VENV_DIR="$SCRIPT_DIR/.venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    if ! python3 -m venv "$VENV_DIR"; then
        echo "Error: Failed to create virtual environment"
        exit 1
    fi
fi

# Activate virtual environment
# shellcheck disable=SC1091
if ! source "$VENV_DIR/bin/activate"; then
    echo "Error: Failed to activate virtual environment"
    exit 1
fi

# Install tox in the virtual environment if not available
if ! command -v tox >/dev/null 2>&1; then
    echo "Installing tox in virtual environment..."
    if ! pip install tox; then
        echo "Error: Failed to install tox"
        exit 1
    fi
fi

# Generate s3tests.conf with actual credentials from environment variables
# The template uses __AWS_ACCESS_KEY_ID__ and __AWS_SECRET_ACCESS_KEY__ as placeholders
# Use S3TEST_CONF_TEMPLATE env var to override the template file (e.g., s3tests-tls.conf)
S3TEST_CONF_TEMPLATE="${S3TEST_CONF_TEMPLATE:-s3tests.conf}"
S3TEST_CONF="$SCRIPT_DIR/s3tests.conf.generated"
if ! sed -e "s|__AWS_ACCESS_KEY_ID__|${AWS_ACCESS_KEY_ID}|g" \
    -e "s|__AWS_SECRET_ACCESS_KEY__|${AWS_SECRET_ACCESS_KEY}|g" \
    "$SCRIPT_DIR/$S3TEST_CONF_TEMPLATE" > "$S3TEST_CONF"; then
    echo "Error: Failed to generate s3tests.conf"
    exit 1
fi
export S3TEST_CONF

cd s3-tests || exit

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

# Helper function to run a test and track failures
run_test() {
    local test_file="$1"
    local test_name="$2"
    echo "  Running: $test_name"
    if tox -- "s3tests/functional/${test_file}::${test_name}"; then
        ((PASSED_COUNT++))
    else
        FAILED_TESTS+=("${test_file}::${test_name}")
    fi
}

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
    # Additional header validation tests
    # "test_object_create_bad_expect_mismatch"  # TAG returns 417, S3 may handle differently
    #"test_object_create_bad_contentlength_none"
    # "test_object_create_bad_authorization_empty"
    # "test_object_create_bad_authorization_none"
    "test_bucket_put_bad_canned_acl"
    # "test_bucket_create_bad_expect_mismatch"
    "test_bucket_create_bad_contentlength_empty"
    # "test_bucket_create_bad_authorization_empty"
    # "test_bucket_create_bad_authorization_none"
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
    # "test_object_set_get_metadata_overwrite_to_empty"
    # Additional object operations tests
    "test_object_read_not_exist"
    # "test_object_read_unreadable"
    "test_object_requestid_matches_header_on_error"
    "test_multi_object_delete"
    "test_multi_objectv2_delete"
    # "test_multi_object_delete_key_limit"  # Tigris doesn't support object versioning yet
    # "test_multi_objectv2_delete_key_limit"  # Tigris doesn't support object versioning yet
    # Range requests
    "test_ranged_request_response_code"
    "test_ranged_big_request_response_code"
    "test_ranged_request_skip_leading_bytes_response_code"
    "test_ranged_request_return_trailing_bytes_response_code"
    "test_ranged_request_invalid_range"
    "test_ranged_request_empty_object"
    # Conditional GET operations
    "test_get_object_ifmatch_good"
    "test_get_object_ifmatch_failed"
    # "test_get_object_ifnonematch_good"  # Blocked: Tigris doesn't return ETag in 304 (RFC 7232 violation)
    "test_get_object_ifnonematch_failed"
    "test_get_object_ifmodifiedsince_good"
    # "test_get_object_ifmodifiedsince_failed"  # Blocked: Tigris doesn't return ETag in 304 (RFC 7232 violation)
    "test_get_object_ifunmodifiedsince_good"
    "test_get_object_ifunmodifiedsince_failed"
    # Conditional PUT operations
    "test_put_object_ifmatch_failed"
    # Large object copy
    "test_object_copy_16m"
    # Special prefix handling
    "test_bucket_list_special_prefix"
    # Chunked encoding tests
    # "test_object_write_with_chunked_transfer_encoding"  # Requires HTTP Transfer-Encoding: chunked (Ceph RGW-specific, not supported by S3/Tigris)
    # "test_object_content_encoding_aws_chunked"  # Tigris stores aws-chunked in Content-Encoding as-is; stripping it in TAG breaks signatures in transparent mode
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
    # Additional bucket operations tests
    "test_bucket_notexist"
    "test_bucket_delete_notexist"
    # "test_bucket_create_exists"
    "test_bucket_create_exists_nonowner"
    "test_buckets_create_then_list"
    "test_buckets_list_ctime"
    # "test_bucket_recreate_not_overriding"
    "test_bucket_recreate_new_acl"
    # "test_bucket_list_return_data"
    "test_bucket_head"
    "test_bucket_head_notexist"
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
    # Additional multipart tests
    "test_multipart_copy_small"
    "test_multipart_copy_invalid_range"
    # "test_multipart_copy_improper_range"
    "test_multipart_copy_without_range"
    # "test_multipart_copy_special_names"
    "test_multipart_copy_multiple_sizes"
    "test_multipart_upload_multiple_sizes"
    # "test_multipart_upload_size_too_small"
    "test_multipart_upload_missing_part"
    "test_multipart_upload_incorrect_etag"
    # "test_multipart_resend_first_finishes_last"
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

# Tagging tests
test_tagging=(
    "test_set_bucket_tagging"
    "test_get_obj_tagging"
    "test_get_obj_head_tagging"
    "test_put_max_tags"
    "test_put_excess_tags"
    "test_put_max_kvsize_tags"
    "test_put_excess_key_tags"
    "test_put_excess_val_tags"
    "test_put_modify_tags"
    "test_put_delete_tags"
    "test_put_obj_with_tags"
    "test_set_multipart_tagging"
    # "test_get_tags_acl_public"
    # "test_put_tags_acl_public"
    # "test_delete_tags_obj_public"
)

# Run header validation tests
echo "Running header validation tests..."
for test in "${test_headers[@]}"; do
    run_test "test_headers.py" "$test"
done

# Run core S3 operations tests
echo "Running core S3 operations tests..."
for test in "${test_s3[@]}"; do
    run_test "test_s3.py" "$test"
done

# Run object operations tests
echo "Running object operations tests..."
for test in "${test_objects[@]}"; do
    run_test "test_s3.py" "$test"
done

# Run bucket operations tests
echo "Running bucket operations tests..."
for test in "${test_buckets[@]}"; do
    run_test "test_s3.py" "$test"
done

# Run multipart upload tests
echo "Running multipart upload tests..."
for test in "${test_multipart[@]}"; do
    run_test "test_s3.py" "$test"
done

# Run copy object tests
echo "Running copy object tests..."
for test in "${test_copy[@]}"; do
    run_test "test_s3.py" "$test"
done

# Run tagging tests
echo "Running tagging tests..."
for test in "${test_tagging[@]}"; do
    run_test "test_s3.py" "$test"
done

# Report results
echo ""
echo "========================================="
echo "S3 Compatibility Tests Summary"
echo "========================================="
TOTAL_TESTS=$((PASSED_COUNT + ${#FAILED_TESTS[@]}))
echo "Total: $TOTAL_TESTS | Passed: $PASSED_COUNT | Failed: ${#FAILED_TESTS[@]}"
echo ""

if [ ${#FAILED_TESTS[@]} -eq 0 ]; then
    echo "All tests passed!"
    exit 0
else
    echo "FAILED TESTS:"
    for failed in "${FAILED_TESTS[@]}"; do
        echo "  - $failed"
    done
    exit 1
fi

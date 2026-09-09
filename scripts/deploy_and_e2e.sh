#!/usr/bin/env bash
# Deploy the PoC stack, run the four end-to-end scenarios, and tear it down.
#
#   ./scripts/deploy_and_e2e.sh --profile AdministratorAccess-123456789012 --destroy
#
# Everything is parameterised. The stack is tagged project=agent-wait and every resource
# has RemovalPolicy.DESTROY, so --destroy leaves nothing behind and nothing billable.
#
# Prerequisites: an active AWS SSO login, Python 3.12, uv, Node (for the CDK CLI).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REGION="ap-south-1"
STACK_NAME="agent-wait-poc"
WAIT_TIMEOUT="PT2M"
TIMEOUT_SECONDS=120
SKIP_DEPLOY=0
SKIP_TESTS=0
DESTROY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)         export AWS_PROFILE="$2"; shift 2 ;;
    --region)          REGION="$2"; shift 2 ;;
    --stack)           STACK_NAME="$2"; shift 2 ;;
    --wait-timeout)    WAIT_TIMEOUT="$2"; shift 2 ;;
    --timeout-seconds) TIMEOUT_SECONDS="$2"; shift 2 ;;
    --skip-deploy)     SKIP_DEPLOY=1; shift ;;
    --skip-tests)      SKIP_TESTS=1; shift ;;
    --destroy)         DESTROY=1; shift ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

export AWS_DEFAULT_REGION="$REGION"
export CDK_DEFAULT_REGION="$REGION"

echo "== identity =="
aws sts get-caller-identity --query Arn --output text
CDK_DEFAULT_ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
export CDK_DEFAULT_ACCOUNT

# The CDK app is `python app.py`, so the workspace venv has to come first on PATH.
if [[ -d "$ROOT/.venv/bin" ]]; then export PATH="$ROOT/.venv/bin:$PATH"
else export PATH="$ROOT/.venv/Scripts:$PATH"; fi

# Under Git Bash the CDK CLI is a *Windows* process, so a POSIX path like /c/Users/...
# reaches it as C:\c\Users\... and the Lambda asset is reported "not found". cygpath does
# the conversion; on Linux and macOS it does not exist and the path is already correct.
BUNDLE="$ROOT/build/lambda"
if command -v cygpath >/dev/null 2>&1; then BUNDLE="$(cygpath -w "$BUNDLE")"; fi

e2e_status=0

if [[ "$SKIP_DEPLOY" -eq 0 ]]; then
  echo -e "\n== local test suite =="
  uv run pytest -q

  echo -e "\n== building the lambda bundle =="
  uv run python scripts/build_lambda_bundle.py

  echo -e "\n== cdk deploy $STACK_NAME =="
  ( cd "$ROOT/packages/agent-wait-aws/cdk" && \
    npx --yes aws-cdk@2 deploy --require-approval never \
      -c "stackName=$STACK_NAME" -c "region=$REGION" -c "waitTimeout=$WAIT_TIMEOUT" \
      -c "bundlePath=$BUNDLE" )
fi

if [[ "$SKIP_TESTS" -eq 0 ]]; then
  echo -e "\n== end-to-end scenarios =="
  uv run python examples/refund_agent/demo_scenarios.py \
    --stack "$STACK_NAME" --region "$REGION" --timeout-seconds "$TIMEOUT_SECONDS" || e2e_status=$?
fi

if [[ "$DESTROY" -eq 1 ]]; then
  echo -e "\n== tearing down =="
  ( cd "$ROOT/packages/agent-wait-aws/cdk" && \
    npx --yes aws-cdk@2 destroy --force \
      -c "stackName=$STACK_NAME" -c "region=$REGION" -c "bundlePath=$BUNDLE" )
fi

if [[ "$e2e_status" -ne 0 ]]; then
  echo "End-to-end scenarios reported failures; see reports/." >&2
  exit "$e2e_status"
fi
echo -e "\nDone."

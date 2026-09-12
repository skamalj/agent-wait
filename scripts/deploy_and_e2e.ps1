# Deploy the PoC stack, run the four end-to-end scenarios, and tear it down.
#
#   pwsh scripts/deploy_and_e2e.ps1 -Profile AdministratorAccess-123456789012
#
# Everything is parameterised. The stack is tagged project=agent-wait and every resource
# has RemovalPolicy.DESTROY, so -Destroy leaves nothing behind and nothing billable.
#
# Prerequisites: an active AWS SSO login, Python 3.12, uv, Node (for the CDK CLI).

[CmdletBinding()]
param(
    [string]$Profile = $env:AWS_PROFILE,
    [string]$Region = "ap-south-1",
    [string]$StackName = "agent-wait-poc",
    [string]$WaitTimeout = "PT2M",
    [switch]$SkipDeploy,
    [switch]$SkipTests,
    [switch]$Destroy
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if ($Profile) { $env:AWS_PROFILE = $Profile }
$env:AWS_DEFAULT_REGION = $Region
$env:CDK_DEFAULT_REGION = $Region

Write-Host "== identity ==" -ForegroundColor Cyan
aws sts get-caller-identity --query Arn --output text
if ($LASTEXITCODE -ne 0) { throw "No AWS credentials. Run 'aws sso login' first." }

# The CDK app is `python app.py`, so the workspace venv has to come first on PATH.
$env:PATH = "$Root\.venv\Scripts;$env:PATH"
$env:CDK_DEFAULT_ACCOUNT = (aws sts get-caller-identity --query Account --output text)

if (-not $SkipDeploy) {
    Write-Host "`n== local test suite ==" -ForegroundColor Cyan
    uv run pytest -q
    if ($LASTEXITCODE -ne 0) { throw "Local tests failed; not deploying." }

    Write-Host "`n== building the lambda bundle ==" -ForegroundColor Cyan
    uv run python scripts/build_lambda_bundle.py
    if ($LASTEXITCODE -ne 0) { throw "Bundle build failed." }

    Write-Host "`n== cdk deploy $StackName ==" -ForegroundColor Cyan
    Push-Location "$Root\examples\refund_agent\cdk"
    try {
        npx --yes aws-cdk@2 deploy --require-approval never `
            -c stackName=$StackName -c region=$Region -c waitTimeout=$WaitTimeout `
            -c bundlePath="$Root\build\lambda"
        if ($LASTEXITCODE -ne 0) { throw "cdk deploy failed." }
    } finally { Pop-Location }
}

if (-not $SkipTests) {
    Write-Host "`n== end-to-end scenarios ==" -ForegroundColor Cyan
    uv run python examples/refund_agent/demo_scenarios.py `
        --stack $StackName --region $Region
    $e2e = $LASTEXITCODE
} else { $e2e = 0 }

if ($Destroy) {
    Write-Host "`n== tearing down ==" -ForegroundColor Cyan
    Push-Location "$Root\examples\refund_agent\cdk"
    try {
        npx --yes aws-cdk@2 destroy --force `
            -c stackName=$StackName -c region=$Region -c bundlePath="$Root\build\lambda"
    } finally { Pop-Location }
}

if ($e2e -ne 0) { throw "End-to-end scenarios reported failures; see reports/." }
Write-Host "`nDone." -ForegroundColor Green

# Run pytest with the project interpreter without relying on `conda activate`.
# Examples:
#   pwsh -File scripts/pytest.ps1 tests/test_config.py -q
#   pwsh -File scripts/pytest.ps1 -m "not network"
#   pwsh -File scripts/pytest.ps1            # defaults to the offline suite
#
# Override the interpreter with `$env:DSA_PYTHON = '<path>'` when needed.

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'resolve_python.ps1')

$pythonPath = Initialize-DsaPython
$env:PYTHONIOENCODING = 'utf-8'

$pytestArgs = @($args)
if ($pytestArgs.Count -eq 0) {
    $pytestArgs = @('-m', 'not network')
}

& $pythonPath -c "import sys; print('pytest.ps1: python', sys.executable, sys.version.split()[0])"
if ($LASTEXITCODE -ne 0) {
    [Console]::Error.WriteLine("pytest.ps1: cannot run the selected interpreter: $pythonPath")
    exit 2
}

Push-Location $repoRoot
try {
    & $pythonPath -m pytest @pytestArgs
    $code = $LASTEXITCODE
}
finally {
    Pop-Location
}
exit $code

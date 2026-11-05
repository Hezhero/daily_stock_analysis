<#
.SYNOPSIS
Run the POSIX backend gate inside WSL when GitHub Actions is unavailable.

.DESCRIPTION
Creates (or reuses) a virtualenv in the WSL filesystem, installs the CI
toolchain when requirements.txt / .github/requirements-ci.txt change, then
runs scripts/ci_gate.sh against an ext4 mirror of the worktree. The mirror
matters: on /mnt/<drive> (DrvFs) a `multiprocessing` child re-importing pandas
takes longer than the 5s startup grace used by the scheduler/codex tests, so
those tests fail only because of the filesystem. Use -InPlace to run against
the mounted worktree for debugging. The optional fxmacrodata-public-client
wheel is skipped because its GitHub Release URL returns 403 from WSL
(ADR 0005).

.PARAMETER Phase
The ci_gate.sh phase: all, syntax, flake8, deterministic, offline-tests.

.PARAMETER Distro
WSL distribution name. Defaults to Ubuntu-24.04.

.PARAMETER Venv
Virtualenv path inside WSL. Defaults to /tmp/dsa-venv.

.PARAMETER SourceMirror
ext4 directory that receives a copy of the worktree. Defaults to /tmp/dsa-src.

.PARAMETER InPlace
Run against /mnt/<drive> instead of the ext4 mirror (slower, flakier).

.PARAMETER Reinstall
Force pip install even when requirements-ci.txt is unchanged.

.PARAMETER PipIndex
Optional pip index URL (for example a regional mirror) used for the WSL install.
#>
[CmdletBinding()]
param(
    [ValidateSet('all', 'syntax', 'flake8', 'deterministic', 'offline-tests')]
    [string]$Phase = 'all',
    [string]$Distro = 'Ubuntu-24.04',
    [string]$Venv = '/tmp/dsa-venv',
    [string]$SourceMirror = '/tmp/dsa-src',
    [string]$PipIndex = '',
    [switch]$Reinstall,
    [switch]$InPlace
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot

if (-not (Get-Command wsl.exe -ErrorAction SilentlyContinue)) {
    throw 'wsl.exe not found. Install WSL2, or run the POSIX gate in Docker/CI.'
}

& wsl.exe -d $Distro -- true 2>$null
if ($LASTEXITCODE -ne 0) {
    $available = (& wsl.exe -l -q) -join ', '
    throw "WSL distribution '$Distro' is not available. Available: $available"
}

$wslRepo = (& wsl.exe -d $Distro -- wslpath -a $repoRoot.Replace('\', '/') 2>$null).Trim()
if ($LASTEXITCODE -ne 0 -or -not $wslRepo) {
    throw "cannot translate '$repoRoot' to a WSL path"
}

$reinstallFlag = if ($Reinstall) { '1' } else { '0' }
$inPlaceFlag = if ($InPlace) { '1' } else { '0' }
$runRepo = if ($InPlace) { $wslRepo } else { $SourceMirror }
$bash = @"
set -euo pipefail
if [ '$inPlaceFlag' != '1' ]; then
  # ext4 mirror: DrvFs (/mnt/<drive>) makes multiprocessing spawn children take
  # longer than the tests' 5s startup grace, which is a filesystem artifact,
  # not a product failure.
  mkdir -p '$SourceMirror'
  if command -v rsync >/dev/null 2>&1; then
    rsync -a --delete \
      --exclude='.git/' --exclude='data/' --exclude='logs/' \
      --exclude='node_modules/' --exclude='__pycache__/' \
      --exclude='.pytest_cache/' \
      '$wslRepo'/ '$SourceMirror'/
  else
    rm -rf '$SourceMirror'
    mkdir -p '$SourceMirror'
    tar -C '$wslRepo' \
      --exclude=./.git --exclude=./data --exclude=./logs --exclude=node_modules \
      --exclude=__pycache__ --exclude=.pytest_cache \
      -cf - . | tar -C '$SourceMirror' -xf -
  fi
fi
cd '$runRepo'
if [ ! -x '$Venv/bin/python' ]; then
  python3 -m venv '$Venv'
fi
. '$Venv/bin/activate'
python -V
req_hash=`$(sha256sum requirements.txt .github/requirements-ci.txt | sha256sum | cut -d' ' -f1)
if [ '$reinstallFlag' = '1' ] || [ ! -f '$Venv/.dsa-requirements.sha' ] || [ "`$(cat '$Venv/.dsa-requirements.sha' 2>/dev/null)" != "`$req_hash" ]; then
  # fxmacrodata-public-client is a GitHub-Release-only optional wheel (ADR 0005).
  # WSL installs have returned 403 for it, so install the filtered dependency set.
  mkdir -p /tmp/dsa-req
  grep -v 'fxmacrodata-public-client' requirements.txt > /tmp/dsa-req/requirements.txt || true
  {
    cat /tmp/dsa-req/requirements.txt
    printf '\nflake8\npytest\npytest-timeout>=2.3.0\n'
  } > /tmp/dsa-req/requirements-ci.txt
  if [ -n '$PipIndex' ]; then
    export PIP_INDEX_URL='$PipIndex'
  fi
  export PIP_TIMEOUT=120
  export PIP_RETRIES=5
  pip install -q -r /tmp/dsa-req/requirements-ci.txt
  printf '%s' "`$req_hash" > '$Venv/.dsa-requirements.sha'
fi
./scripts/ci_gate.sh '$Phase'
"@

$tempScript = Join-Path ([System.IO.Path]::GetTempPath()) ("dsa-ci-gate-{0}.sh" -f [guid]::NewGuid())
try {
    [System.IO.File]::WriteAllText($tempScript, $bash, [System.Text.UTF8Encoding]::new($false))
    $wslScript = (& wsl.exe -d $Distro -- wslpath -a $tempScript.Replace('\', '/')).Trim()
    & wsl.exe -d $Distro -- bash $wslScript
    exit $LASTEXITCODE
}
finally {
    Remove-Item -LiteralPath $tempScript -Force -ErrorAction SilentlyContinue
}

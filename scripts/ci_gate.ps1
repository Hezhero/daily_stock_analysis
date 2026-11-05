[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('all', 'syntax', 'flake8', 'governance', 'deterministic', 'offline-tests')]
    [string]$Phase = 'all',
    [string]$Python = ''
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'resolve_python.ps1')

function Find-GitBash {
    $candidates = @()
    if ($env:DSA_GIT_BASH) {
        $candidates += $env:DSA_GIT_BASH
    }
    $git = Get-Command git -ErrorAction SilentlyContinue
    if ($git) {
        $gitSource = $git.Source
        # Under Git Bash, Get-Command git returns /usr/bin/git; ask Git for its
        # Windows install root instead of trying to Split-Path a POSIX path.
        if ($gitSource -notmatch '^[A-Za-z]:') {
            $execPath = (& git --exec-path 2>$null)
            if ($execPath) {
                $gitRoot = Split-Path (Split-Path (Split-Path $execPath -Parent) -Parent) -Parent
                $gitSource = Join-Path $gitRoot 'cmd\git.exe'
            }
        }
        if ($gitSource -match '^[A-Za-z]:') {
            $gitRoot = Split-Path (Split-Path $gitSource -Parent) -Parent
            $candidates += Join-Path $gitRoot 'bin\bash.exe'
        }
    }
    foreach ($base in @($env:ProgramFiles, ${env:ProgramFiles(x86)}, $env:ProgramW6432)) {
        if ($base) {
            $candidates += Join-Path $base 'Git\bin\bash.exe'
        }
    }
    $candidates += @(
        'C:\Program Files\Git\bin\bash.exe'
        'C:\Program Files (x86)\Git\bin\bash.exe'
    )
    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate)) {
            return $candidate
        }
    }
    return $null
}

$bash = Find-GitBash
if (-not $bash) {
    [Console]::Error.WriteLine(
        'backend-gate: Git Bash not found. Install Git for Windows, or run the commands in docs/agents/validation-matrix.md.'
    )
    exit 2
}

$pythonPath = Initialize-DsaPython -Explicit $Python

& $pythonPath -c "import sys; print('backend-gate: python', sys.executable, sys.version.split()[0])"
if ($LASTEXITCODE -ne 0) {
    [Console]::Error.WriteLine("backend-gate: cannot run the selected interpreter: $pythonPath")
    exit 2
}

Push-Location $repoRoot
try {
    & $bash 'scripts/ci_gate.sh' $Phase
    $code = $LASTEXITCODE
}
finally {
    Pop-Location
}
exit $code

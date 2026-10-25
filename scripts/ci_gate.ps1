[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('all', 'syntax', 'flake8', 'deterministic', 'offline-tests')]
    [string]$Phase = 'all'
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot

function Find-GitBash {
    $candidates = @()
    $git = Get-Command git -ErrorAction SilentlyContinue
    if ($git) {
        $gitRoot = Split-Path (Split-Path $git.Source -Parent) -Parent
        $candidates += Join-Path $gitRoot 'bin\bash.exe'
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

Push-Location $repoRoot
try {
    & $bash 'scripts/ci_gate.sh' $Phase
    $code = $LASTEXITCODE
}
finally {
    Pop-Location
}
exit $code

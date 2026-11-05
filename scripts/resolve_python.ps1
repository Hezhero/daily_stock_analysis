# Shared interpreter resolution for PowerShell entry points.
# Dot-source this file, then call `Resolve-DsaPython -Explicit $path`.

function Resolve-DsaPython {
    param([string]$Explicit = '')

    if ($Explicit) {
        if (-not (Test-Path -LiteralPath $Explicit)) {
            throw "Python not found: $Explicit"
        }
        return (Resolve-Path -LiteralPath $Explicit).Path
    }
    if ($env:DSA_PYTHON) {
        if (-not (Test-Path -LiteralPath $env:DSA_PYTHON)) {
            throw "DSA_PYTHON not found: $env:DSA_PYTHON"
        }
        return (Resolve-Path -LiteralPath $env:DSA_PYTHON).Path
    }

    $conda = Get-Command conda -ErrorAction SilentlyContinue
    if ($conda) {
        try {
            # conda.bat emits a trailing blank line, so the result is an array.
            $lines = @(& conda run -n daily_stock_analysis python -c "import sys; print(sys.executable)" 2>$null)
            $resolved = ($lines | Where-Object { $_ -and $_.Trim() } | Select-Object -First 1)
            if ($resolved) {
                $resolved = $resolved.Trim()
            }
            if ($LASTEXITCODE -eq 0 -and $resolved -and (Test-Path -LiteralPath $resolved)) {
                return (Resolve-Path -LiteralPath $resolved).Path
            }
        }
        catch {
            # Fall through to the interpreter on PATH.
        }
    }

    $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCommand) {
        Write-Warning "could not resolve the daily_stock_analysis environment; using $($pythonCommand.Source)"
        return $pythonCommand.Source
    }
    throw 'Python not found. Activate daily_stock_analysis or pass -Python <path>.'
}

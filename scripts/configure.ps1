<#
.SYNOPSIS
    Interactive configuration helper for HYDRA (Windows entry point).

.DESCRIPTION
    Dispatches to scripts/configure.py when a Python interpreter is
    available. If no Python is found, falls back to a minimal
    PowerShell-native prompt loop that still writes a valid .env.

    The Python version is the canonical implementation — it validates
    webhook URLs, substitutes placeholders into
    alertmanager/alertmanager.yml, and handles edge cases (existing
    .env, gitignore verification, non-interactive mode). Prefer it.

.PARAMETER Force
    Overwrite an existing .env without asking.

.PARAMETER NonInteractive
    Accept every default; fail if any required value lacks one.

.PARAMETER Localhost
    (With -NonInteractive) Compose DSNs against localhost rather than
    docker-compose service names.

.PARAMETER DryRun
    Show what would be written without touching any files.

.EXAMPLE
    .\scripts\configure.ps1

.EXAMPLE
    .\scripts\configure.ps1 -NonInteractive -Localhost

.NOTES
    Run from the repo root so relative paths resolve correctly.
#>

[CmdletBinding()]
param(
    [switch]$Force,
    [switch]$NonInteractive,
    [switch]$Localhost,
    [switch]$DryRun,
    [switch]$SkipAlertmanager
)

$ErrorActionPreference = 'Stop'

# -----------------------------------------------------------------------------
# Locate repo root and Python interpreter
# -----------------------------------------------------------------------------

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir '..')

Push-Location $repoRoot
try {
    # Prefer repo-local venv if present, then `py` launcher, then bare `python`.
    $pythonCandidates = @(
        (Join-Path $repoRoot '.venv\Scripts\python.exe'),
        (Join-Path $repoRoot '.venv/bin/python'),
        'py',
        'python',
        'python3'
    )

    $python = $null
    foreach ($candidate in $pythonCandidates) {
        if (Test-Path $candidate) {
            $python = $candidate
            break
        }
        # Fall back to command lookup for bare names
        $cmd = Get-Command $candidate -ErrorAction SilentlyContinue
        if ($cmd) {
            $python = $cmd.Source
            break
        }
    }

    if ($null -eq $python) {
        Write-Warning "No Python interpreter found."
        Write-Warning "The Python helper is the canonical implementation."
        Write-Warning "Install Python 3.12+ and rerun, or continue with a minimal native prompt."
        $continueNative = Read-Host "Continue with PowerShell-native fallback? [y/N]"
        if ($continueNative -notmatch '^[Yy]') {
            Write-Host "Aborted." -ForegroundColor Yellow
            exit 1
        }
        Invoke-NativeFallback
        exit 0
    }

    # -----------------------------------------------------------------------------
    # Build arg list for the Python script
    # -----------------------------------------------------------------------------

    $pyArgs = @((Join-Path $repoRoot 'scripts\configure.py'))
    if ($Force) { $pyArgs += '--force' }
    if ($NonInteractive) { $pyArgs += '--non-interactive' }
    if ($Localhost) { $pyArgs += '--localhost' }
    if ($DryRun) { $pyArgs += '--dry-run' }
    if ($SkipAlertmanager) { $pyArgs += '--skip-alertmanager' }

    Write-Verbose "Dispatching to Python: $python $($pyArgs -join ' ')"

    & $python @pyArgs
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}


# =============================================================================
# Native PowerShell fallback — used only when no Python is found.
# Minimal: prompts for the handful of variables that most deployments
# need; does NOT validate webhook URLs or edit alertmanager.yml. If you
# reach this path, please install Python afterwards.
# =============================================================================

function Invoke-NativeFallback {
    $envExample = Join-Path $repoRoot '.env.example'
    $envFile = Join-Path $repoRoot '.env'
    $gitignore = Join-Path $repoRoot '.gitignore'

    if (-not (Test-Path $envExample)) {
        Write-Error ".env.example not found at $envExample"
        exit 1
    }

    # Gitignore check
    $covered = $false
    if (Test-Path $gitignore) {
        $patterns = Get-Content $gitignore | ForEach-Object { $_.Trim() }
        if ($patterns -contains '.env') { $covered = $true }
    }
    if (-not $covered) {
        Write-Warning ".env is not listed in .gitignore — aborting to avoid accidental secret commit."
        exit 1
    }

    if ((Test-Path $envFile) -and (-not $Force)) {
        $overwrite = Read-Host ".env already exists. Overwrite? [y/N]"
        if ($overwrite -notmatch '^[Yy]') {
            Write-Host "Aborted." -ForegroundColor Yellow
            exit 0
        }
    }

    function Read-Value {
        param(
            [string]$Prompt,
            [string]$Default = '',
            [switch]$Secret
        )
        $label = $Prompt
        if ($Default) { $label = "$Prompt [$Default]" }
        if ($Secret) {
            $secure = Read-Host "$label" -AsSecureString
            $ptr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
            $value = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
            [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr) | Out-Null
        } else {
            $value = Read-Host $label
        }
        if ([string]::IsNullOrWhiteSpace($value)) { return $Default }
        return $value
    }

    $values = @{}
    Write-Host "`n── PostgreSQL ────────────────────────────────────────" -ForegroundColor Blue
    $values['POSTGRES_DB'] = Read-Value 'PostgreSQL database name' 'hydra'
    $values['POSTGRES_USER'] = Read-Value 'PostgreSQL username' 'hydra'
    $values['POSTGRES_PASSWORD'] = Read-Value 'PostgreSQL password' 'hydra' -Secret
    Write-Host "`n── Neo4j ─────────────────────────────────────────────" -ForegroundColor Blue
    $values['NEO4J_USER'] = Read-Value 'Neo4j username' 'neo4j'
    $values['NEO4J_PASSWORD'] = Read-Value 'Neo4j password' 'hydrapass' -Secret
    Write-Host "`n── MinIO ─────────────────────────────────────────────" -ForegroundColor Blue
    $values['MINIO_ROOT_USER'] = Read-Value 'MinIO root user' 'hydra'
    $values['MINIO_ROOT_PASSWORD'] = Read-Value 'MinIO root password' 'hydrapass' -Secret
    Write-Host "`n── Grafana ───────────────────────────────────────────" -ForegroundColor Blue
    $values['GRAFANA_ADMIN_PASSWORD'] = Read-Value 'Grafana admin password' 'admin' -Secret
    Write-Host "`n── Alerts (optional, blank to skip) ──────────────────" -ForegroundColor Blue
    $values['SLACK_WEBHOOK_URL'] = Read-Value 'Slack webhook URL' ''
    $values['PAGERDUTY_ROUTING_KEY'] = Read-Value 'PagerDuty routing key' '' -Secret

    # Compose DSNs using compose service names (the common path)
    $pgUser = $values['POSTGRES_USER']
    $pgPass = $values['POSTGRES_PASSWORD']
    $pgDb = $values['POSTGRES_DB']
    $values['HYDRA_DATABASE__POSTGRES_DSN'] = "postgresql+asyncpg://${pgUser}:${pgPass}@postgres:5432/${pgDb}"
    $values['HYDRA_DATABASE__INFLUXDB_URL'] = 'http://influxdb:8086'
    $values['HYDRA_DATABASE__ELASTICSEARCH_URL'] = 'http://elasticsearch:9200'
    $values['HYDRA_DATABASE__NEO4J_URI'] = 'bolt://neo4j:7687'
    $values['HYDRA_DATABASE__MINIO_URL'] = 'http://minio:9000'
    $values['HYDRA_DATABASE__REDIS_URL'] = 'redis://redis:6379/0'

    # Apply overrides line-by-line
    $templateLines = Get-Content $envExample
    $output = @()
    $seen = New-Object System.Collections.Generic.HashSet[string]
    foreach ($line in $templateLines) {
        if ($line -match '^([A-Z][A-Z0-9_]*)=') {
            $key = $Matches[1]
            if ($values.ContainsKey($key)) {
                $output += "$key=$($values[$key])"
                [void]$seen.Add($key)
                continue
            }
        }
        $output += $line
    }
    # Append any keys not seen in template
    foreach ($key in $values.Keys) {
        if (-not $seen.Contains($key)) {
            $output += "$key=$($values[$key])"
        }
    }

    if ($DryRun) {
        Write-Host "`n── dry-run — would write ─────────────────────────────" -ForegroundColor Blue
        $output | ForEach-Object { Write-Host $_ }
        return
    }

    $output | Set-Content -Path $envFile -Encoding UTF8
    Write-Host "✓ wrote .env" -ForegroundColor Green

    Write-Host "`n── next steps ────────────────────────────────────────" -ForegroundColor Blue
    Write-Host "  1. Review .env (secrets are plain text)"
    Write-Host "  2. docker compose up -d"
    Write-Host "  3. Visit:"
    Write-Host "       API        http://localhost:8000/api/v1/health"
    Write-Host "       Prometheus http://localhost:9090"
    Write-Host "       Grafana    http://localhost:3000  (user: admin)"
    Write-Warning "Native fallback does not edit alertmanager/alertmanager.yml."
    Write-Warning "Run the Python version later or edit the file manually to substitute webhook URLs."
}

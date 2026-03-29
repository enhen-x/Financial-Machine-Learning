param(
    [Parameter(Mandatory = $false)]
    [string]$SecretFile = "secrets\\tushare.env",

    [Parameter(Mandatory = $false)]
    [string]$Token,

    [Parameter(Mandatory = $false)]
    [string]$HttpUrl = "http://8.136.22.187:8010/"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Protect-SecretFile {
    param([string]$Path)

    $resolved = (Resolve-Path -LiteralPath $Path).Path
    $acl = Get-Acl -LiteralPath $resolved

    # Disable inheritance and remove inherited rules.
    $acl.SetAccessRuleProtection($true, $false)

    foreach ($rule in @($acl.Access)) {
        [void]$acl.RemoveAccessRule($rule)
    }

    $currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    $ruleUser = New-Object System.Security.AccessControl.FileSystemAccessRule($currentUser, "FullControl", "Allow")
    $ruleSystem = New-Object System.Security.AccessControl.FileSystemAccessRule("SYSTEM", "FullControl", "Allow")

    [void]$acl.AddAccessRule($ruleUser)
    [void]$acl.AddAccessRule($ruleSystem)

    try {
        Set-Acl -LiteralPath $resolved -AclObject $acl
    } catch {
        Write-Warning "Failed to set ACL with Set-Acl. Try running PowerShell as Administrator."
        Write-Warning $_
    }
}

$dir = Split-Path -Parent $SecretFile
if (-not (Test-Path -LiteralPath $dir)) {
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
}

if (-not $Token) {
    $secure = Read-Host "Input Tushare token" -AsSecureString
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    try {
        $Token = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

if (-not $Token) {
    throw "Token cannot be empty."
}

@(
    "TUSHARE_TOKEN=$Token"
    "TUSHARE_HTTP_URL=$HttpUrl"
) | Set-Content -LiteralPath $SecretFile -Encoding UTF8

Protect-SecretFile -Path $SecretFile
Write-Host "Secret saved:" $SecretFile
Write-Host "ACL protection attempted (check warnings above)."

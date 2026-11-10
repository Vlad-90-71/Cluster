[CmdletBinding()]
param(
    [string]$VmDbHost = "192.168.10.12",
    [string]$VmAppHost = "192.168.10.13",
    [string]$SshUser = "ubuntu",
    [string]$ManagerHost = "192.168.10.13",
    [string]$ServiceName = "mvc",
    [string]$ImageName = "cluster",
    [string]$Dockerfile = "Cluster/Dockerfile",
    [string]$BuildContext = ".",
    [string]$RemoteDir = "/home/ubuntu",
    [int]$UpdateMonitorSeconds = 30,
    [int]$UpdateDelaySeconds = 5
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Invoke-Native {
    param(
        [Parameter(Mandatory)][string]$File,
        [Parameter(Mandatory)][string[]]$Arguments,
        [Parameter(Mandatory)][string]$Step
    )

    Write-Host "`n>>> $Step" -ForegroundColor Cyan
    & $File @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Step failed. Exit code: $LASTEXITCODE"
    }
}

function Invoke-Ssh {
    param(
        [Parameter(Mandatory)][string]$HostName,
        [Parameter(Mandatory)][string]$RemoteCommand,
        [Parameter(Mandatory)][string]$Step
    )

    Invoke-Native -File "ssh" -Arguments @(
        "$SshUser@$HostName",
        $RemoteCommand
    ) -Step $Step
}

# Проверяем необходимые программы.
foreach ($command in @("docker", "ssh", "scp")) {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "Не найдена команда '$command'. Установи Docker Desktop и/или OpenSSH Client."
    }
}

if (-not (Test-Path -LiteralPath $Dockerfile -PathType Leaf)) {
    throw "Не найден Dockerfile: $Dockerfile"
}
if (-not (Test-Path -LiteralPath $BuildContext -PathType Container)) {
    throw "Не найден build context: $BuildContext"
}

# Уникальная версия образа. Можно передать -Tag вручную, если добавить параметр.
$tag = Get-Date -Format "yyyyMMdd-HHmmss"
$image = "${ImageName}:$tag"
$archive = "cluster-app-$tag.tar"
$localArchive = Join-Path (Get-Location) $archive
$remoteArchive = "$RemoteDir/$archive"

Write-Host "Image:   $image"
Write-Host "Archive: $localArchive"
Write-Host "Manager: $ManagerHost"
Write-Host "Service: $ServiceName"

try {
    # 1. Собираем образ с уникальным тегом.
    Invoke-Native -File "docker" -Arguments @(
        "build",
        "-t", $image,
        "-f", $Dockerfile,
        $BuildContext
    ) -Step "Build Docker image"

    # latest оставляем только как локальный удобный тег.
    Invoke-Native -File "docker" -Arguments @(
        "tag", $image, "${ImageName}:latest"
    ) -Step "Tag image as latest"

    # 2. Экспортируем только версионный тег.
    Invoke-Native -File "docker" -Arguments @(
        "save", "-o", $localArchive, $image
    ) -Step "Export image"

    # 3. Передаём архив на оба узла.
    foreach ($hostName in @($VmDbHost, $VmAppHost)) {
        Invoke-Native -File "scp" -Arguments @(
            $localArchive,
            "${SshUser}@${hostName}:$remoteArchive"
        ) -Step "Copy image to $hostName"
    }

    # 4. Загружаем образ на обоих узлах.
    foreach ($hostName in @($VmDbHost, $VmAppHost)) {
        Invoke-Ssh -HostName $hostName `
            -RemoteCommand "sudo docker load -i '$remoteArchive'" `
            -Step "Load image on $hostName"
    }

    # Удаляем удалённые TAR только после успешной загрузки на обоих узлах.
    foreach ($hostName in @($VmDbHost, $VmAppHost)) {
        Invoke-Ssh -HostName $hostName `
            -RemoteCommand "rm -f '$remoteArchive'" `
            -Step "Remove archive from $hostName"
    }

    # 5. Обновляем Swarm с ManagerHost.
    # stop-first обязателен при двух репликах и placement max 1 per node:
    # временная третья задача не сможет разместиться на этих двух узлах.
    $updateCommand = @(
        "sudo docker service update",
        "--detach=false",
        "--image '$image'",
        "--no-resolve-image",
        "--update-parallelism 1",
        "--update-order stop-first",
        "--update-delay ${UpdateDelaySeconds}s",
        "--update-monitor ${UpdateMonitorSeconds}s",
        "--update-failure-action rollback",
        "'$ServiceName'"
    ) -join " "

    Invoke-Ssh -HostName $ManagerHost `
        -RemoteCommand $updateCommand `
        -Step "Update Swarm service on manager $ManagerHost"

    # 6. Показываем состояние сервиса и задач.
    Invoke-Ssh -HostName $ManagerHost `
        -RemoteCommand "sudo docker service ls --filter name='$ServiceName'; sudo docker service ps '$ServiceName' --no-trunc" `
        -Step "Check service status"

    Write-Host "`nDeploy completed. Image: $image" -ForegroundColor Green
    Write-Host "Local archive retained: $localArchive"
}
catch {
    Write-Host "`nDEPLOY FAILED: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "Не удаляй локальный архив: $localArchive"
    Write-Host "Проверь состояние: ssh $SshUser@$ManagerHost 'sudo docker service ps $ServiceName'"
    exit 1
}

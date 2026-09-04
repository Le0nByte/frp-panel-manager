# Download latest release from github
#
# ⚠️ 上游遗留脚本，本仓库（fork）不使用，也不要用它安装本平台的客户端。
# 该脚本从上游 VaalaCat/frp-panel 的 Release 下载二进制，那是上游版本，
# 不含本仓库的端口中心、事务安全与审计能力，客户端还会缺 EVENT_INSPECT_PORTS
# 等本仓库新增的 RPC，端口扫描将不可用。另外它默认装到 C:\frpp\，与本仓库
# 文档推荐的 D:\frp-panel\ 不一致。
# 本仓库的安装步骤见 docs/usage-guide.md §1.3 与 WebUI 侧边栏的「操作指南」页。
if($PSVersionTable.PSVersion.Major -lt 5){
    Write-Host "Require PS >= 5,your PSVersion:"$PSVersionTable.PSVersion.Major -BackgroundColor DarkGreen -ForegroundColor White
    exit
}
$clientrepo = "VaalaCat/frp-panel"
#  x86 or x64
if ([System.Environment]::Is64BitOperatingSystem) {
    if ([System.Environment]::Is64BitProcess) {
        $file = "frp-panel-windows-amd64.exe"
    } else {
        $file = "frp-panel-windows-arm64.exe"
    }
} else {
    Write-Host "Your system is 32-bit, please use 64-bit operating system" -BackgroundColor DarkGreen -ForegroundColor White
    exit
}

#重复运行自动更新
if (Test-Path "C:\frpp\frpp.exe") {
    Write-Host "frp panel client already exists, delete and reinstall" -BackgroundColor DarkGreen -ForegroundColor White
    C:/frpp/frpp.exe stop
    C:/frpp/frpp.exe uninstall
    Start-Sleep -Seconds 3
    Remove-Item "C:\frpp\frpp.exe" -Recurse
}

#TLS/SSL
Write-Host "Check network connection to google" -BackgroundColor DarkGreen -ForegroundColor White
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$networkAvailable = Test-Connection -ComputerName google.com -Count 1 -ErrorAction SilentlyContinue
if([string]::IsNullOrEmpty($networkAvailable)){
    $download = "https://ghfast.top/https://github.com/$clientrepo/releases/latest/download/$file"
    Write-Host "Location:CN,use mirror address" -BackgroundColor DarkRed -ForegroundColor Green
}else{
    $download = "https://github.com/$clientrepo/releases/latest/download/$file"
    Write-Host "Location: google ok,connect directly!" -BackgroundColor DarkRed -ForegroundColor Green
}
echo $download
Invoke-WebRequest $download -OutFile "C:\frpp.exe"
New-Item -Path "C:\frpp" -ItemType Directory -ErrorAction SilentlyContinue
Move-Item -Path "C:\frpp.exe" -Destination "C:\frpp\frpp.exe"
C:\frpp\frpp.exe install $args
C:\frpp\frpp.exe start
Write-Host "Enjoy It!" -BackgroundColor DarkGreen -ForegroundColor Red
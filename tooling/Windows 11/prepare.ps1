# Install Java 23
$url = "https://download.oracle.com/java/23/latest/jdk-23_windows-x64_bin.msi"
$output = "$env:TEMP\jdk-23_windows-x64_bin.msi"
Invoke-WebRequest -Uri $url -OutFile $output
Start-Process msiexec.exe -Wait -ArgumentList "/i $output /qn"
Remove-Item $output

# Install chocolatey
Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Instal general dependencies
## This sometimes works and then you cannot find the executables in PATH, it's a known issue, I do not know what causes it
choco install -y sbt


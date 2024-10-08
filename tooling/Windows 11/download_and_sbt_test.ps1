# download the repository and enter it
$url = "https://github.com/aherlihy/tyql/archive/refs/heads/main.zip"
$downloadPath = "$env:TEMP\downloaded.zip"
$extractPath = "$env:TEMP\extracted"
Invoke-WebRequest -Uri $url -OutFile $downloadPath
if (-not (Test-Path $downloadPath)) {
    throw "Download failed: File not found at $downloadPath"
}
Expand-Archive -Path $downloadPath -DestinationPath $extractPath -Force
if (-not (Test-Path $extractPath)) {
    throw "Extraction failed: Directory not found at $extractPath"
}
$extractedDir = Get-ChildItem -Path $extractPath -Directory | Select-Object -First 1
if (-not $extractedDir) {
    throw "No directory found in the extracted contents"
}
Set-Location -Path $extractedDir.FullName


sbt test


# clean up
Set-Location -Path $env:TEMP
Remove-Item -Path $downloadPath -Force
Remove-Item -Path $extractPath -Recurse -Force


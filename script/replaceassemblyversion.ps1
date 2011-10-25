param($filename, $type, $version)
# Avoid line breaks at 80th column
$Host.UI.RawUI.BufferSize = New-Object Management.Automation.Host.Size (500, 25)
$content = Get-Content -path $filename
$content | 
foreach {if ($_ -match "^\[assembly: $type\(`".*`"\)\]$") {$_ -replace "^\[assembly: $type\(`".*`"\)\]$", "[assembly: $type(`"$version`")]"} else {$_}} |
out-string -width 100

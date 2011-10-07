param($filename, $commaVersion, $dottedVersion)
$content = Get-Content -path $filename
$content | 
foreach {$_ -replace "FILEVERSION .*$", "FILEVERSION $commaVersion"} |
foreach {$_ -replace "PRODUCTVERSION .*$", "PRODUCTVERSION $commaVersion"} |
foreach {$_ -replace "VALUE `"FileVersion`", .*$", "VALUE `"FileVersion`", `"$dottedVersion`""} |
foreach {$_ -replace "VALUE `"ProductVersion`", .*$", "VALUE `"ProductVersion`", `"$dottedVersion`""} 
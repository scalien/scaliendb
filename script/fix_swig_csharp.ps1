param($filename)
# Avoid line breaks at 80th column
$Host.UI.RawUI.BufferSize = New-Object Management.Automation.Host.Size (500, 25)
$setterBadSignature = "SWIGEXPORT void SWIGSTDCALL CSharp_SDBP_Buffer_data_set\(void \* jarg1, int jarg2\) {"
$setterGoodSignature = "SWIGEXPORT void SWIGSTDCALL CSharp_SDBP_Buffer_data_set(void * jarg1, intptr_t jarg2) {"
$getterBadSignature = "SWIGEXPORT int SWIGSTDCALL CSharp_SDBP_Buffer_data_get\(void \* jarg1\) {"
$getterGoodSignature = "SWIGEXPORT intptr_t SWIGSTDCALL CSharp_SDBP_Buffer_data_get(void * jarg1) {"
$getterFound = 0
$content = Get-Content -path $filename
$content | 
foreach {if ($_ -match $setterBadSignature) {$_ -replace $setterBadSignature, $setterGoodSignature} else {$_}} |
foreach {if ($_ -match $getterBadSignature) {$getterFound = 1; $_ -replace $getterBadSignature, $getterGoodSignature} else {$_}} |
foreach {if (($getterFound -eq 1) -and ($_ -match "int jresult ;")) {$getterFound = 0; $_ -replace "int jresult ;", "intptr_t jresult ;"} else {$_}} |
out-string -width 100

ZipFolder WScript.Arguments(0), WScript.Arguments(1)

' This function expects fully specified path names!
Function ZipFolder(myFolder, myZipFile)
    Dim itemCount
    Dim objApp, objFolder, objFSO, objItem, objTxt
    Dim zipNameSpace

    Const ForWriting = 2

    itemCount = 0

    ' Make sure the path ends with a backslash
    If Right(myFolder, 1) <> "\" Then
        myFolder = myFolder & "\"
    End If

    ' Create an empty ZIP file
    Set objFSO = CreateObject("Scripting.FileSystemObject")
    Set objTxt = objFSO.OpenTextFile(myZipFile, ForWriting, True)
    objTxt.Write "PK" & Chr(5) & Chr(6) & String(18, Chr(0))
    objTxt.Close
    Set objTxt = Nothing

    ' Abort on errors
    If Err Then
        ZipFolder = Array(Err.Number, Err.Source, Err.Description)
        Err.Clear
        On Error Goto 0
        Exit Function
    End If
        
    ' Create a Shell object
    Set objApp = CreateObject("Shell.Application")
    Set zipNameSpace = objApp.NameSpace(myZipFile)
    
    ' Copy the files to the compressed folder
    For Each objItem in objApp.NameSpace(myFolder).Items
        If objItem.IsFolder Then
            ' Check if the subfolder is empty, and if
            ' so, skip it to prevent an error message
            Set objFolder = objFSO.GetFolder(objItem.Path)
            If objFolder.Files.Count + objFolder.SubFolders.Count = 0 Then
                ' dummy
            Else
                zipNameSpace.CopyHere objItem
                itemCount = itemCount + 1
            End If
        Else
            zipNameSpace.CopyHere objItem
            itemCount = itemCount + 1
        End If
        
        ' Wait until compression is finished
        Do Until zipNameSpace.Items.Count = itemCount
            WScript.Sleep 100
        Loop

        If Err Then
            WScript.Echo "Error " & Err.Number & " " & Err.Source & " " & Err.Description
            Exit Function
        End If
    Next

    Set objFolder = Nothing
    Set objFSO    = Nothing

End Function

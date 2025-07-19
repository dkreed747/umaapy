# Helper script to convert gitlab wiki to sphinx compatible restructured text

Get-ChildItem -Recurse -Filter *.md | ForEach-Object { 
    pandoc $_.FullName -f markdown -t rst -o "$($_.DirectoryName)\$($_.BaseName).rst" 
}

Get-ChildItem -Recurse -Filter *.rst | ForEach-Object {
  $projRoot = (Get-Location).Path
  $relPath  = $_.FullName.Substring($projRoot.Length + 1)
  $destFull  = Join-Path $projRoot "..\..\umaapy\docs\wiki\$relPath"
  New-Item -ItemType Directory -Force -Path (Split-Path $destFull -Parent)
  Move-Item $_.FullName -Destination $destFull
}

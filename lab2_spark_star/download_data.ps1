$ErrorActionPreference = 'Stop'

New-Item -ItemType Directory -Force data | Out-Null
$base = 'https://raw.githubusercontent.com/MAIStudents/BigDataSpark/main/%D0%B8%D1%81%D1%85%D0%BE%D0%B4%D0%BD%D1%8B%D0%B5%20%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D0%B5'

Invoke-WebRequest -Uri "$base/MOCK_DATA.csv" -OutFile 'data\MOCK_DATA.csv'
1..9 | ForEach-Object {
    $remote = "MOCK_DATA%20%28$($_)%29.csv"
    $local = "data\MOCK_DATA ($($_)).csv"
    Write-Host "Downloading $local"
    Invoke-WebRequest -Uri "$base/$remote" -OutFile $local
}

Write-Host 'Downloaded CSV files:'
Get-ChildItem .\data\*.csv | Select-Object Name, Length

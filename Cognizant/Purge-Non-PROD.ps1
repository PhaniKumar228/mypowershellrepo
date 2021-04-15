$limit = (Get-Date).AddDays(-7)

Get-ChildItem -Path \\gmo\sqlbackups\dca_dev_ris\*  -Recurse -Force | Where-Object { !$_.PSIsContainer -and $_.CreationTime -lt $limit } | Remove-Item -Force

$limit = (Get-Date).AddDays(-15)

Get-ChildItem -Path \\gmo\sqlbackups\dca_dev_inv\*  -Recurse -Force | Where-Object { !$_.PSIsContainer -and $_.CreationTime -lt $limit } | Remove-Item -Force

$limit = (Get-Date).AddDays(-7)

Get-ChildItem -Path \\gmo\sqlbackups\dca_dev_inf\*  -Recurse -Force | Where-Object { !$_.PSIsContainer -and $_.CreationTime -lt $limit } | Remove-Item -Force

$limit = (Get-Date).AddDays(-16)

Get-ChildItem -Path \\gmo\sqlbackups\dca_dev_css\*  -Recurse -Force | Where-Object { !$_.PSIsContainer -and $_.CreationTime -lt $limit -and $_.fullname -notmatch 'crp' }  | Remove-Item -Force
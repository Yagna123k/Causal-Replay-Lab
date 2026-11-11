$ErrorActionPreference = "Stop"
$BaseUrl = if ($env:BASE_URL) { $env:BASE_URL } else { "http://localhost:8080" }
$Txn = "txn-demo-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"

Write-Host "1. Ingesting workflow for $Txn..."
$events = @(
    @{ event_id = "$Txn-e1"; transaction_id = $Txn; event_type = "TRANSACTION_CREATED"; sequence_number = 1; created_at = "2026-05-01T10:00:01Z"; payload = @{ account_id = "acc-demo"; amount = 250; currency = "USD"; initial_balance = 100 } },
    @{ event_id = "$Txn-e2"; transaction_id = $Txn; event_type = "ACCOUNT_VALIDATED"; sequence_number = 2; created_at = "2026-05-01T10:00:02Z"; payload = @{ valid = $true } },
    @{ event_id = "$Txn-e3"; transaction_id = $Txn; event_type = "DEBIT_INITIATED"; sequence_number = 3; created_at = "2026-05-01T10:00:03Z"; payload = @{ amount = 250; currency = "USD" } }
)
foreach ($e in $events) {
    Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/events" -ContentType "application/json" -Body ($e | ConvertTo-Json -Depth 5) | Out-Null
}

Write-Host "2. Processing original baseline..."
$orig = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/transactions/$Txn/process"
Write-Host "Original status: $($orig.status) error: $($orig.error)"

Write-Host "3. Creating and starting deterministic replay..."
$rep = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/replays" -ContentType "application/json" -Body (@{ transaction_id = $Txn } | ConvertTo-Json)
$start = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/replays/$($rep.session_id)/start"
Write-Host "Replay $($rep.session_id) status: $($start.session.status)"

Write-Host "4. Inspecting failure report..."
Invoke-RestMethod -Uri "$BaseUrl/api/v1/replays/$($rep.session_id)/failures" | ConvertTo-Json -Depth 6

Write-Host "5. Inspecting checkpoints..."
Invoke-RestMethod -Uri "$BaseUrl/api/v1/replays/$($rep.session_id)/checkpoints" | ConvertTo-Json -Depth 6

Write-Host "6. Comparing replay vs original..."
Invoke-RestMethod -Uri "$BaseUrl/api/v1/replay-comparisons?left=$($rep.session_id)&right=original" | ConvertTo-Json -Depth 6

param([int]$UiProcessId = 0, [string]$ProcessName = 'DeepSeek Harness', [int]$HostProcessId = 0)
$ErrorActionPreference='Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
Add-Type @'
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
public class Stage3Windows {
 public delegate bool Callback(IntPtr h, IntPtr l);
 [DllImport("user32.dll")] static extern bool EnumWindows(Callback c, IntPtr l);
 [DllImport("user32.dll")] static extern bool IsWindowVisible(IntPtr h);
 [DllImport("user32.dll")] static extern uint GetWindowThreadProcessId(IntPtr h, out uint pid);
 public static int[] VisiblePids() {
  var result=new HashSet<int>();
  EnumWindows((h,l)=>{uint pid; GetWindowThreadProcessId(h,out pid); if(IsWindowVisible(h)) result.Add((int)pid); return true;},IntPtr.Zero);
  var values=new int[result.Count];result.CopyTo(values);return values;
 }
}
'@
$visible=[Stage3Windows]::VisiblePids()
if($UiProcessId -gt 0) {
 $ui=Get-Process -Id $UiProcessId -ErrorAction Stop
 $ProcessName=$ui.ProcessName
} else {
 $candidates=@(Get-Process -Name $ProcessName -ErrorAction SilentlyContinue | Where-Object {$visible -contains $_.Id -and $_.MainWindowTitle -match 'DeepSeek|DSH'})
 if($HostProcessId -eq 0 -and $candidates.Count -ne 1){throw 'Expected one visible DeepSeek Harness window. Use --ui-pid to select its process.'}
 $ui=$candidates | Select-Object -First 1
}
$matching=@(Get-Process -Name $ProcessName -ErrorAction SilentlyContinue | Where-Object {$visible -contains $_.Id -and $_.MainWindowTitle -match 'DeepSeek|DSH'})
$hostAlive=$false
if($HostProcessId -gt 0){$hostAlive=$null -ne (Get-Process -Id $HostProcessId -ErrorAction SilentlyContinue)}
[pscustomobject]@{at=[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds();pid=if($ui){$ui.Id}else{0};processName=$ProcessName;visible=$matching.Count -gt 0;hidden=$matching.Count -eq 0;hostPid=$HostProcessId;hostAlive=$hostAlive} | ConvertTo-Json -Compress

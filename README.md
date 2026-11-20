# DS1 — Shared album

Run all commands from the repo root. Replace `<...>` with your values.

| Node | Machine | node_id | Fixed IP |
|------|---------|---------|----------|
| 1 | Windows | `1` | `192.168.10.11` |
| 2 | Mac | `2` | `192.168.10.12` |
| 3 | Mac | `3` | `192.168.10.13` |

Floating IP: `192.168.10.50` (set in `src/ds1/config.py`). Web page: `http://192.168.10.50:8080`

<img src="docs/qr.png" alt="QR code for http://192.168.10.50:8080" width="220">

---

## Windows (administrator PowerShell)

### Install

```powershell
irm https://astral.sh/uv/install.ps1 | iex
git clone https://github.com/RohitChillara/ds1.git
cd ds1
uv sync
```

### Network

```powershell
netsh interface show interface
netsh interface ipv4 set address "Ethernet" static 192.168.10.11 255.255.255.0 192.168.10.1
netsh interface ipv4 show config name="Ethernet"
netsh interface set interface "VMware Network Adapter VMnet1" disable
netsh interface set interface "VMware Network Adapter VMnet8" disable
netsh interface set interface "Wi-Fi" disable
```

### Firewall

```powershell
Set-NetConnectionProfile -InterfaceAlias "Ethernet" -NetworkCategory Private
New-NetFirewallRule -DisplayName "ds1-tcp" -Direction Inbound -Protocol TCP -LocalPort 8000,8080 -Action Allow
New-NetFirewallRule -DisplayName "ds1-udp" -Direction Inbound -Protocol UDP -LocalPort 9000 -Action Allow
Get-NetFirewallRule -DisplayName "ds1-*" | Format-Table DisplayName, Enabled, Action
```

### Run

```powershell
uv run python -m ds1.node 1 --iface Ethernet
```

### Check

```powershell
netsh interface ipv4 show ipaddresses
curl.exe http://192.168.10.50:8080/api/status
```

### Clean up

```powershell
netsh interface ipv4 delete address "Ethernet" 192.168.10.50
netsh interface ipv4 set address "Ethernet" dhcp
netsh interface ipv4 set dnsservers "Ethernet" dhcp
netsh interface set interface "Wi-Fi" enable
netsh interface set interface "VMware Network Adapter VMnet1" enable
netsh interface set interface "VMware Network Adapter VMnet8" enable
Remove-NetFirewallRule -DisplayName "ds1-tcp"
Remove-NetFirewallRule -DisplayName "ds1-udp"
```

---

## macOS (Terminal)

### Install

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
git clone https://github.com/RohitChillara/ds1.git
cd ds1
uv sync
```

### Network

```bash
networksetup -listallhardwareports
networksetup -setmanual "<Ethernet service name>" 192.168.10.<12 or 13> 255.255.255.0 192.168.10.1
networksetup -setairportpower en0 off
ifconfig <en device>
```

### Permission for the floating IP

```bash
echo "$(whoami) ALL=(root) NOPASSWD: /sbin/ifconfig" | sudo tee /etc/sudoers.d/ds1
sudo chmod 440 /etc/sudoers.d/ds1
sudo -n /sbin/ifconfig <en device> | head -1
```

### Firewall

```bash
PY=$(realpath "$(uv run python -c 'import sys; print(sys.executable)')")
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add "$PY"
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --unblockapp "$PY"
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --getappblocked "$PY"
```

### Run

```bash
uv run ds1 <2 or 3> --iface <en device>
```

### Check

```bash
ifconfig <en device> | grep inet
curl http://192.168.10.50:8080/api/status
```

### Clean up

```bash
sudo ifconfig <en device> -alias 192.168.10.50
networksetup -setdhcp "<Ethernet service name>"
networksetup -setairportpower en0 on
sudo rm /etc/sudoers.d/ds1
```

---

## Phones

```
Wi-Fi: <router network>   Mobile data: off   "No internet": stay connected
http://192.168.10.50:8080
```

## Reset a node's data

```bash
rm -rf photos_json photos_bin epoch
```

```powershell
Remove-Item -Recurse -Force photos_json, photos_bin, epoch
```
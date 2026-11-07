# Solar System Supervisor

A personal supervisory system for a small solar installation. Its general goal is
to use solar power for my household needs rather than exporting more power than
the monitored loads use. Surplus can charge the battery; when solar production
is below measured usage, the shortfall is supplied by the grid.

The system monitors power use on one selected phase and controls a Hoymiles HM300
via MQTT. It communicates with the HM300 through a DX-DTU gateway running
OpenDTU.

This repository documents and maintains my own setup. It is not intended as a
general-purpose solar controller.

## Hardware

- Eight 100 W solar panels arranged as two subarrays:
  - Each subarray has two parallel strings, with two panels in series in each
    string (2S2P per subarray).
  - The subarrays are connected in parallel, for a total configuration of
    2S4P and 800 W nominal.
- A smart Y-connector between the subarrays, intended to prevent current from
  flowing backward from one subarray into the other
- Victron MPPT 75|15 charge controller
- 25.6 V, 100 Ah LiFePO4 battery, capable of supplying up to 100 A
- Victron Smart BatteryProtect 12/24 V–65 A
- Hoymiles HM300 inverter
- DX-DTU gateway running OpenDTU
- NOUS A1T power meters measuring selected appliances on a single phase
- Fuses installed:
  - Between the charge controller and battery
  - Between the battery and Smart BatteryProtect
  - Between the Smart BatteryProtect and HM300

## Control modes

- **Limit by usage** — Limits HM300 output to the measured power usage on the
  selected phase.
- **Limit by sun and usage** — Uses battery-voltage trends to adjust HM300 output
  and limits it to measured usage. It does not directly measure or calculate
  available solar power.
- **Limit by sun only** — Uses the same voltage-based control approach, but does
  not limit output according to measured usage.
- **Full power** — Commands the HM300 to output near its maximum.
- **Power off** — Puts the HM300 into standby. It remains electrically powered
  on but feeds no power into the grid.

### Voltage-based control

The voltage-based modes use two moving averages of battery voltage:

- A short-term average over 7 readings, which smooths brief fluctuations, such
  as those caused by leaf or cloud shadows passing over the panels.
- A long-term average over 500 readings, which represents the prevailing
  voltage trend under recent solar conditions.

The controller adjusts HM300 output to bring the short-term average toward the
long-term average. The aim is to keep battery voltage relatively stable. This
uses voltage behavior as an indirect signal of the balance between solar input
and system output; it is not a direct measurement of available solar power.

## Repository contents

- `deploy2dockstar.bash` and `deploy2vim1s.bash` copy the project from the
  development machine to the target machine.
- `deployservice.bash` installs the service on the target machine in
  `/etc/systemd/system/`.
- The mode scripts switch the system to their corresponding modes when run on
  the target machine.
- `mosquitto/mosquitto.conf` contains the Mosquitto configuration.
- `solar.py` is the main supervisory program.
- `systemd/solary.service` is the systemd unit file.

## Deployment and service management

Run the appropriate `deploy2...` script on the development machine to copy the
project to its target.

On the target machine, run `deployservice.bash` from the directory containing
the script. For example:

```bash
sudo bash ./deployservice.bash
```

Then reload systemd, enable the service to start at boot, and start it
immediately:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now solary.service
```

Check the service status with:

```bash
sudo systemctl status solary.service
```

To start the service for the current session without enabling it at boot:

```bash
sudo systemctl start solary.service
```

## Running manually

The system can also be started manually:

```bash
/usr/bin/python3 /home/piotr/solary/solar.py --verbose
```

## Logs and current state

While running, the system writes logs under the `logs` directory in the
installation directory. The current state and measurements are reflected in
the `solar.stats`.

From the installation directory, watch the stats file update with:

```bash
watch -n1 -d cat solar.stats
```

## Web frontend

A web-based frontend written in Unison can be used to monitor and control the
system remotely. It is not included in this repository.

## HM300 connection and startup caveat

The HM300 manual mentions it should not be connected directly to a battery. In
this installation, it is connected to the battery.

The HM300 is rated for an input current of 11.5 A. The battery can supply up to
100A. An oscilloscope measurement of HM300 power-on shows a very brief current
peak of up to 27A, which is over its rating. Thus, repeatedly powering the
HM300 on and off this way could damage the inverter over time.
To avoid repeated startup current peaks, this system does not switch off the
HM300 electrically. Instead, it puts the inverter into standby, keeping it on
24h. This way the internal capcitors of HM300 retain their charge and the
power-on current peak is avoided.

The Smart BatteryProtect is configured to electrically disconnect the HM300 if
battery voltage falls below a threshold and reconnect it after the voltage
rises to a set level. This threshold is below the thresholds used in this
software.  Thus, the Smart BatteryProtect is a low-voltage /
low-battery-capacity safeguard that comes into play if the software or the
infastructure running this software fails.  The sun will eventually recharge
the battery and the Smart BetteryProtect will power the HM300 back on. Thus the
startup current peak, though less likely, may still occur. A suitable peak
current-limiter between battery and HM3300 would be a welcome addition.

If the control software or its communication with the HM300 fails, the system
will not automatically scale inverter output to match usage or solar
conditions. The hardware will continue to operate, but the HM300 may not be
controlled as intended, though the Smart BatteryProtect and charge controller
will keep the battery in check.

## Safety and limitations

This software controls equipment in a personal solar installation. Its readings
and behavior depend on the connected devices, configuration, and conditions at
the installation. The battery-to-HM300 connection described above is contrary
to the HM300 manuals’ guidance and carries a risk of equipment damage. This
software is provided for my own use and is not a certified safety or
power-management system.

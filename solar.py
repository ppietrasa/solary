#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import time, copy, json, argparse, logging, logging.handlers, sys, math
import collections, io, datetime
from simple_pid import PID

VERSION='0.0.1'
BROKER='docstar'
BROKER_PORT=1883

log  = logging.getLogger(__name__)

class Timer(object):
    """A timer.

    Arguments
    ---------
    seconds (int)
      seconds to count down
    """
    def __init__(self, seconds = 0):
        self.seconds = seconds
        self.start = time.time()

    def __enter__(self):
        return self

    def __exit__(self, typ, val, trcbk):
        pass

    @property
    def expired(self):
        """Returns True if timer expired.

        Note
        ----
        Because the implementation is based on system clock the Timer may
        occasionally prematurely expire, for instance, if system clock has been
        set back.
        """
        return self.elapsed >= self.seconds

    def restart(self, seconds = None):
        """Restarts the timer."""
        self.start = time.time()
        if seconds:
            self.seconds = seconds

    @property
    def remaining(self):
        """Returns time remaining."""
        return max(0, self.seconds - self.elapsed)

    @property
    def elapsed(self):
        """Returns time elapsed since last restart."""
        elapsed =  time.time() - self.start
        if elapsed < 0:
            # time has been set back -> we better expire
            elapsed = self.seconds
        return int(round(elapsed))


class InverterOnBattery(object):
    """
    Inverter connected to a battery.
    """
    TOPIC_SUFFIXES=[
        # R Indicates whether the inverter is reachable - 0 or 1
        '/status/reachable',
        # R Indicates whether the inverter is producing AC power - 0 or 1
        '/status/producing',
        # R Unix timestamp of last inverter statistics udpate
        #   seconds since JAN 01 1970 (UTC)
        #'/status/last_update',

        # R AC active power in watts - Watt (W)
        '/0/power',
        # R DC power in watts - Watt (W)
        #'/0/powerdc',
        # R Power factor in percent - %
        #'/0/powerfactor', # R Temperature of inverter in °C
        '/0/temperature',

        # Energy converted to AC per day in Wh
        '/0/yieldday',

        # R DC power of specific input in watt - Watt (W)
        '/1/power',
        # R DC voltage of specific input in volt - Volt (V)
        '/1/voltage',

        # R Current applied production limit of the inverter - Watt (W)
        '/status/limit_absolute',
        ]

    SUMMER_LOW_VOLTAGE = 24.5
    SUMMER_HGH_VOLTAGE = 25.9
    WINTER_LOW_VOLTAGE = 25.6
    WINTER_HGH_VOLTAGE = 26.7
    ABS_VOLTAGE = 28.4
    HGH_TEMPERATURE = 50

    def __init__(self, serial, bat_capacity=1280, base_topic='solar/'):
        """
        Parameters
        ----------
        publisher: obj
           Paho MQTT client
        serial : str
           Serial number of the inverter
        bat_capacity: int
           The battery capacity in Wh.
        """
        # an MQTT client friend class, which will be set later by the
        # client itself
        self.publisher = None
        self.serial = serial
        self.capacity_map = {
            29.2 :    1  * bat_capacity,
            27.2 :    1  * bat_capacity,
            26.8 : 0.99  * bat_capacity,
            26.6 : 0.90  * bat_capacity,
            26.4 : 0.70  * bat_capacity,
            26.2 : 0.40  * bat_capacity,
            26.0 : 0.30  * bat_capacity,
            25.8 : 0.20  * bat_capacity,
            25.6 : 0.17  * bat_capacity,
            25.0 : 0.14  * bat_capacity,
            24.0 : 0.09  * bat_capacity,
            22.0 :    0  * bat_capacity,
        }
        self.t_prefix = base_topic + serial # topic prefix
        self.prev_state = dict()
        self.prev_timestamp = int(time.time())
        self.curr_state = dict()
        self.curr_timestamp = int(time.time())
        self.yieldyesterday = 0
        self._voltage_queue = collections.deque([25.6, 25.6], 540)
        self.voltage_reading_num = 0
        self._on_voltage_queue = collections.deque([25.6, 25.6], 18)
        self._off_voltage_queue = collections.deque([24.5, 24.5], 18)

        #
        # see _send_cmd(), where (and only where) these are employed
        #
        self._sending_command = False
        self._prev_cmd = None
        self._prev_val = None
        self._prev_verify_f = None
        # 120: consider the commands issued expired after 2 minutes
        self._prev_timer = Timer(120)


    @property
    def max_power(self):
        '''Maximum power the inverter can safely output.'''
        # The inverter's input should not exceed 11A and the minimal battery
        # voltage is 22V. It is at the minimal voltage that the inverter would
        # try to pull max current.
        return 11 * 22

    @property
    def topics(self):
        return [ self.t_prefix + t_suffix
                    for t_suffix in InverterOnBattery.TOPIC_SUFFIXES ]


    def update(self, topic, payload):
        """Updates the current state of the inverter. Returns True if the state
        changed since previous update."""
        prev_state = copy.copy(self.curr_state)
        if topic in self.topics:
            self.curr_state[topic] = payload

        if '/1/voltage' in topic:
            new_voltage = float(payload)
            self._voltage_queue.append(new_voltage)
            self.voltage_reading_num += 1
            if self.is_on:
                self._on_voltage_queue.append(new_voltage)
            else:
                self._off_voltage_queue.append(new_voltage)

        return prev_state != self.curr_state

    @property
    def is_reachable(self):
        return self.curr_state.get(
            self.t_prefix + '/status/reachable', False) == b'1'

    @property
    def is_producing(self):
        return self.curr_state.get(
            self.t_prefix + '/status/producing', True) == b'1'

    @property
    def yieldday(self):
        return int(self.curr_state.get(self.t_prefix + '/0/yieldday', 0))

    @property
    def temperature(self):
        """Temperature of the inverter in °C."""
        return float(self.curr_state.get(self.t_prefix + '/0/temperature', 23))

    @property
    def prev_voltage(self):
        """Previous voltage of the battery."""
        return float(self._voltage_queue[-2])

    @property
    def curr_voltage(self):
        """Current voltage of the battery."""
        return float(self._voltage_queue[-1])

    #def n_readings_sma_voltage(self, n):
    #    """Average voltage over `n` observations"""
    #    voltages = list(self._voltage_queue)
    #    my_n = min(len(voltages), n)
    #    return float(sum(voltages[-my_n:]) / my_n)

    def n_readings_sma_voltage(self, n):
        """Average voltage over `n` observations"""
        numerator = 0; denominator = 0;
        reversed_q = reversed(self._voltage_queue)
        for (k, v) in enumerate(reversed_q):
            if k < n:
                numerator   += v
                denominator += 1
            else:
                break
        return numerator/max(1, denominator)


    def n_readings_wma_voltage(self, n):
        """Average voltage over `n` observations"""
        numerator = 0; denominator = 0
        for (k, v) in enumerate(reversed(self._voltage_queue)):
            if k < n:
                numerator   += (n-k)*v
                denominator += (n-k)
            else:
                break
        return numerator/max(1, denominator)

    @property
    def sma_voltage(self):
        """Average voltage over the last few observations (up to 18 currently)"""
        return float(sum(self._voltage_queue) / len(self._voltage_queue))

    @property
    def sma_voltage(self):
        """Average voltage over the last few observations (up to 18 currently)"""
        return float(sum(self._voltage_queue) / len(self._voltage_queue))

    @property
    def on_sma_voltage(self):
        """Average voltage over the last few (18 currently) observations while
        the inverter is on."""
        return float(sum(self._on_voltage_queue) / len(self._on_voltage_queue))

    @property
    def off_sma_voltage(self):
        """Average voltage over the last few (18 currently) observations while
        the inverter is off."""
        return float(sum(self._off_voltage_queue) / len(self._off_voltage_queue))

    @property
    def power(self):
        return float(self.curr_state.get(self.t_prefix + '/0/power', 300))

    @staticmethod
    def interpolate_linear(x, x0, y0, x1, y1):
        return y0 + (x - x0) * (y1 - y0) / (x1 - x0)

    @staticmethod
    def voltage_to_capacity(voltage, capacity_map):
        sorted_voltages = sorted(capacity_map.keys())

        if voltage <= min(sorted_voltages):
            return capacity_map[min(sorted_voltages)]
        elif voltage >= max(sorted_voltages):
            return capacity_map[max(sorted_voltages)]
        else:
            for i in range(len(sorted_voltages) - 1):
                voltage1, voltage2 = sorted_voltages[i], sorted_voltages[i + 1]
                capacity1, capacity2 = capacity_map[voltage1], \
                    capacity_map[voltage2]

                if voltage1 <= voltage <= voltage2:
                    return InverterOnBattery.interpolate_linear(
                            voltage, voltage1, capacity1, voltage2, capacity2
                            )
        assert False, "Unreachable code!"

    @property
    def curr_capacity(self):
        """Current *estimated* power in Watt."""
        return self.voltage_to_capacity(self.curr_voltage, self.capacity_map)

    @property
    def prev_capacity(self):
        """Previous *estimated* power in Watt."""
        return self.voltage_to_capacity(self.prev_voltage, self.capacity_map)

    @property
    def battery_cycling_power(self):
        """The *estimated* charge (positv) or discharge (negative) rate of
        battery in Watt."""
        return 0 # TODO

    @property
    def battery_charging(self):
        return battery_cycling_power > 0

    @property
    def battery_discharging(self):
        return battery_cycling_power < 0

    @property
    def is_on(self):
        return self.is_producing

    @property
    def is_off(self):
        return not self.is_on

    def _send_cmd(self, cmd, val, verify_f):
        """Sends a command to the inverter.
        Does not allow new commands, unless the previous one is confirmed (the
        request is fulfilled), or a command expires due to a timeout.

        Parameters
        ----------
        cmd: str
            The command to send.
        val: obj,
            The value set by the command
        verify_f:
            A predicate to check whether the request has been fulfilled
            (command considered confirmed).
        """
        assert self.publisher, "Publisher not set!"
        if self._sending_command:
            if self._prev_verify_f():
                log.debug("previous '%s, %s' confirmed"
                    % (self._prev_cmd, self._prev_val))
                self._sending_command = False
            elif self._prev_timer.expired:
                log.debug("previous '%s, %s' expired after %s s"
                    % (self._prev_cmd, self._prev_val,
                       self._prev_timer.elapsed))
                self._sending_command = False
            else:
                log.debug("waiting for '%s, %s', %s s .."
                    % (self._prev_cmd, self._prev_val,
                       self._prev_timer.elapsed))
        if not self._sending_command:
            log.debug("sending '%s, %s'" % (cmd, val))
            self.publisher.publish(self.t_prefix + cmd, val)
            self._sending_command = True
            self._prev_cmd = cmd
            self._prev_val = val
            self._prev_verify_f = verify_f
            self._prev_timer.restart()

    def power_on(self):
        log.info("power_on")
        self._send_cmd('/cmd/power', 1, lambda : self.is_on)

    def power_off(self):
        log.info("power_off")
        self._send_cmd('/cmd/power', 0, lambda : self.is_off)

    def restart(self):
        log.info("restart")
        yieldday_at_restarttime = self.yieldday
        def _restarted():
            has_restarted = self.yieldday < 1
            if has_restarted:
                self.yieldyesterday = yieldday_at_restarttime
            return has_restarted
        self._send_cmd('/cmd/restart', 1, _restarted)

    @property
    def power_limit(self):
        pwlimit = math.ceil(float(self.curr_state.get(self.t_prefix +
            '/status/limit_absolute', 300)))
        return pwlimit

    @power_limit.setter
    def power_limit(self, limit_new):
        log.info("power_limit := %s" % limit_new)
        self._send_cmd('/cmd/limit_nonpersistent_absolute', int(limit_new),
            lambda : self.power_limit == limit_new)

    def stats_as_str(self):
        table = collections.OrderedDict({})
        table['status:'] = 'on' if self.is_on else 'OFF'
        table['reachable:'] = 'yes' if self.is_reachable else 'NO'
        table['feeding:'] = "%.1f W" % self.power
        table['limit:'] = str(self.power_limit) + ' W'
        table['temperature:'] = str(self.temperature) + '°C'
        table['bat. voltage:'] = "%.1f V" % self.curr_voltage
        table['bat. sma_voltage:'] = "%.1f V" % self.sma_voltage
        table['est. bat. capacity:']    = "%.1f Wh" % self.curr_capacity
        table['yield day:'] = "%s Wh" % self.yieldday
        table['yield yesterday:'] = "%s Wh" % self.yieldyesterday
        max_key_length = max(len(key)
            for key in table.keys())
        max_value_length = max(len(str(value))
            for value in table.values()) + 2

        title = "Inverter"
        title_length = len(title)

        table_width = max_key_length + max_value_length + 1

        result = f"{title:^{table_width}}\n"
        result += f"{'-' * table_width}\n"

        for key, value in table.items():
            value_str = str(value)
            result += (
                f"{key:<{max_key_length}}"
                f" {value_str:>{max_value_length}}\n"
            )
        return result


class PowerMeters(object):
    TOPICS = [
        'tele/office/SENSOR',
        'tele/tv/SENSOR',
        'tele/fridge/SENSOR'
        ]

    def __init__(self, baseload = 27):
        """
        Parameters
        ----------
        baseload: int
            The consumption base load not covered by any power meters. The
            baseload will be included in the total_power reported by all the
            power meters.
            Default = 27 ; 18.8 (router) + 4.8 (docstar) + 3.6 (kitchen/canteen)
        """
        self.topics = PowerMeters.TOPICS
         # maps sensor name (like 'tv') to power in W
        self.curr_state = dict()
        self.baseload = baseload

    def update(self, topic, payload):
        prev_state = copy.copy(self.curr_state)
        if topic in self.topics:
            payload_json = payload
            if payload_json:
                payload_dict = json.loads(payload_json)
                pwmeter_power = payload_dict.get("ENERGY", {}).get("Power", 0)
                pwmeter_name  = topic.split('/')[1]
                self.curr_state[pwmeter_name] = pwmeter_power
        return prev_state != self.curr_state

    def stats_as_str(self):
        curr_state = collections.OrderedDict(self.curr_state)
        curr_state['baseload'] = self.baseload
        curr_state['total']    = self.total_power
        max_name_length = max(len(pwmeter_name)
            for pwmeter_name in curr_state.keys())
        max_value_length = max(len(str(value))
            for value in curr_state.values()) + 2
        title = "Power meters"
        title_length = len(title)

        table_width = max_name_length + max_value_length + 2

        result = f"{title:^{table_width}}\n"
        result += f"{'-' * table_width}\n"

        for pwmeter_name, value in curr_state.items():
            value_str = str(value) + ' W'
            result += (
                f"{pwmeter_name:<{max_name_length}}: "
                f"{value_str:>{max_value_length}}\n"
            )
        return result

    @property
    def total_power(self):
        """Total power measured by all the power meters"""
        total_power=self.baseload
        for sensor in self.curr_state:
            sensor_power = self.curr_state.get(sensor, 0)
            total_power += sensor_power
        return total_power

class PowerOperator(mqtt.Client):
    """Monitors power meters, battery, inverter and adjusts the inverter based
    on monitored inputs."""
    def __init__(self, inverter, pwmeters):
        super(PowerOperator, self).__init__("solar@%s" % BROKER)
        self.inverter = inverter
        self.pwmeters = pwmeters
        # we are a friend of inverter, which sends out commands on our behalf
        self.inverter.publisher = self
        self.lower_pw_bound = pwmeters.total_power - 15
        self.upper_pw_bound = pwmeters.total_power + 15
        self.pw_bound_midpoint = pwmeters.total_power
        self._lst_msg = 'None'
        self.prev_date = datetime.date.today()
        self._abs_timer = Timer(3 * 60)
        self._absorbing = False

        self.power = inverter.max_power / 2
        self.power = 11

        self.voltage_reading_num = 0

        #p -235 (oscilates)
        #p -175 (oscilates)

        self.pid = PID(-230.0, -0, 300, setpoint=25.8,
                proportional_on_measurement=True,
                differential_on_measurement=False)
        self.pid_sample_time = 5

        # runrise/sunset times per month of year
        self.sunrise_sunset_times = {
            1:  {"sunrise": datetime.time(8,44),
                 "sunset": datetime.time(16,10)},

            2:  {"sunrise": datetime.time(8,11),
                 "sunset": datetime.time(17,13)},

            3:  {"sunrise": datetime.time(7,21),
                 "sunset": datetime.time(18,3)},

            4:  {"sunrise": datetime.time(7,8),
                 "sunset": datetime.time(20,1)},

            5:  {"sunrise": datetime.time(6,9),
                 "sunset": datetime.time(20,47)},

            6:  {"sunrise": datetime.time(5,38),
                 "sunset": datetime.time(21,21)},

            7:  {"sunrise": datetime.time(6,5),
                 "sunset": datetime.time(21,5)},

            8:  {"sunrise": datetime.time(6,49),
                 "sunset": datetime.time(20,22)},

            9:  {"sunrise": datetime.time(7,47),
                 "sunset": datetime.time(19,3)},

            10: {"sunrise": datetime.time(8,26),
                 "sunset": datetime.time(18,4) },

            11: {"sunrise": datetime.time(8,9),
                 "sunset": datetime.time(16,7) },

            12: {"sunrise": datetime.time(8,49),
                 "sunset": datetime.time(15,44)}
            }
        # How big a portion of energy consumption to feed. Between 0 and 1.
        self.pwr_factor = 0.5
        self._pwr_factor_timer = Timer(7)

    def log(self, msg):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._lst_msg = "%s %s" % (timestamp, msg)
        log.info(msg)

    def on_message(self, client, userdata, msg):
        assert client == self, "on_connect: unexpected client"
        log.debug(msg.topic+" "+str(msg.payload))
        pwmeters_udpated = self.pwmeters.update(msg.topic, msg.payload)
        inverter_updated = self.inverter.update(msg.topic, msg.payload)
        if pwmeters_udpated or inverter_updated:
            self.process()

    def on_pre_connect(*args):
        """This seems to be needed due to a bug in paho-1-6-1"""
        log.debug('running on_pre_connect, apparently paho needs this now')

    # The callback for when the client receives a CONNACK response from the
    # server.
    def on_connect(self, client, userdata, flags, rc):
        assert client == self, "on_connect: unexpected client"
        print("Connected with result code "+str(rc))
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        for topic in self.inverter.topics:
            client.subscribe(topic)
        for topic in self.pwmeters.topics:
            client.subscribe(topic)

    def dump_stats(self, stats_f = sys.stdout):
        """Dump statistics to a file `solar.stats`."""
        inv_stats = self.inverter.stats_as_str()
        pwm_stats = self.pwmeters.stats_as_str()
        stats     = self._join_tables_side_by_side(inv_stats, pwm_stats)
        print(file = stats_f)
        print('consuming: %*s W (range [%s..%s])'
            % (5, self.pwmeters.total_power, self.lower_pw_bound,
                self.upper_pw_bound)
            , file = stats_f)
        print('feeding  : %*s W (target %s W)'
            % (5, self.inverter.power, self.inverter.power_limit)
            , file = stats_f)
        print(file = stats_f)
        print(self._lst_msg, file = stats_f)
        print(file = stats_f)
        print(stats, file = stats_f)

    @staticmethod
    def _join_tables_side_by_side(table1, table2):
        rows1 = table1.split('\n')
        rows2 = table2.split('\n')

        max_rows = max(len(rows1), len(rows2))
        result = ''
        for row1, row2 in zip(rows1, rows2):
            result += ('{:<30s}  {}\n'.format(row1, row2))

        # Print remaining rows if tables have different sizes
        if len(rows1) > len(rows2):
            for row in rows1[len(rows2):]:
                result += ('{:<30s}\n'.format(row))
        elif len(rows2) > len(rows1):
            for row in rows2[len(rows1):]:
                result += (' '*30 + '\n')
        return result


    @staticmethod
    def power_factor(target_voltage, current_voltage):
        v_delta = 1.15
        v_min = target_voltage - v_delta/2
        pf = 1/v_delta * current_voltage - v_min/v_delta
        log.info("Pf = 1/%s * %s - %s/%s = %s" % (v_delta, current_voltage,
            v_min, v_delta, pf))
        return min(max(pf,0), 1)

    def process(self):

        with open("solar.stats", "w") as stats_f:
            self.dump_stats(stats_f)

        if not self.inverter.is_reachable:
            self.log("(Inverter not reachable!)")
            return

        curr_date = datetime.date.today()
        if curr_date > self.prev_date:
            self.log("It's a new day (%s), resetting inverter's YieldDay.."
                    % curr_date)
            self.inverter.restart()
            self.prev_date = curr_date

        curr_month = datetime.datetime.now().month
        if 3 <= curr_month < 10:
            #self.summer_program()
            self.general_program(0.9,
                InverterOnBattery.SUMMER_LOW_VOLTAGE,
                InverterOnBattery.SUMMER_HGH_VOLTAGE
                )
        else:
            self.general_program(0.9,
                InverterOnBattery.WINTER_LOW_VOLTAGE,
                InverterOnBattery.WINTER_LOW_VOLTAGE
                )

    def general_program(self, sun_factor, low_voltage, high_voltage):
        """Feed `sun_factor` of consumption during sun hours,
        shutdown the inverter at `low_voltage` and startup at `high_voltage`.
        """


        # shortcuts to ease reading
        inv = self.inverter
        pwm = self.pwmeters

        curr_voltage_smoothed = inv.n_readings_sma_voltage(7)
        curr_voltage = inv.curr_voltage

        # Protective measures. We protect the battery by switching off the
        # inverter when the battery voltage gets too low. Also, we protect the
        # inverter itself from overheating, by monitoring its temperature.
        if inv.is_on:
            # ~ 18.5% capacity (likely more because discharging)
            if curr_voltage_smoothed < low_voltage:
                self.log("Battery low (%.2f V), about to power off.."
                    % curr_voltage_smoothed)
                inv.power_off()
                return
            elif inv.temperature > InverterOnBattery.HGH_TEMPERATURE:
                self.log("Temperature (%s°C) too high, about to power off.."
                    % inv.temperature)
                inv.power_off()
                return
            else:
                pass
                 # ~ 95% capacity (likely much less because charging)
        elif     curr_voltage_smoothed > high_voltage \
             and inv.temperature < InverterOnBattery.HGH_TEMPERATURE - 10:
                self.log("Battery full enough (%.2f V) and temperature ok"
                    " (%s°C) to power on.."
                    % (curr_voltage_smoothed, inv.temperature))
                inv.power_on()


        now = datetime.datetime.now().time()
        curr_month = datetime.datetime.now().month

        assert 12 >= curr_month > 0, "current month is %s" % curr_month
        sun_start = self.sunrise_sunset_times[curr_month]["sunrise"]
        sun_end   = self.sunrise_sunset_times[curr_month]["sunset"]
        sun_prct = sun_factor if sun_start <= now <= sun_end else 0.4


        if self.voltage_reading_num != inv.voltage_reading_num:
            # we've got new reading which we react to
            self.voltage_reading_num = inv.voltage_reading_num
            log.info("voltage reading: #%s" % self.voltage_reading_num)

            # stearing singal min, max
            ss_max = 6
            ss_min = -ss_max
            log.info("ss_max = %.2f" % ss_max)

            v_delta_max = 0.0999999
            v_delta_min = -v_delta_max

            sma_v_long  = inv.n_readings_sma_voltage(500)
            max_feed = max(pwm.baseload, pwm.total_power - 20.0)
            min_feed = 7.0
            #self.pid.setpoint = 25.8#sma_v_long
            self.pid.output_limits = (min_feed, max_feed)
            target_power = self.pid(inv.curr_voltage)
            self.log("setpoint: %.4f" % self.pid.setpoint)
            self.log("current_voltage: %s" % inv.curr_voltage)
            self.log("%s = pid(%s)" % (target_power, inv.curr_voltage))

            p, i, d = self.pid.components
            self.log("%.1f -> %.3f(%.3f, %3f, %3f)" %  (inv.curr_voltage, target_power, p, i, d))
            target_power = int(round(target_power))

            if target_power != inv.power_limit:
                self.log("Adjusting target power from %s W to %s W"
                    % (inv.power_limit, target_power))
                inv.power_limit = target_power

def main():
    arg_parser = argparse.ArgumentParser(
        description = '''
            Supervise and control an inverter running on battery for feeding
            current into my home power network.
            ''',
        )
    arg_parser.add_argument('--version', action='version',
            version='%(prog)s ' + VERSION,
            help='''show program's version number and exit''')
    arg_parser.add_argument('--verbose', '-v', action='store_true',
            help='''be a bit more verbose''')
    arg_parser.add_argument('--debug', action='store_true',
        help='''be very very verbose''')
    args = arg_parser.parse_args()

    logger = logging.getLogger()
    logger.addHandler(logging.NullHandler())

    if args.verbose or args.debug:
        # configure the root logger
        logger.setLevel(logging.INFO)
        sh = logging.StreamHandler(sys.stdout)
        trfh = logging.handlers.TimedRotatingFileHandler('logs/solars.log',
            when='D', backupCount = 5)
        logformatter_spec='%(asctime)s [%(levelname)s] %(message)s'
        sh.setFormatter(logging.Formatter(logformatter_spec))
        trfh.setFormatter(logging.Formatter(logformatter_spec))
        #logger.addHandler(sh)
        logger.addHandler(trfh)
        if args.debug:
            logger.setLevel(logging.DEBUG)

    try:
        inverter = InverterOnBattery('112183843243')
        pwmeters = PowerMeters(baseload = 27)
        operator = PowerOperator(inverter, pwmeters)

        operator.connect(BROKER, BROKER_PORT, 60)
        while True:
            # Blocking call that processes network traffic, dispatches
            # callbacks and handles reconnecting.
            operator.loop()

    except KeyboardInterrupt:
        print("Interrupted!")
        sys.exit(1)


if __name__ == '__main__':
    main()

# modeline for VIM - do not remove:
# vim: set ts=4 sw=4 tw=79 et :

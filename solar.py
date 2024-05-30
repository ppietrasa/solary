#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import time, copy, json, argparse, logging, sys, math, collections

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

    def expired(self):
        """Returns True if timer expired.

        Note
        ----
        Because the implementation is based on system clock the Timer may
        occasionally prematurely expire, for instance, if system clock has been
        set back.
        """
        return self.elapsed() >= self.seconds

    def restart(self, seconds = None):
        """Restarts the timer."""
        self.start = time.time()
        if seconds:
            self.seconds = seconds

    def remaining(self):
        """Returns time remaining."""
        return max(0, self.seconds - self.elapsed())

    def elapsed(self):
        """Returns time elapsed since last restart."""
        elapsed =  time.time() - self.start
        if elapsed < 0:
            # time has been set back -> we better expire
            elapsed = self.seconds
        return elapsed


class InverterOnBattery(object):
    """
    Inverter connected to a battery.
    """
    TOPIC_SUFFIXES=[
        # R Name of the inverter as configured in web GUI
        #'/name',
        # R Indicates whether the inverter is reachable - 0 or 1
        '/status/reachable',
        # R Indicates whether the inverter is producing AC power - 0 or 1
        '/status/producing',
        # R Unix timestamp of last inverter statistics udpate seconds since JAN 01 1970 (UTC)
        '/status/last_update',

        # R AC active power in watts - Watt (W)
        '/0/power',
        # R DC power in watts - Watt (W)
        #'/0/powerdc',
        # R Power factor in percent - %
        #'/0/powerfactor', # R Temperature of inverter in degree celsius - Degree Celsius (°C)
        '/0/temperature',
        # R Energy converted to AC per day in watt hours - Watt hours (Wh)
        #'/0/yieldday',

        # R DC current of specific input in ampere - Ampere (A)
        #'/1/current',
        # R DC power of specific input in watt - Watt (W)
        '/1/power',
        # R DC voltage of specific input in volt - Volt (V)
        '/1/voltage',

        # R Current applied production limit of the inverter - Watt (W)
        '/status/limit_absolute',

#solar/112183843243/1/voltage b'26.3'
#solar/112183843243/1/power b'16.9'
#solar/112183843243/status/limit_absolute b'18.90'

        ]

    LOW_VOLTAGE = 24
    HGH_VOLTAGE = 25.6
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
        self._voltage_queue = collections.deque([25.6, 25.6], 2)
        self._setting_pwlimit = False
        self._requested_pwlimit = 0
        self._pwlimit_timer = Timer(1)
        self._voltage_change_timer = Timer(1)
        self._powering_off = False
        self._powering_on = False


    @property
    def max_power(self):
        '''Maimum power the inverter can safly output.'''
        # The inverter's input should not exceed 11A and the minimal battery
        # voltage is 22V. It is at the minmal voltage that the inverter would
        # try to pull max current.
        return 11 * 22

    @property
    def topics(self):
        return [ self.t_prefix + t_suffix for t_suffix in InverterOnBattery.TOPIC_SUFFIXES ]


    def update(self, topic, payload):
        if topic in self.topics:
            #print("UPDATED INVERTER TOPIC: %s" % topic)
            #self.prev_state = copy.copy(self.curr_state)
            self.curr_state[topic] = payload

        if '/1/voltage' in topic:
            new_voltage = float(payload)
            if not math.isclose(new_voltage, self.curr_voltage, abs_tol=0.03):
                self._voltage_change_timer.restart()
                self._voltage_queue.append(new_voltage)

    @property
    def is_reachable(self):
        return self.curr_state.get(self.t_prefix + '/status/reachable', False) == b'1'

    @property
    def is_producing(self):
        return self.curr_state.get(self.t_prefix + '/status/producing', False) == b'1'

    @property
    def temperature(self):
        """Temperature of the inverter in °C."""
        return float(self.curr_state.get(self.t_prefix + '/0/temperature', 23))

    @property
    def prev_voltage(self):
        """Current voltage of the battery."""
        return float(self._voltage_queue[0])

    @property
    def curr_voltage(self):
        """Current voltage of the battery."""
        return float(self._voltage_queue[1])

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
                capacity1, capacity2 = capacity_map[voltage1], capacity_map[voltage2]

                if voltage1 <= voltage <= voltage2:
                    return InverterOnBattery.interpolate_linear(voltage, voltage1, capacity1, voltage2, capacity2)
        assert False, "Unreachable code!"

    @property
    def curr_capacity(self):
        """Current *estimated* power in Watt."""
        return InverterOnBattery.voltage_to_capacity(self.curr_voltage, self.capacity_map)

    @property
    def prev_capacity(self):
        """Previous *estimated* power in Watt."""
        return InverterOnBattery.voltage_to_capacity(self.prev_voltage, self.capacity_map)

    @property
    def battery_cycling_power(self):
        """The *estimated* charge (positv) or discharge (negative) rate of
        battery in Watt."""
        # 0.5: a guess if we are below 1s
        time_delta = self._voltage_change_timer.elapsed() / 3600
        log.debug("(time_delta = %s)" % time_delta)
        log.debug("(curr_capacity = %s)" % self.curr_capacity)
        log.debug("(prev_capacity = %s)" % self.prev_capacity)
        est_cycling_power = (self.curr_capacity - self.prev_capacity) / time_delta
        log.debug("(cycling_power = %s" % est_cycling_power)
        return est_cycling_power

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

    def power_on(self):
        assert self.publisher, "Publisher not set!"
        self.publisher.publish(self.t_prefix + '/cmd/power', 1)
        if self._powering_on:
            if self.is_on:
                log.debug("previous power_on confirmed")
                self._powering_on = False
            else:
                log.debug("wating for power_on ..")
        else:
            log.info("Power on!")
            self.publisher.publish(self.t_prefix + '/cmd/power', 0)
            self._powering_on = True

    def power_off(self):
        assert self.publisher, "Publisher not set!"
        if self._powering_off:
            if self.is_off:
                log.debug("previous power_off confirmed")
                self._powering_off = False
            else:
                log.debug("wating for power_off ..")
        else:
            log.info("Power off!")
            self.publisher.publish(self.t_prefix + '/cmd/power', 0)
            self._powering_off = True

    @property
    def power_limit(self):
        pwlimit = math.ceil(float(self.curr_state.get(self.t_prefix +
            '/status/limit_absolute', 300)))
        log.debug('(power_limit is %s W)' % pwlimit)
        return pwlimit

    def _set_power_limit(self, limit):
        assert self.publisher, "Publisher not set!"
        self.publisher.publish(self.t_prefix + '/cmd/limit_nonpersistent_absolute', str(limit))

    @power_limit.setter
    def power_limit(self, limit_new):
        log.debug('(power_limit = %s W)' % limit_new)
        if self._setting_pwlimit:
            limit_now = self.power_limit
            if limit_now == self._requested_pwlimit:
                # previous set_power_limit confirmed
                log.debug("previousloy set power_limit confirmed (%s W)" %
                        limit_now)
                log.debug("setting new power_limit to %s W" % limit_new)
                self._set_power_limit(limit_new)
                self._requested_pwlimit = limit_new
                self._pwlimit_timer.restart()
            else:
                log.debug("waiting for %s W power_limit, %ss.." %
                    (self._requested_pwlimit, round(self._pwlimit_timer.elapsed())))
        else:
            log.debug("requesting %s W power_limit" % limit_new)
            self._set_power_limit(limit_new)
            self._setting_pwlimit = True
            self._requested_pwlimit = limit_new
            self._pwlimit_timer.restart()



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
                capacity1, capacity2 = capacity_map[voltage1], capacity_map[voltage2]

                if voltage1 <= voltage <= voltage2:
                    return InverterOnBattery.interpolate_linear(voltage, voltage1, capacity1, voltage2, capacity2)
        assert False, "Unreachable code!"


class PowerMeters(object):
    TOPICS = [
        'tele/office/SENSOR',
        'tele/tv/SENSOR',
        #'tele/fridge/SENSOR'
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
        self.prev_state = dict()
        self.curr_state = dict()
        self.baseload = baseload

    def update(self, topic, payload):
        if topic in self.topics:
            self.prev_state = copy.copy(self.curr_state)
            self.curr_state[topic] = payload

    @property
    def total_power(self):
        """Total power measured by all the power meters"""
        total_power=self.baseload
        for t in self.topics:
            payload_json = self.curr_state.get(t, None)
            if payload_json:
                payload_dict = json.loads(payload_json)
                pwmeter_power = payload_dict.get("ENERGY", {}).get("Power")
                pwmeter_name = t.split('/')[-2]
                #log.debug("power %s: %s W" % (pwmeter_name, pwmeter_power))
                total_power += pwmeter_power
        return total_power

class MqttClient(mqtt.Client):

    def __init__(self, inverter, pwmeters):
        super(MqttClient, self).__init__("solar@%s" % BROKER)
        self.inverter = inverter
        self.pwmeters = pwmeters
        # we are a friend of inverter, which sends out commands on behalf of inverter
        self.inverter.publisher = self
        self.lower_pw_bound = pwmeters.total_power - 15
        self.upper_pw_bound = pwmeters.total_power + 15


    def on_message(self, client, userdata, msg):
        assert client == self, "on_connect: unexpected client"
        log.debug(msg.topic+" "+str(msg.payload))
        self.pwmeters.update(msg.topic, msg.payload)
        self.inverter.update(msg.topic, msg.payload)
        self.process()

    def on_pre_connect(*args):
        """This seems to be needed due to a bug in paho-1-6-1"""
        log.debug('running on_pre_connect, apparently paho needs this now')

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc):
        assert client == self, "on_connect: unexpected client"
        print("Connected with result code "+str(rc))
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        for topic in self.inverter.topics:
            client.subscribe(topic)
        for topic in self.pwmeters.topics:
            client.subscribe(topic)

    def dump_stats(self):
        """Dump statistics to a file `solar.stats`."""
        # shortcuts to ease reading
        inverter = self.inverter
        pwmeters = self.pwmeters
        with open("solar.stats", "w") as stats_f:
            print("Status:      %s" 
                % ("ON" if inverter.is_on else "OFF"),
                file = stats_f)
            print("Reachable:   %s" 
                % ("YES" if inverter.is_reachable else "NO"),
                file = stats_f)
            if (    pwmeters.total_power < self.lower_pw_bound
                 or pwmeters.total_power > self.upper_pw_bound
               ):
                in_range_str = "outside of range"
            else:
                in_range_str = "within range"
            print("Consumption: %s W (%s [%s..%s])" % (
                    pwmeters.total_power,
                    in_range_str,
                    self.lower_pw_bound,
                    self.upper_pw_bound
                    ), file = stats_f)
            in_limit_str = ''
            if (     inverter.power >= inverter.power_limit - 1
                and  inverter.power <= inverter.power_limit + 1
               ):
                in_limit_str = "close to"
            elif inverter.power > inverter.power_limit:
                in_limit_str = "above"
            else:
                in_limit_str = "below"
            print("Feeding:     %s W (%s limit of %s W)" % (
                    inverter.power, in_limit_str, inverter.power_limit
                    ), file = stats_f)
            print("Temperature: %s °C" % inverter.temperature, file = stats_f)
            print("Voltage:     %s V" % inverter.curr_voltage, file = stats_f)
            print("Estimated bat. capacity: %.1f Wh" 
                % inverter.curr_capacity, file = stats_f)
            print("Estimated cycling power: %.1f W"
                % inverter.battery_cycling_power, file = stats_f)


    def process(self):
        # shortcuts to ease reading
        inv = self.inverter
        pwm = self.pwmeters
        self.dump_stats()
        if not inv.is_reachable:
            log.info("(inv not reachable!)")
            return
        if (    pwm.total_power < self.lower_pw_bound
             or pwm.total_power > self.upper_pw_bound
           ):
            log.info("Consumption (%s) outside of range: [%s..%s]" %
                (pwm.total_power, self.lower_pw_bound, self.upper_pw_bound)
                )
            new_lower_pw_bound = pwm.total_power - 15
            new_upper_pw_bound = pwm.total_power + 15
            log.info("New range: [%s, %s]" % (new_lower_pw_bound, new_upper_pw_bound))
            self.lower_pw_bound = new_lower_pw_bound
            self.upper_pw_bound = new_upper_pw_bound
            log.info("Feeding: %s W (limit %s)"
                    % (inv.power, inv.power_limit))
            feed_power = math.floor(min(max(pwm.baseload,
                new_lower_pw_bound), inv.max_power))
            inv.power_limit = feed_power
        else:
            log.info("Consumption (%s) within range: [%s..%s]" %
                    (pwm.total_power, self.lower_pw_bound, self.upper_pw_bound))
            log.info("Feeding: %s W (limit %s)"
                % (inv.power, inv.power_limit))
            if inv.is_on:
                if inv.curr_voltage < InverterOnBattery.LOW_VOLTAGE:
                    log.info("Battery low (%s) , about to power off.." 
                        % inv.curr_voltage)
                    inv.power_off()
                elif inv.temperature > InverterOnBattery.HGH_TEMPERATURE:
                    log.info("Temperature (%s) too high, about to power off.."
                        % inv.temperature)
                    inv.power_off()
                else:
                    pass
            elif     inv.curr_voltage >= InverterOnBattery.HGH_VOLTAGE \
                 and inv.temperature   < InverterOnBattery.HGH_TEMPERATURE - 10:
                    log.info("Battery full enough (%s) and temperature ok (%s)"
                        " to power up.." % (inv.curr_voltage, inv.temperature))
                    inv.power_on()

inverter = InverterOnBattery('112183843243')
pwmeters = PowerMeters(baseload = 27)
client   = MqttClient(inverter, pwmeters)

def main():
    arg_parser = argparse.ArgumentParser(
        description = '''
            Supervise and conrol an inverter running on battery for feeding
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
        logformatter_spec='%(message)s'
        if args.debug:
            logformatter_spec = '[%(levelname)s] ' + logformatter_spec
        sh.setFormatter(logging.Formatter(logformatter_spec))
        logger.addHandler(sh)
        if args.debug:
            logger.setLevel(logging.DEBUG)

    try:
        client.connect(BROKER, BROKER_PORT, 60)
        while True:
            # Blocking call that processes network traffic, dispatches
            # callbacks and handles reconnecting.
            client.loop()

    except KeyboardInterrupt:
        print("Interrupted!")
        sys.exit(1)


if __name__ == '__main__':
    main()

# modeline for VIM - do not remove:
# vim: set ts=4 sw=4 tw=79 et :


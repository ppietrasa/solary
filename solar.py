#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import time, copy, json, argparse, logging, sys

VERSION='0.0.1'
BROKER='tuxedo.fritz.box'
BROKER_PORT=1883

log  = logging.getLogger(__name__)

def arg_parser():
    # This has to be a module level function for Sphinx to extract the usage
    # text
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
    return arg_parser

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
        '/name',
        # R Indicates whether the inverter is reachable - 0 or 1
        '/status/reachable',
        # R Indicates whether the inverter is producing AC power - 0 or 1
        '/status/producing',
        # R Unix timestamp of last inverter statistics udpate seconds since JAN 01 1970 (UTC)
        '/status/last_update',

        # R AC active power in watts - Watt (W)
        '/0/power',
        # R DC power in watts - Watt (W)
        '/0/powerdc',
        # R Power factor in percent - %
        '/0/powerfactor',
        # R Temperature of inverter in degree celsius - Degree Celsius (°C)
        '/0/temperature',
        # R Energy converted to AC per day in watt hours - Watt hours (Wh)
        '/0/yieldday',

        # R DC current of specific input in ampere - Ampere (A)
        '/1/current',
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


    def __init__(self, serial, baseload, bat_capacity=1280, base_topic='solar/'):
        """
        Parameters
        ----------
        serial : str
           Serial number of the inverter
        baseload: int
           The consumption base load in Watt the inverter should try to maintain/keep.
        bat_capacity: int
           The battery capacity in Wh.
        """
        self.serial = serial
        self.baseload = baseload
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

    @property
    def topics(self):
        return [ self.t_prefix + t_suffix for t_suffix in InverterOnBattery.TOPIC_SUFFIXES ]


    def update(self, topic, payload):
        if topic in self.topics:
            #print("UPDATED INVERTER TOPIC: %s" % topic)
            self.prev_state = copy.copy(self.curr_state)
            self.curr_state[topic] = payload

    @property
    def is_reachable(self):
        return self.curr_state.get(self.t_prefix + '/status/reachable', False) == b'1'

    @property
    def is_producing(self):
        return self.curr_state.get(self.t_prefix + '/status/producing', False) == b'1'

    @property
    def curr_voltage(self):
        """Current voltage of the battery."""
        return float(self.curr_state.get(self.t_prefix + '/1/voltage', 26))

    @property
    def prev_voltage(self):
        """Previous voltage of the battery."""
        return float(self.curr_state.get(self.t_prefix + '/1/voltage', 26))


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
        return voltage_to_capacity(self.prev_power, self.capacity_map)

    def battery_cycling_power(self):
        """The *estimated* charge (positv) or discharge (negative) rate of
        battery in Watt."""
        return (self.curr_capacity - self.prev_capacity) / self.time_delta

    def power_on(self, client):
        client.publish(self.t_prefix + '/cmd/power', 1)

    def power_off(self, client):
        client.publish(self.t_prefix + '/cmd/power', 0)

    @property
    def power_limit(self):
        return float(self.curr_state.get(self.t_prefix + '/status/limit_absolute', 300))

    def set_power_limit(self, client, limit):
        client.publish(self.t_prefix + '/cmd/limit_nonpersistent_absolute', str(limit))

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

    def __init__(self):
        self.topics = PowerMeters.TOPICS
        self.prev_state = dict()
        self.curr_state = dict()

    def update(self, topic, payload):
        if topic in self.topics:
            self.prev_state = copy.copy(self.curr_state)
            self.curr_state[topic] = payload

    @property
    def total_power(self):
        """Total power measured by all the power meters"""
        total_power=0
        for t in self.topics:
            payload_json = self.curr_state.get(t, None)
            if payload_json:
                payload_dict = json.loads(payload_json)
                pwmeter_power = payload_dict.get("ENERGY", {}).get("Power")
                pwmeter_name = t.split('/')[-2]
                log.debug("power %s: %s W" % (pwmeter_name, pwmeter_power))
                total_power += pwmeter_power
        return total_power

inverter = InverterOnBattery('112183843243', 29)
pwmeters = PowerMeters()
client   = mqtt.Client()


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    for topic in inverter.topics:
        client.subscribe(topic)
    for topic in pwmeters.topics:
        client.subscribe(topic)


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    log.debug(msg.topic+" "+str(msg.payload))
    pwmeters.update(msg.topic, msg.payload)
    inverter.update(msg.topic, msg.payload)

def process(client, inverter, pwmeters):
    if inverter.is_reachable:
        #log.info("(inverter.power_limit = %s)" % inverter.power_limit)
        #log.info("(pwmeters.total_power = %s)" % pwmeters.total_power)
        log.info("Currently feeding: %s W" % inverter.power)
        log.info("Currently using: %s W" % pwmeters.total_power)
        proposed_power_limit = min(220, pwmeters.total_power)
        current_power_limit  = inverter.power_limit
        limit_delta = abs(proposed_power_limit - current_power_limit)
        if (limit_delta > 17): 
            if (inverter.power_limit < proposed_power_limit):
                log.info("Bumping power to %s W" %  proposed_power_limit)
            else:
                log.info("Reducing power to %s W" %  proposed_power_limit)
            inverter.set_power_limit(client, proposed_power_limit)
            # Wait for the change to take effect (inverter confirms). The inverter
            # and/or DTU need seconds to process this command. Flooding the
            # DTU/inverter renders those non-responsive for a while.
            while True:
                log.info("waiting for new inverter limit..")
                limit_delta = abs(proposed_power_limit - inverter.power_limit)
                if limit_delta < 3:
                    log.info("ok. new inverter limit set (%s)" % inverter.power_limit)
                    break
                time.sleep(3)
                client.loop()
    else:
        log.info("(inverter not reachable!)")


def main():
    args = arg_parser().parse_args()

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
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(BROKER, BROKER_PORT, 60)
        t = Timer(0)
        while True:
            # Blocking call that processes network traffic, dispatches callbacks and
            # handles reconnecting.
            client.loop()
           
            # Process everything in 15s intervals. This is not to flood the
            # consol/log too much and also to give DTU/inverter a break. It
            # stops responding for a while if we flood it with commands too
            # much.
            if t.expired():
                process(client, inverter, pwmeters)
                t.restart(3)

    except KeyboardInterrupt:
        print("Interrupted!")
        sys.exit(1)


if __name__ == '__main__':
    main()

# modeline for VIM - do not remove:
# vim: set ts=4 sw=4 tw=79 et :


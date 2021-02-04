const ROUTE_DEFAULT_COLOR = 'darkred';
const UNKNOWN_COLOR = 'red';

// route types are here:
// https://sites.google.com/site/gtfschanges/proposals/route-type
const ROUTE_COLOR_MAPPING = {
  // tram/rail, subway/metro, rail
  '0': '#FF0000',
  '1': '#FF0000',
  '2': '#FF0000',
  // bus
  '3': 'FireBrick',
  // ferry
  '4': 'DeepSkyBlue',
}

const TrainMarkerIcon = L.divIcon({
  html: `<i class="fa train fa-2x"></i>`,
  iconSize: [20, 20],
  className: 'train-marker-icon'
});

const BusMarkerIcon = L.divIcon({
  html: `<i class="fa fa-bus fa-2x"></i>`,
  iconSize: [20, 20],
  className: 'bus-marker-icon'
});

const BoatMarkerIcon = L.divIcon({
  html: `<i class="fa fa-ship fa-2x"></i>`,
  iconSize: [20, 20],
  className: 'boat-marker-icon'
});

const ROUTE_ICON_MAPPING = {
  // tram/rail, subway/metro, rail
  '0': TrainMarkerIcon,
  '1': TrainMarkerIcon,
  '2': TrainMarkerIcon,
  // bus
  '3': BusMarkerIcon,
  // ferry
  '4': BoatMarkerIcon,
}

const randomColor = () => "#" + ((1<<24)*Math.random() | 0).toString(16)
const currentTime = () => new Date();

class Route {
  static stopToLatLong({ latitude, longitude }) {
    return [latitude, longitude];
  }

  constructor(type, map, schedule) {
    this.type = type;
    this.polyline = L.polyline(schedule.map(Route.stopToLatLong), {
      color: ROUTE_DEFAULT_COLOR,
    });
    this.polyline.addTo(map);
    this.stops = schedule.map(({ stopName, latitude, longitude }) => {
      const circle = L.circleMarker([latitude, longitude], {
        color: ROUTE_DEFAULT_COLOR,
        radius: 2,
        fill: true,
        fillOpacity: 0.8,
      });
      circle.bindTooltip('Stop: ' + stopName);
      circle.addTo(map);
      return circle;
    });
  }

  setColor() {
    const color = ROUTE_COLOR_MAPPING[this.type] || UNKNOWN_COLOR
    this.polyline.setStyle({ color });
    // this.stops.forEach((stop) => stop.setStyle({ color }));
  }

  remove() {
    this.polyline.remove();
    this.stops.forEach((stop) => stop.remove());
  }
}

class Vehicle {
  constructor(map, routeId, routeType, schedule, position, name, onFinalStopCb) {
    this.routeId = routeId;
    this.routeType = routeType;
    this.schedule = schedule;
    this.name = name;

    this._map = map;
    this._onFinalStopCb = onFinalStopCb;
    this._route = new Route(this.routeType, this._map, this.schedule);
    this._marker = undefined;
    this._heartbeatIntervalId = undefined;
    this._lastKnownPosition = position;

    this._createHeartbeat();
  }

  updateData({position, schedule}) {
    this.schedule = schedule;
    this._lastKnownPosition = position
    this._refresh({immediatePosition: position});
  }

  _createHeartbeat() {
    this._heartbeatIntervalId = setInterval(() => this._refresh(), 1000);
    this._refresh();
  }

  _refresh({immediatePosition}={}) {
    if (this._hasMovementEnded) {
      // console.log(555, 'movement has ended, not refreshing, removing.')
      this._onFinalStop();
      return;
    }

    if (!this._hasMovementStarted) {
      // console.log(555, 'movement not started, not refreshing.')
      return;
    }

    // console.log(555, 'should see movement...', this.routeId)

    if (!this._marker) {
      this._createNewVehicle();
      this._route.setColor();
      return;
    }

    if (immediatePosition) {
      console.log(`immediate position for ${this.routeId}: ${immediatePosition}`)
      this._marker.setLatLng([immediatePosition.latitude, immediatePosition.longitude]);
    } else {
      // hits while waiting for new data. this makes things move...
      this._marker.setLatLng(this._currentLatLong);
    }
  }

  _createNewVehicle() {
    const DEBUG_ICON = L.divIcon({
      html: `<i class="fa fa-bus fa-2x" style="color: ${randomColor()}"></i>`,
      iconSize: [20, 20],
      className: 'boat-marker-icon'
    });

    const icon = DEBUG_ICON

    // xxx
    // const icon = ROUTE_ICON_MAPPING[this.routeType]
    this._marker = L.marker(this._currentLatLong, {icon});
    this._marker.bindTooltip(this.name);
    this._marker.addTo(this._map);
  }

  _onFinalStop() {
    if (this._marker) {
      this._marker.remove();
      this._marker = undefined;
    }

    if (this._route) {
      this._route.remove();
      this._route = undefined;
    }

    clearInterval(this._heartbeatIntervalId);

    this._onFinalStopCb(this);
  }

  get _currentLatLong() {
    if (!this._hasMovementStarted) {
      return Route.stopToLatLong(this.schedule[0]);
    }

    const t = currentTime();

    const nextStopI = this.schedule.findIndex(({ arrival }) => t < arrival);

    if (nextStopI === -1) {
      // Vehicle has arrived at the final stop
      return Route.stopToLatLong(this.schedule[this.schedule.length - 1]);
    }

    const nextStop = this.schedule[nextStopI];
    const prevStop = this.schedule[nextStopI - 1];

    // xxx this is a noop from day 1. "top"???
    // if (top < prevStop.departure) {
    //   // Vehicle hasn't departed yet
    //   return Route.stopToLatLong(prevStop);
    // }

    const distancePassed =
      (t - prevStop.departure) / (nextStop.arrival - prevStop.departure);

    // xxx
    //let { latitude: prevLat, longitude: prevLong } = prevStop;
    //if (this._lastKnownPosition) {
      //console.log('basing from ', this._lastKnownPosition)
      let prevLat = this._lastKnownPosition.latitude
      let prevLong = this._lastKnownPosition.longitude
    //}
    const { latitude: nextLat, longitude: nextLong } = nextStop;

    const currentLat = prevLat + (nextLat - prevLat) * distancePassed;
    const currentLong = prevLong + (nextLong - prevLong) * distancePassed;

    return [currentLat, currentLong];
  }

  get _hasMovementEnded() {
    const routeEndTime = this.schedule[this.schedule.length - 1].arrival;
    return currentTime() > routeEndTime;
  }

  get _hasMovementStarted() {
    const routeStartTime = this.schedule[0].arrival;
    return currentTime() > routeStartTime;
  }
}

class Container {
  constructor() {
    this.map = L.map('map').setView([37.6688, -122.0810], 10);
    this._vehicles = {};
    this._socket = undefined;
    this._stomp = undefined;
  }

  initialize() {
    L.tileLayer(
      'https://tile.thunderforest.com/transport/{z}/{x}/{y}{r}.png?apikey=170be1cff4224274add97bf552fd4745',
      {
        attribution:
          '&copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors,' +
          '<a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>',
      },
    ).addTo(this.map);

    this._socket = new SockJS('/hazelcast');
    this._stomp = Stomp.over(this._socket);
    this._stomp.reconnect_delay = 2000;
    this._stomp.debug = null; // turns off logging
    this._stomp.connect({}, () => {
      console.log('Connected to stomp server.')
      this._stomp.subscribe('/topic/updates', (update) => {
        const data = JSON.parse(update.body);

        //
        // transform the data a bit:
        //
        if (!data.schedule) return // lax it a bit; does hit occasionally
        data.routeId = data.vehicle.trip.route.id
        data.routeName = data.vehicle.trip.route.route_name
        data.routeType = data.vehicle.trip.route.route_type
        data.agencyName = data.agencyId
        data.position = data.vehicle.position

        data.schedule = data.schedule.map((schobj) => {
          return {
            departure: new Date(schobj.departure * 1000),
            arrival: new Date(schobj.arrival * 1000),
            longitude: parseFloat(schobj.stop.stop_long),
            latitude: parseFloat(schobj.stop.stop_lat),
            stopName: schobj.stop.stop_name,
            stopid: schobj.stop.stop_id
          }
        });

        // if (data.routeType !== "3") alert(data.routeType)

        this._processData(data);
      });
    });

    $.ajax('/data/');
  }

  _processData({
    routeId,
    position,
    schedule,
    routeName,
    routeType,
    agencyName,
  }) {
    // console.log('processing ', routeId, {now: currentTime(), lastStopArrival: schedule[schedule.length - 1].arrival})
    /// console.log('processing ', routeId, currentTime(), schedule.map(({arrival}) => arrival))
    if (currentTime() > schedule[schedule.length - 1].arrival) {
      return;
    }

    const existingVehicle = this._vehicles[routeId];

    if (!existingVehicle) {
      const newVehicle = new Vehicle(
        this.map,
        routeId,
        routeType,
        schedule,
        position,
        `${routeName} (${agencyName}) debug: ${routeType}`,
        (vehicle) => this._onVehicleFinalStop(vehicle),
      );
      this._vehicles[routeId] = newVehicle;
      return;
    }

    existingVehicle.updateData({position, schedule});
  }

  _onVehicleFinalStop(vehicle) {
    delete this._vehicles[vehicle.routeId];
  }
}

new Container().initialize();

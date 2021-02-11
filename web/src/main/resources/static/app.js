const DEBUG = true
const DEBUG_REPLAY = false

const UNKNOWN_COLOR = 'red';

// route types are here:
// https://sites.google.com/site/gtfschanges/proposals/route-type
const ROUTE_COLOR_MAPPING = {
  // tram/rail, subway/metro, rail
  '0': '#FF0000',
  '1': '#FF0000',
  '2': '#FF0000',
  // bus
  '3': 'RoyalBlue',
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
// const currentTime = () => new Date();

function currentTime() {
  if (DEBUG && DEBUG_REPLAY) {
    var dateOffset = ((149)*60*60*1000);
    var fakeNow = new Date();
    fakeNow.setTime(fakeNow.getTime() - dateOffset);
    return fakeNow
  }
  return new Date();
}

let KNOWN_ROUTES = {}

class Route {
  static stopToLatLong({ latitude, longitude }) {
    return [latitude, longitude];
  }

  constructor(type, map, schedule) {
    this.type = type;
    this.polyline = L.polyline(schedule.map(Route.stopToLatLong), {
      color: ROUTE_COLOR_MAPPING[this.type] || UNKNOWN_COLOR,
      weight: 2,
    });
    this.polyline.addTo(map);
    this.latlngs = this.polyline.getLatLngs()
    this.stops = schedule.map(({ stopName, latitude, longitude }, idx) => {
      const isLastStop = idx === schedule.length - 1
      const circle = L.circleMarker([latitude, longitude], {
        color: ROUTE_COLOR_MAPPING[this.type] || UNKNOWN_COLOR,
        radius: isLastStop ? 5 : 3, // make last stops larger
        fillColor: isLastStop ? 'red' : 'lime',
        fill: true,
        fillOpacity: 0.8,
      });
      circle.bindTooltip('Stop: ' + stopName);
      circle.addTo(map);
      return circle;
    });
  }

  setColor() {
    // xxx why call this at all
    const color = ROUTE_COLOR_MAPPING[this.type] || UNKNOWN_COLOR
    this.polyline.setStyle({ color });
    this.stops.forEach((stop) => stop.setStyle({ color }));
  }

  remove() {
    this.polyline.remove();
    this.stops.forEach((stop) => stop.remove());
  }
}

class Vehicle {
  constructor(map, vehicleId, routeId, routeType, schedule, position, name, onFinalStopCb) {
    this.vehicleId = vehicleId;
    this.routeId = routeId;
    this.routeType = routeType;
    this.schedule = schedule;
    this.name = name;

    this._map = map;
    this._onFinalStopCb = onFinalStopCb;

    // use existing route if possible
    this._route = KNOWN_ROUTES[routeId]
    if (!this._route) {
      this._route = new Route(this.routeType, this._map, this.schedule);
      KNOWN_ROUTES[routeId] = this._route
    }
    this._marker = undefined;

    this._lastKnownPosition = position;
  }

  updateData({position, schedule}) {
    this.schedule = schedule;
    this._lastKnownPosition = position;

    if (this._hasMovementEnded) {
      DEBUG && console.log(555, 'xxx movement has ended, removing.', this.vehicleId)
      this._onFinalStop();
      return;
    }

    // xxx why the time diff?
    if (this._lastKnownPosition.speed = 0) return
    // if (!this._hasMovementStarted) {
    //   DEBUG && console.log(555, 'xxx movement not started, not doing anything.', this.vehicleId)
    //   DEBUG && console.log(555, currentTime(), this.schedule[0].arrival, this._lastKnownPosition.speed)
    //   return;
    // }

    if (!this._marker) {
      this._createMarker();
      this._route.setColor();
    }

    // this._debugMarker(this._lastKnownLatLong, `${this.vehicleId} - ${new Date()}`)

    // this._route.latlngs.forEach(latlong => {
    //   console.log(999, latlong.distanceTo(this._marker.getLatLng()))
    // })

    // const distancesToMarker = this._route.latlngs.map(latlong => latlong.distanceTo(this._marker.getLatLng()))
    // const closestDistance = Math.min(...distancesToMarker)
    // const idx = distancesToMarker.findIndex(distance => distance === closestDistance)

    // console.log(999999, `closest stop: (${idx}/${this._route.latlngs.length})`)
    // const nextStopIdx = this.schedule.findIndex(({ latitude, longitude }) => t < arrival);
  }

  _createMarker() {
    const DEBUG_ICON = L.divIcon({
      html: `<i class="fa fa-bus fa-2x" style="color: ${randomColor()}"></i>`,
      iconSize: [20, 20],
      className: 'boat-marker-icon'
    });

    const icon = DEBUG_ICON

    // const icon = ROUTE_ICON_MAPPING[this.routeType]
    this._marker = L.marker([this._lastKnownPosition.latitude, this._lastKnownPosition.longitude], {icon});
    this._marker.bindTooltip(this.name);
    this._marker.addTo(this._map);
  }

  _debugMarker(latlng, s) {
    if (!this.__order) this.__order = 1
    let m = L.marker(latlng);
    m.bindTooltip(s + "order:" + this.__order++);
    m.addTo(this._map);
  }

  _onFinalStop() {
    DEBUG && console.log('xxx _onFinalStop')
    if (this._marker) {
      this._marker.remove();
      this._marker = undefined;
    }

    if (this._route) {
      this._route.remove();
      this._route = undefined;
    }

    this._onFinalStopCb(this);
  }

  get _hasMovementEnded() {
    const routeEndTime = this.schedule[this.schedule.length - 1].arrival;
    return currentTime() > routeEndTime;
  }

  get _hasMovementStarted() {
    const routeStartTime = this.schedule[0].arrival;
    return currentTime() > routeStartTime;
  }

  get _lastKnownLatLong() {
    return [this._lastKnownPosition.latitude, this._lastKnownPosition.longitude]
  }

  _tick() {
    if (!this._marker) return
    if (!this._lastKnownPosition) return // no need process unless new data comes in.

    // move to the last known position with the given speed
    // animation time is an approximation
    // trick - use css transitions for a smoother animation:
    const marker = this._marker
    const speed = this._lastKnownPosition.speed || 10 // meters/sec
    // console.log(this._lastKnownPosition)
    if (!speed) return;

    const animSpeed = Math.floor(30000 / speed) // animates good enough.
    if (marker._icon) { marker._icon.style[L.DomUtil.TRANSITION] = ('all ' + animSpeed + 'ms linear'); }
    if (marker._shadow) { marker._shadow.style[L.DomUtil.TRANSITION] = 'all ' + animSpeed + 'ms linear'; }

    this._marker.setLatLng(this._lastKnownLatLong);
    this._lastKnownPosition = null;
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

    this.listen()

    this.animationLoop()

    DEBUG && DEBUG_REPLAY && this.replayFakeData()
  }

  replayFakeData() {
    let allowed = [
        'AC:72R',
        'AC:72M',
        'AC:97'
    ]

    let idx = 0;
    setInterval(() => {

      for (let i=0; i < 10; i++) {
        if (REPLAYS.length - 1 === idx) return
        const data = REPLAYS[idx]
        data.vehicleId = data.vehicle.vehicle.id
        data.schedule.forEach((schobj) => {
          schobj.departure = new Date(schobj.departure)
          schobj.arrival = new Date(schobj.arrival)
        });
        idx++

        // if (!allowed.includes(data.routeId)) continue;
        console.log('processing:', data.routeId, data.vehicleId)

        this._processData(data);
      }
    }, 1000)
  }


  listen() {
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
        if (!(data.schedule && data.schedule.length)) return // lax it a bit; does hit occasionally

        data.vehicleId = data.vehicle.vehicle.id
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

        this._processData(data);
      });
    });

    $.ajax('/data/');
  }

  animationLoop() {
    const self = this;

    function worldTick(timestamp) {
      // tick for each vehicle
      //console.time('worldTick')
      Object.values(self._vehicles).forEach(v => v._tick())
      //console.timeEnd('worldTick')
    }

    // we could use a timer as well. overkill.
    window.setInterval(worldTick, 1000);
  }

  _processData({
    vehicleId,
    routeId,
    position,
    schedule,
    routeName,
    routeType,
    agencyName,
  }) {
    if (currentTime() > schedule[schedule.length - 1].arrival) {
      DEBUG && console.log('xxx trip old, quit.', currentTime(), ' --- ', schedule[schedule.length - 1].arrival)
      return;
    }

    let existingVehicle = this._vehicles[vehicleId];

    if (!existingVehicle) {
      existingVehicle = new Vehicle(
        this.map,
        vehicleId,
        routeId,
        routeType,
        schedule,
        position,
        `${agencyName}/${routeName}/${vehicleId}`,
        (vehicle) => this._onVehicleFinalStop(vehicleId),
      );
      this._vehicles[vehicleId] = existingVehicle;
    }

    existingVehicle.updateData({position, schedule});
  }

  _onVehicleFinalStop(vehicleId) {
    delete this._vehicles[vehicleId];
  }
}

new Container().initialize();

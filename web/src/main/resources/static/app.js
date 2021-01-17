const routeDefaultColor = '#808080';

const trainColor = '#FF0000';
const busColor = 'purple'; // todo
const boatColor = '#0000FF';
const otherColor = '#00FF00';

const currentTime = () => Date.now() / 1000;

const fontAwesomeIcon = L.divIcon({
  html: '<i class="fa fa-train fa-2x"></i>',
  iconSize: [20, 20],
  className: 'myDivIcon'
});

class Route {
  static stopToLatLong({ latitude, longitude }) {
    return [latitude, longitude];
  }

  constructor(type, map, schedule) {
    this.type = type;
    this.polyline = L.polyline(schedule.map(Route.stopToLatLong), {
      color: routeDefaultColor,
    });
    this.polyline.addTo(map);
    console.log(666, 'route constr')
    this.stops = schedule.map(({ stop, latitude, longitude }) => {
      console.log(777, 'iter stops')
      const circle = L.circleMarker([latitude, longitude], {
        color: routeDefaultColor,
        radius: 5,
        fill: true,
        fillOpacity: 0.8,
      });
      circle.bindTooltip(stop);
      circle.addTo(map);
      return circle;
    });
  }

  setColor() {
    let newColor;
    // route types are here:
    // https://sites.google.com/site/gtfschanges/proposals/route-type
    switch(this.type) {
      case('0'):
      case('1'):
      case('2'):
        newColor = trainColor;
        break;
      case('3'):
        newColor = busColor;
        break;
      case('4'):
        newColor = boatColor;
        break;
      default:
        newColor = otherColor; // todo: green dots - type needed
        break;
    }
    this.polyline.setStyle({ color: newColor });
    this.stops.forEach((stop) => stop.setStyle({ color: newColor }));
  }

  remove() {
    this.polyline.remove();
    this.stops.forEach((stop) => stop.remove());
  }
}

class Train {
  constructor(map, routeId, routeType, schedule, name, onFinalStopCb) {
    this.routeId = routeId;
    this.routeType = routeType;
    this.schedule = schedule;
    this.name = name;

    this._map = map;
    this._onFinalStopCb = onFinalStopCb;
    this._route = new Route(this.routeType, this._map, this.schedule);
    this._train = undefined; // todo: rename to _marker
    this._heartbeatIntervalId = undefined;

    this._createHeartbeat();
  }

  updateSchedule(newSchedule) {
    this.schedule = newSchedule;
    this._refresh();
  }

  _createHeartbeat() {
    this._heartbeatIntervalId = setInterval(() => this._refresh(), 1000);
    this._refresh();
  }

  _refresh() {
    // console.log(999, this._hasMovementEnded, this._hasMovementStarted)

    // disable these to see
    // if (this._hasMovementEnded) {
    //   this._onFinalStop();
    //   return;
    // }

    // if (!this._hasMovementStarted) {
    //   return;
    // }

    //console.log(555555555)
    if (!this._train) {
      this._createNewTrain();
      this._route.setColor();
      return;
    }

    this._train.setLatLng(this._currentLatLong);
  }

  _createNewTrain() {
    // this._train = L.marker(this._currentLatLong);
    this._train = L.marker(this._currentLatLong, {icon: fontAwesomeIcon});
    this._train.bindTooltip(this.name);
    this._train.addTo(this._map);
  }

  _onFinalStop() {
    if (this._train) {
      this._train.remove();
      this._train = undefined;
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
      // Train has arrived at the final stop
      return Route.stopToLatLong(this.schedule[this.schedule.length - 1]);
    }

    const nextStop = this.schedule[nextStopI];
    const prevStop = this.schedule[nextStopI - 1];

    if (top < prevStop.departure) {
      // Train hasn't departed yet
      return Route.stopToLatLong(prevStop);
    }

    const distancePassed =
      (t - prevStop.departure) / (nextStop.arrival - prevStop.departure);

    const { latitude: prevLat, longitude: prevLong } = prevStop;
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

function randomTimeToday() {
  // get the difference between the 2 dates, multiply it by 0-1,
  // and add it to the start date to get a new date
  const start = new Date(Date.now() - 86400000); // yesterday
  const end = new Date()
  var diff =  end.getTime() - start.getTime();
  var new_diff = diff * Math.random();
  var date = new Date(start.getTime() + new_diff);
  return date;
}

class Container {
  constructor() {
    this.map = L.map('map').setView([37.6688, -122.0810], 10);
    this._trains = {};
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
    this._stomp.connect({}, () => {
      this._stomp.subscribe('/topic/updates', (update) => {
        const data = JSON.parse(update.body);

        //
        // todo: patch step 1. bogus .schedules .departure, .arrival.
        //
        const stop = data.vehicle.stop
        data.schedule = [
          {
            ...stop,
            longitude: stop.stop_long,
            latitude: stop.stop_lat,
            departure: randomTimeToday(),
            arrival: randomTimeToday()
          }
        ]

        data.schedule.forEach((stop) => {
          stop.longitude = parseFloat(stop.longitude);
          stop.latitude = parseFloat(stop.latitude);
        });

        //
        // todo: patch step 2. adapt to _processData
        //
        data.route_id = data.vehicle.trip.route.id
        data.route_name = data.vehicle.trip.route.route_name
        data.route_type = data.vehicle.trip.route.route_type
        data.agency_name = data.agencyId
        // console.log(11111, data.route_id)

        this._processData(data);
      });
    });

    $.ajax('/data/');
  }

  _processData({
    route_id: routeId,
    schedule,
    route_name: routeName,
    route_type: routeType,
    agency_name: agencyName,
  }) {
    if (currentTime() > schedule[schedule.length - 1].arrival) {
      return;
    }

    const existingTrain = this._trains[routeId];
    // console.log(222, 'existingTrain', existingTrain)

    if (!existingTrain) {
      const newTrain = new Train(
        this.map,
        routeId,
        routeType,
        schedule,
        `${routeType} ${routeName} (${agencyName})`,
        (train) => this._onTrainFinalStop(train),
      );
      this._trains[routeId] = newTrain;
      // console.log(333, newTrain)
      return;
    }

    existingTrain.updateSchedule(schedule);
  }

  _onTrainFinalStop(train) {
    delete this._trains[train.routeId];
  }
}

new Container().initialize();

const routeDefaultColor = '#808080';

const trainColor = '#FF0000';
const boatColor = '#0000FF';
const otherColor = '#00FF00';

const currentTime = () => Date.now() / 1000;

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
    this.stops = schedule.map(({ stop, latitude, longitude }) => {
      const circle = L.circle([latitude, longitude], {
        color: routeDefaultColor,
        radius: 10,
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
    switch(this.type) {
      case('InterRegio'):
      case('Intercity'):
      case('Schnelles Nachtnetz'):
      case('Standseilbahn'):
      case('Regionalzug'):
      case('Luftseilbahn'):
      case('Eurocity'):
      case('EN'):
      case('Ice'):
      case('TGV'):
      case('Drahtseilbahn'):
      case('Sesselbahn'):
      case('Extrazug'):
      case('RegioExpress'):
      case('TER200'):
      case('PanoramaExpress'):
      case('Aufzug'):
      case('Zahnradbahn'):
      case('S-Bahn'):
        newColor = trainColor;
        break;
      case('Schiff'):
        newColor = boatColor;
        break;
      default:
        newColor = otherColor;
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
    this._train = undefined;
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
    if (this._hasMovementEnded) {
      this._onFinalStop();
      return;
    }

    if (!this._hasMovementStarted) {
      return;
    }

    if (!this._train) {
      this._createNewTrain();
      this._route.setColor();
      return;
    }

    this._train.setLatLng(this._currentLatLong);
  }

  _createNewTrain() {
    this._train = L.marker(this._currentLatLong);
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

class Container {
  constructor() {
    this.map = L.map('map').setView([46.819382, 8.416515], 9);
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
        data.schedule.forEach((stop) => {
          stop.longitude = parseFloat(stop.longitude);
          stop.latitude = parseFloat(stop.latitude);
        });
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
      return;
    }

    existingTrain.updateSchedule(schedule);
  }

  _onTrainFinalStop(train) {
    delete this._trains[train.routeId];
  }
}

new Container().initialize();

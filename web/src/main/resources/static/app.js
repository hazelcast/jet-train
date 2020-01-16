const routeDefaultColor = '#808080'

const randomColor = () => {
  const colors = [
    '#FF0000',
    '#FFC000',
    '#FFFC00',
    '#FF0000',
    '#00FFFF',
    '#FF0000',
  ]
  return colors[Math.trunc(Math.random() * colors.length)]
}

class Route {
  static stopToLatLong({ latitude, longitude }) {
    return [latitude, longitude]
  }

  constructor(map, schedule) {
    this.polyline = L.polyline(schedule.map(Route.stopToLatLong), {
      color: routeDefaultColor,
    })
    this.polyline.addTo(map)
    this.stops = schedule.map(({ stop, latitude, longitude }) => {
      const circle = L.circle([latitude, longitude], {
        color: routeDefaultColor,
        radius: 10,
        fill: true,
        fillOpacity: 0.8,
      })
      circle.bindTooltip(stop)
      circle.addTo(map)
      return circle
    })
  }

  setColor(newColor) {
    this.polyline.setStyle({ color: newColor })
    this.stops.forEach((stop) => stop.setStyle({ color: newColor }))
  }

  remove() {
    this.polyline.remove()
    this.stops.forEach((stop) => stop.remove())
  }
}

class Train {
  constructor(map, routeId, schedule, name, onFinalStopCb) {
    this.routeId = routeId
    this.schedule = schedule
    this.name = name

    this._map = map
    this._onFinalStopCb = onFinalStopCb
    this._route = undefined
    this._train = undefined
    this._heartbeatIntervalId = undefined

    if (this._hasMovementEnded) {
      this._onFinalStop()
      return
    }

    this._createRoute()
    this._createHeartbeat()
  }

  updateSchedule(newSchedule) {
    this.schedule = newSchedule
    this._refresh()
  }

  _createRoute() {
    this._route = new Route(this._map, this.schedule)
  }

  _createHeartbeat() {
    this._heartbeatIntervalId = setInterval(() => this._refresh(), 1000)
    this._refresh()
  }

  _refresh() {
    if (this._hasMovementEnded) {
      this._onFinalStop()
      return
    }

    if (!this._hasMovementStarted) {
      return
    }

    if (!this._train) {
      this._createNewTrain()
      this._route.setColor(randomColor())
      return
    }

    this._train.setLatLng(this._currentLatLong)
  }

  _createNewTrain() {
    this._train = L.marker(this._currentLatLong)
    this._train.bindTooltip(this.name)
    this._train.addTo(this._map)
  }

  _onFinalStop() {
    if (this._train) {
      this._train.remove()
      this._train = undefined
    }

    if (this._route) {
      this._route.remove()
      this._route = undefined
    }

    clearInterval(this._heartbeatIntervalId)

    this._onFinalStopCb(this)
  }

  get _currentLatLong() {
    if (!this._hasMovementStarted) {
      return Route.stopToLatLong(this.schedule[0])
    }

    const nextStopI = this.schedule.findIndex(
      ({ arrival }) => this._currentTime < arrival,
    )

    if (nextStopI === -1) {
      // Train has arrived at the final stop
      return Route.stopToLatLong(this.schedule[this.schedule.length - 1])
    }

    const nextStop = this.schedule[nextStopI]
    const prevStop = this.schedule[nextStopI - 1]

    const currentTime = this._currentTime

    if (currentTime < prevStop.departure) {
      // Train hasn't departed yet
      return Route.stopToLatLong(prevStop)
    }

    const distancePassed =
      (currentTime - prevStop.departure) /
      (nextStop.arrival - prevStop.departure)

    const { latitude: prevLat, longitude: prevLong } = prevStop
    const { latitude: nextLat, longitude: nextLong } = nextStop

    const currentLat = prevLat + (nextLat - prevLat) * distancePassed
    const currentLong = prevLong + (nextLong - prevLong) * distancePassed

    return [currentLat, currentLong]
  }

  get _hasMovementEnded() {
    const routeEndTime = this.schedule[this.schedule.length - 1].arrival
    return this._currentTime > routeEndTime
  }

  get _hasMovementStarted() {
    const routeStartTime = this.schedule[0].arrival
    return this._currentTime > routeStartTime
  }

  get _currentTime() {
    return Date.now() / 1000
  }
}

class Container {
  constructor() {
    this.map = L.map('map').setView([46.819382, 8.416515], 9)
    this._trains = {}
    this._socket = undefined
    this._stomp = undefined
  }

  initialize() {
    L.tileLayer(
      'https://tile.thunderforest.com/transport/{z}/{x}/{y}{r}.png?apikey=170be1cff4224274add97bf552fd4745',
      {
        attribution:
          '&copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors,' +
          '<a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>',
      },
    ).addTo(this.map)

    this._socket = new SockJS('/hazelcast')
    this._stomp = Stomp.over(this._socket)
    this._stomp.reconnect_delay = 2000
    this._stomp.connect({}, () => {
      this._stomp.subscribe('/topic/updates', (update) => {
        const data = JSON.parse(update.body)
        data.schedule.forEach((stop) => {
          stop.longitude = parseFloat(stop.longitude)
          stop.latitude = parseFloat(stop.latitude)
        })
        this._processData(data)
      })
    })

    $.ajax('/data/')
  }

  _processData({
    route_id: routeId,
    schedule,
    route_name: routeName,
    route_type: routeType,
    agency_name: agencyName,
  }) {
    const existingTrain = this._trains[routeId]

    if (!existingTrain) {
      const newTrain = new Train(
        this.map,
        routeId,
        schedule,
        `${routeType} ${routeName} (${agencyName})`,
        (train) => this._onTrainFinalStop(train),
      )
      this._trains[routeId] = newTrain
      return
    }

    existingTrain.updateSchedule(schedule)
  }

  _onTrainFinalStop(train) {
    delete this._trains[train.routeId]
  }
}

new Container().initialize()

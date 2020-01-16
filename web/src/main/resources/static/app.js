// ;(function() {
//   let stomp = null
//   let map = null

//   function connect() {
//     const socket = new SockJS('/hazelcast')
//     stomp = Stomp.over(socket)
//     stomp.reconnect_delay = 2000
//     stomp.connect({}, function(frame) {
//       stomp.subscribe('/topic/updates', showUpdate)
//     })
//   }

//   function showUpdate(update) {
//     const body = JSON.parse(update.body)
//     for (const stop of body.schedule) {
//       const marker = L.marker([stop.latitude, stop.longitude])
//       marker.addTo(map)
//     }
//   }

//   function createMap() {
//     map = L.map('map').setView([46.819382, 8.416515], 9)
//     L.tileLayer(
//       'https://tile.thunderforest.com/transport/{z}/{x}/{y}{r}.png?apikey=170be1cff4224274add97bf552fd4745',
//       {
//         attribution:
//           '&copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors,' +
//           '<a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>',
//       },
//     ).addTo(map)
//   }

//   this.initialize = function() {
//     createMap()
//     connect()
//     $.ajax('/data/')
//   }

//   return {
//     initialize: initialize,
//   }
// })().initialize()

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

class Train {
  static _stopToLatLong({ latitude, longitude }) {
    return [parseFloat(latitude), parseFloat(longitude)]
  }

  constructor(map, routeId, schedule, onFinalStop) {
    this.routeId = routeId
    this.schedule = schedule

    this._map = map
    this._onFinalStop = onFinalStop
    this._route = undefined
    this._train = undefined
    this._heartbeatIntervalId = undefined

    this._createRoute()
    this._createHeartbeat()
  }

  updateSchedule(newSchedule) {}

  _createRoute() {
    if (this._hasRouteEnded) {
      return
    }

    const color = this._hasRouteStarted ? randomColor() : '#808080'

    const stops = this.schedule.map(Train._stopToLatLong)

    this._route = L.polyline(stops, { color })
    this._route.addTo(this._map)
  }

  _createHeartbeat() {
    this._heartbeatIntervalId = setInterval(() => this._move(), 1000)
  }

  _move() {
    if (this._hasRouteEnded) {
      if (this._train) {
        this._train.remove()
        this._train = undefined
      }

      if (this._route) {
        this._route.remove()
        this._route = undefined
      }

      this._onFinalStop(this)
      return
    }

    if (this._hasRouteStarted && !this._train) {
      this._train = L.marker(Train._stopToLatLong(this.schedule[0]), {
        color: randomColor(),
      })
      this._train.addTo(this._map)
    }
  }

  get _hasRouteEnded() {
    const routeEndTime = this.schedule[this.schedule.length - 1].arrival
    return this._currentTime > routeEndTime
  }

  get _hasRouteStarted() {
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
      this._stomp.subscribe('/topic/updates', (update) =>
        this._processData(JSON.parse(update.body)),
      )
    })

    $.ajax('/data/')
  }

  _processData({ route_id: routeId, schedule }) {
    const existingTrain = this._trains[routeId]

    if (!existingTrain) {
      const newTrain = new Train(this.map, routeId, schedule, (train) =>
        this._onTrainFinalStop(train),
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

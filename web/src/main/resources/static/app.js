const DEBUG = true

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
  html: `<i class="fa train fa-2x" style="color: ${ROUTE_COLOR_MAPPING['0']}"></i>`,
  iconSize: [20, 20],
  className: 'train-marker-icon'
})

const BusMarkerIcon = L.divIcon({
  html: `<i class="fa fa-bus fa-2x" style="color: ${ROUTE_COLOR_MAPPING['3']}"></i>`,
  iconSize: [20, 20],
  className: 'bus-marker-icon'
})

const BoatMarkerIcon = L.divIcon({
  html: `<i class="fa fa-ship fa-2x" style="color: ${ROUTE_COLOR_MAPPING['4']}"></i>`,
  iconSize: [20, 20],
  className: 'boat-marker-icon'
})

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

let KNOWN_ROUTES = {}

function currentTime() {
  return new Date()
}

class Route {
  static stopToLatLong({
                         latitude,
                         longitude
                       }) {
    return [latitude, longitude]
  }

  constructor(type, color, map, schedule) {
    if (!color.startsWith('#')) {
      color = '#' + color
    }
    this.polyline = L.polyline(schedule.map(Route.stopToLatLong), {
      color: color,
      weight: 2,
    })
    this.polyline.addTo(map)
    this.stops = schedule.map(({
                                 stopName,
                                 latitude,
                                 longitude
                               }, idx) => {
      const isLastStop = idx === schedule.length - 1
      const circle = L.circleMarker([latitude, longitude], {
        color: color,
        radius: isLastStop ? 5 : 3, // make last stops larger
        fillColor: isLastStop ? 'red' : 'lime',
        fill: true,
        fillOpacity: 0.8,
      })
      circle.bindTooltip('Stop: ' + stopName)
      circle.addTo(map)
      return circle
    })
  }

  remove() {
    this.polyline.remove()
    this.stops.forEach((stop) => stop.remove())
  }
}

class Vehicle {
  constructor(map, vehicleId, agencyName, routeId, routeType, routeColor, schedule, position, name, onFinalStopCb) {
    this.vehicleId = vehicleId
    this.agencyName = agencyName
    this.routeId = routeId
    this.routeType = routeType
    this.routeColor = routeColor
    this.schedule = schedule
    this.name = name

    this._map = map
    this._onFinalStopCb = onFinalStopCb

    // use existing route if possible
    this._route = KNOWN_ROUTES[routeId]
    if (!this._route) {
      this._route = new Route(this.routeType, this.routeColor, this._map, this.schedule)
      KNOWN_ROUTES[routeId] = this._route
    }
    this._marker = undefined

    this._lastKnownPosition = position
  }

  updateData({
               position,
               schedule
             }) {
    this.schedule = schedule
    this._lastKnownPosition = position

    if (this._hasMovementEnded) {
      DEBUG && console.log(`movement for vehicle ${this.vehicleId} has ended, removing.`)
      this._onFinalStop()
      return
    }

    if (!this._marker) {
      this._createMarker()
    }
  }

  _createMarker() {
    const icon = ROUTE_ICON_MAPPING[this.routeType]
    icon.options.html = icon.options.html.replace(/style="[^"]*"/g, `style="color: #${this.routeColor}"`)

    this._marker = L.marker([this._lastKnownPosition.latitude, this._lastKnownPosition.longitude], {
      icon
    })
    this._marker.bindTooltip(this.name)
    this._marker.addTo(this._map)
  }

  _onFinalStop() {
    DEBUG && console.log(`${this.agencyName}/${this.routeId}/${this.vehicleId} must have finished its route; removing from map.`)

    if (this._marker) {
      this._marker.remove()
      this._marker = undefined
    }

    this._onFinalStopCb(this)
  }

  get _hasMovementEnded() {
    const routeEndTime = this.schedule[this.schedule.length - 1].arrival
    return currentTime() > routeEndTime
  }

  get _lastKnownLatLong() {
    return [this._lastKnownPosition.latitude, this._lastKnownPosition.longitude]
  }

  _tick() {
    if (!this._marker) return

    const t = currentTime()
    const nextStopIdx = this.schedule.findIndex(({
                                                   arrival
                                                 }) => t < arrival)
    if (nextStopIdx === -1) {
      this._onFinalStop()
      return
    }

    let targetLatLon, animSpeed
    if (this._lastKnownPosition) {
      targetLatLon = this._lastKnownLatLong
      animSpeed = 500 // move to last known position quickly
      this._lastKnownPosition = null // skip until the next known position comes in
    } else {
      const speed = 2 // meters/sec
      const animTimerDuration = 90000
      animSpeed = Math.floor(animTimerDuration / speed) // animates good enough.

      const nextStop = this.schedule[nextStopIdx]

      // move to estimated position with fake speed above
      targetLatLon = [nextStop.latitude, nextStop.longitude]
    }

    // animation time is an approximation
    // trick - use css transitions for a smoother animation:
    const marker = this._marker
    if (marker._icon) {
      marker._icon.style[L.DomUtil.TRANSITION] = ('all ' + animSpeed + 'ms linear')
    }
    if (marker._shadow) {
      marker._shadow.style[L.DomUtil.TRANSITION] = 'all ' + animSpeed + 'ms linear'
    }

    // moves with the transition above
    this._marker.setLatLng(targetLatLon)
  }

  _disableAnimation() {
    // call this before zooming to disable animation on vehicles
    const marker = this._marker
    if (!marker) return
    if (marker._icon) {
      marker._icon.style[L.DomUtil.TRANSITION] = 'none'
    }
    if (marker._shadow) {
      marker._shadow.style[L.DomUtil.TRANSITION] = 'none'
    }
  }
}

class Container {
  constructor() {
    this.map = L.map('map').setView([37.6688, -122.0810], 10)
    this.map.on('zoomstart', () => {
      Object.values(this._vehicles).forEach(v => v._disableAnimation())
    })
    this.map.on('zoomend', () => {
      Object.values(this._vehicles).forEach(v => v._tick())
    })
    this._vehicles = {}
    this._socket = undefined
    this._stomp = undefined
  }

  initialize() {
    L.tileLayer(
        'https://tile.thunderforest.com/transport/{z}/{x}/{y}{r}.png?apikey=170be1cff4224274add97bf552fd4745', {
          attribution: '&copy; <a href="https://openstreetmap.org">OpenStreetMap</a> contributors,' +
              '<a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>',
        },
    ).addTo(this.map)

    this.listen()

    this.animationLoop()
  }

  listen() {
    this._socket = new SockJS('/hazelcast')
    this._stomp = Stomp.over(this._socket)
    this._stomp.reconnect_delay = 2000
    this._stomp.debug = null // turns off logging
    this._stomp.connect({}, () => {
      console.log('Connected to stomp server.')
      this._stomp.subscribe('/topic/updates', (update) => {
        const data = JSON.parse(update.body)

        //
        // transform the data a bit:
        //
        if (!(data.schedule && data.schedule.length)) return // lax it a bit; does hit occasionally

        data.schedule = data.schedule.map((schobj) => {
          return {
            departure: new Date(schobj.departure * 1000),
            arrival: new Date(schobj.arrival * 1000),
            longitude: parseFloat(schobj.stop.stop_long),
            latitude: parseFloat(schobj.stop.stop_lat),
            stopName: schobj.stop.stop_name,
            stopid: schobj.stop.stop_id
          }
        })

        this._processData(data)
      })
    })

    $.ajax('/data/')
  }

  animationLoop() {
    window.setInterval(() => {
      // tick for each vehicle
      Object.values(this._vehicles).forEach(v => v._tick())
    }, 3000)
  }

  _processData({
                 vehicleId,
                 routeId,
                 position,
                 schedule,
                 routeName,
                 routeType,
                 routeColor,
                 agencyName,
               }) {
    if (currentTime() > schedule[schedule.length - 1].arrival) {
      DEBUG && console.log(`trip for vehicle ${vehicleId} has ended; nothing to do.`)
      return
    }

    let existingVehicle = this._vehicles[vehicleId]

    if (!existingVehicle) {
      existingVehicle = new Vehicle(
          this.map,
          vehicleId,
          agencyName,
          routeId,
          routeType,
          routeColor,
          schedule,
          position,
          `${agencyName}/${routeName}/${vehicleId}`,
          (vehicle) => this._onVehicleFinalStop(vehicle),
      )
      this._vehicles[vehicleId] = existingVehicle
    }

    existingVehicle.updateData({
      position,
      schedule
    })
  }

  _onVehicleFinalStop(vehicle) {
    delete this._vehicles[vehicle.vehicleId]
  }
}

new Container().initialize()
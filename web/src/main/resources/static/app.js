(function () {

    let stomp = null;
    let map = null;
    let value = 8.516515;

    function connect() {
        const socket = new SockJS('/websocket');
        stomp = Stomp.over(socket);
        stomp.connect({}, function (frame) {
            stomp.subscribe('/topic/updates', showUpdate);
        });
    }

    function showUpdate(update) {
        const layer = new ol.layer.Vector({
            source: new ol.source.Vector({
                features: [
                    new ol.Feature({
                        geometry: new ol.geom.Point(ol.proj.fromLonLat([value++, 46.819382])),
                    })
                ]
            })
        });
        map.addLayer(layer);
    }

    function createMap() {
        map = new ol.Map({
            target: "map",
            layers: [
                new ol.layer.Tile({
                    source: new ol.source.OSM()
                })],
            view: new ol.View({
                center: ol.proj.fromLonLat([8.416515, 46.819382]),
                zoom: 8
            })
        });
    }

    this.initialize = function () {
        createMap();
        connect();
        $.ajax("/data/");
    };

    return {
        initialize: initialize
    }

})().initialize();
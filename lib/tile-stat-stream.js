var Transform = require('stream').Transform;
var util = require('util');
var tiletype = require('@mapbox/tiletype');
var zlib = require('zlib');
var Set = require('es6-set');
var mapboxVectorTile = require('@mapbox/vector-tile');
var Protobuf = require('pbf');
var VectorTile = mapboxVectorTile.VectorTile;
var vectorTileGeometryTypes = mapboxVectorTile.VectorTileFeature.types;

/**
 * TileStatStream is the exported functionality of this module: it is a
 * writable stream that collects statistics from vector tiles.
 */
function TileStatStream(options) {
    this.options = options;
    this.vectorLayers = {};
    Transform.call(this, { readableObjectMode: true, objectMode: true });
}

util.inherits(TileStatStream, Transform);

TileStatStream.prototype._transform = function(data, enc, callback) {
    // duck-type tile detection to avoid requiring tilelive
    if (data.x !== undefined &&
        data.y !== undefined &&
        data.z !== undefined &&
        data.buffer !== undefined &&
        tiletype.type(data.buffer) === 'pbf') {
        zlib.gunzip(data.buffer, function(err, inflatedBuffer) {
            this.push(data);
            if (err) return callback();
            var vectorTile = new VectorTile(new Protobuf(inflatedBuffer));
            for (var layerName in vectorTile.layers) {
                if (this.vectorLayers[layerName] === undefined) {
                    this.vectorLayers[layerName] = new VectorLayerStats(this.options);
                }
                var layer = vectorTile.layers[layerName];
                for (var i = 0; i < layer.length; i++) {
                    this.vectorLayers[layerName].analyzeFeature(layer.feature(i));
                }
            }
            callback();
        }.bind(this));
    } else {
        this.push(data);
        callback();
    }
};

TileStatStream.prototype.getStatistics = function() {
    var stats = {};
    for (var layer in this.vectorLayers) {
        stats[layer] = this.vectorLayers[layer].getStatistics();
    }
    return stats;
};

/**
 * VectorLayer collects statistics from a single vector_layer within
 * a vector tile.
 */
function VectorLayerStats(options) {
    options = options || {};
    this.UNIQUE_VALUES_MAX = options.maxValues || 100;
    this.count = 0;
    this.fields = {};
    this.geometryTypeCounts = [0, 0, 0, 0];
}

VectorLayerStats.prototype.analyzeFeature = function(feature) {
    this.count++;
    this.geometryTypeCounts[feature.type]++;
    for (var name in feature.properties) {
        this.analyzeProperty(name, feature.properties[name]);
    }
};

VectorLayerStats.prototype.analyzeProperty = function(name, value) {
    if (this.fields[name] === undefined) {
        this.fields[name] = {
            min: null,
            max: null,
            uniqueValues: new Set()
        };
    }
    var field = this.fields[name];

    if (typeof value === 'string' && value.length > 256) {
      return;
    }

    if (field.max === null || value > field.max) {
        field.max = value;
    }

    if (field.min === null || value < field.min) {
        field.min = value;
    }

    if (field.uniqueValues && field.uniqueValues.size < this.UNIQUE_VALUES_MAX &&
        field.uniqueValues[value] === undefined) {
        field.uniqueValues.add(value);
    }
};

function setToArray(set) {
    var values = [];
    set.forEach(function(value) { values.push(value); });
    return values;
}

VectorLayerStats.prototype.getStatistics = function() {
    var fields = {};

    for (var field in this.fields) {
        fields[field] = {
            min: this.fields[field].min,
            max: this.fields[field].max,
            values: setToArray(this.fields[field].uniqueValues)
        };
    }

    return {
        geometryTypes: this.geometryTypeCounts.reduce(function(memo, count, i) {
            memo[vectorTileGeometryTypes[i]] = count;
            return memo;
        }, {}),
        count: this.count,
        fields: fields
    };
};

module.exports = TileStatStream;

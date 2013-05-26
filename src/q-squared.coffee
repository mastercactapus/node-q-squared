Q = require('q')
Queue = require('q/queue')
Connection = require('q-connection')
os = require('os')
fork = require('child_process').fork

Q.longStackJumpLimit = 0;

_extend = (baseObject, extendWith...) ->
  for object in extendWith
    for key,val of object
      baseObject[key] = val
  baseObject

class qSquared
    @defaults:
        concurrent: os.cpus().length
    constructor: (@filePath, options) ->
        @options = _extend({}, qSquared.defaults, options)
        @workerQueue = Queue()
        @workers = []
        for n in [1..@options.concurrent]
            worker = new Worker(@filePath,options)
            @workers.push worker
            @workerQueue.put worker.get()
        @
    map: (array, methodName, extraArgs...) ->
        deferred = Q.defer()
        chunkSize = 1
        retr = []
        finished = 0
        Q(array).then (array) =>
            retr.length = array.length
            data = new ArrayData(array)
            _doChunk = =>
                localChunkSize = chunkSize
                [chunk, index] = data.get(localChunkSize)
                if finished is retr.length
                    return deferred.resolve(retr)
                return unless chunk? and index?
                @_procChunk(chunk, methodName, extraArgs).spread (totalTime, procTime, result) =>
                    elapsed = procTime
                    elapsed = 1 if elapsed is 0
                    if elapsed < 150 or elapsed > 1000
                        chunkSize = Math.max(Math.floor(localChunkSize * 150 / elapsed),1)
                    [].splice.apply(retr,[index,chunk.length].concat(result))
                    finished += chunk.length
                    _doChunk()
            for n in [1..@options.concurrent]
                _doChunk()
        deferred.promise
    _procChunk: (chunk, methodName, extraArgs) =>
        @workerQueue.get()
        .then( (worker) =>
            [new Date(), worker, worker.map(chunk, methodName, extraArgs)]
        ).spread (timestamp, worker, results) =>
            @workerQueue.put(worker)
            [
                new Date() - timestamp
                results.elapsed
                results.value
            ]
    close: ->
        for worker in @workers
            worker.close()

class ArrayData
    constructor: (@array) ->
        @index = 0
    get: (size) ->
        return [null,null] if @array.length is @index
        end = Math.min(@array.length, @index + size)
        retr = [@array.slice(@index,end), @index]
        @index=end
        retr

class Worker
    get: ->
        @init.promise
    _ready: =>
        @init.resolve(@)
        null
    constructor: (@filePath, options) ->
        wrapperPath = require.resolve('./child')
        @proc = fork(wrapperPath, [@filePath], options)
        @init = Q.defer()
        @conn = Connection(@proc, {ready: @_ready})
    map: (chunkData, methodName, extraArgs) ->
        @conn.invoke('map', chunkData, methodName, extraArgs)
    close: ->
        @proc.kill()

module.exports = qSquared

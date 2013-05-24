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
            @workerQueue.put worker
            @workers.push worker
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
                console.log chunkSize
                [chunk, index] = data.get(localChunkSize)
                if finished is retr.length
                    return deferred.resolve(retr)
                return unless chunk? and index?
                @_procChunk(chunk, methodName, extraArgs).spread (result) =>
                    elapsed = result.totalTime
                    console.log 'elapsed',elapsed
                    elapsed = 1 if elapsed is 0
                    if elapsed < 50 or elapsed > 250
                        chunkSize = Math.max(Math.floor(localChunkSize * 50 / elapsed),1)
                    [].splice.apply(retr,[index,chunk.length].concat(result.result))
                    finished += chunk.length
                    _doChunk()
            for n in [1..@options.concurrent]
                _doChunk()
        deferred.promise
    _procChunk: (chunk, methodName, extraArgs) =>
        @workerQueue.get()
        .then (worker) =>
            [new Date(), worker, worker.map(chunk, methodName, extraArgs)]
        .spread (timestamp, worker, result) =>
            @workerQueue.put(worker)
            {
                totalTime: new Date() - timestamp
                result: result.result
                procTime: result.elapsed
            }
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
    constructor: (@filePath, options) ->
        wrapperPath = require.resolve('./child')
        @proc = fork(wrapperPath, [@filePath], options)
        @conn = Connection(@proc)
    map: (chunkData, methodName, extraArgs) ->
        @conn.invoke('map', chunkData, methodName, extraArgs)
    close: ->
        @proc.kill()

module.exports = qSquared
